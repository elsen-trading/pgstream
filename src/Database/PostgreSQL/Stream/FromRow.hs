{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleInstances #-}

module Database.PostgreSQL.Stream.FromRow (
  FromRow(..),
  FromField(..),
  Row(..),
  field,

  runRowParser,
  parseRows,
) where

import Database.PostgreSQL.Stream.Types

import Data.Int

import Control.Applicative
import Control.Exception
import Control.Monad (when)
import Control.Monad.Trans
import Control.Monad.Trans.State
import Control.Monad.Trans.Reader

import Data.Text (Text)
import Data.UUID (UUID)
import Data.Word (Word8)
import Data.Scientific (Scientific)
import Data.ByteString (ByteString)
import Data.ByteString.Internal (toForeignPtr)
import qualified Data.Vector.Storable as V
import qualified Data.Vector.Storable.Mutable as VM

import qualified PostgreSQLBinary.Decoder as PD
import qualified Database.PostgreSQL.LibPQ as PQ

import Unsafe.Coerce
import System.IO.Unsafe

import Foreign.C
import Foreign.Ptr
import Foreign.ForeignPtr.Safe

-------------------------------------------------------------------------------
-- FFI
-------------------------------------------------------------------------------

foreign import ccall unsafe "array_conversion.h extract_int4_array"
  extract_int4_array :: Ptr Word8 -> Ptr CInt -> CInt -> IO Int
foreign import ccall unsafe "array_conversion.h extract_float_array"
  extract_numeric_array :: Ptr Word8 -> Ptr CInt -> CInt -> IO Int

type ExFun = Ptr Word8 -> Ptr CInt -> CInt -> IO Int

-------------------------------------------------------------------------------
-- Rows
-------------------------------------------------------------------------------

class FromRow a where
    fromRow :: RowParser a

instance (FromField a) => FromRow (Only a) where
    fromRow = Only <$> field

instance (FromField a, FromField b) => FromRow (a,b) where
    fromRow = (,) <$> field <*> field

instance (FromField a, FromField b, FromField c) => FromRow (a,b,c) where
    fromRow = (,,) <$> field <*> field <*> field

instance (FromField a, FromField b, FromField c, FromField d) => FromRow (a,b,c,d) where
    fromRow = (,,,) <$> field <*> field <*> field <*> field

instance (FromField a, FromField b, FromField c, FromField d, FromField e) => FromRow (a,b,c,d,e) where
    fromRow = (,,,,) <$> field <*> field <*> field <*> field <*> field

-------------------------------------------------------------------------------
-- FieldConversion
-------------------------------------------------------------------------------

data Row = Row
  { row        :: {-# UNPACK #-} !PQ.Row
  , rowresult  :: !PQ.Result
  }

type FieldParser a = (PQ.Oid, Int, Maybe ByteString) -> a

class FromField a where
    -- Unchecked type conversion from raw postgres bytestring
    fromField :: (PQ.Oid, Int, Maybe ByteString) -> a

-- int2
instance FromField Int8 where
    fromField (ty, length, Just bs) = case PD.int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null int2"

-- int4
instance FromField Int16 where
    fromField (ty, length, Just bs) = case PD.int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null int4"

-- int4
instance FromField Int32 where
    fromField (ty, length, Just bs) = case PD.int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null int4"

-- int8
instance FromField Int64 where
    fromField (ty, length, Just bs) = case PD.int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null int8"

-- int8
instance FromField Int where
    fromField (ty, length, Just bs) = case PD.int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null int4"

-- float4
instance FromField Float where
    fromField (ty, length, Just bs) = case PD.float4 bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null float4"

-- float8
instance FromField Double where
    fromField (ty, length, Just bs) = case PD.float8 bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null float8"

-- integer
instance FromField Integer where
    fromField (ty, length, Just bs) = case PD.int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null integer"

-- numeric
instance FromField Scientific where
    fromField (ty, length, Just bs) = case PD.numeric bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null numeric"

-- uuid
instance FromField UUID where
    fromField (ty, length, Just bs) = case PD.uuid bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null uuid"

-- char
instance FromField Char where
    fromField (ty, length, Just bs) = case PD.char bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null char"

-- text
instance FromField Text where
    fromField (ty, length, Just bs) = case PD.text bs of
      Left x -> throw $ ConversionError "Malformed bytestring."
      Right x -> x
    fromField _ = throw $ ConversionError "Excepted non-null text"

-- bytea
instance FromField ByteString where
    fromField (ty, length, Just bs) = case PD.bytea bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null bytea"

-- bool
instance FromField Bool where
    fromField (ty, length, Just bs) = case PD.bool bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null bool"

-- nullable
instance FromField a => FromField (Maybe a) where
    fromField (_, _, Nothing) = Nothing
    fromField x = Just (fromField x)

-- int4[]
instance FromField (V.Vector Int32) where
    fromField (ty, arrlength, Just bs) = unsafeDupablePerformIO $ int4vector bs arrlength
    fromField _ = throw $ ConversionError "Excepted non-null int4[]"

-- float4[]
instance FromField (V.Vector Float) where
    fromField (ty, arrlength, Just bs) = unsafeDupablePerformIO $ float32vector bs arrlength
    fromField _ = throw $ ConversionError "Excepted non-null float4[]"

-- Converter that is independent of the given length and type
converter :: Show e => (ByteString -> Either e a) -> (PQ.Oid, Int, Maybe ByteString) -> a
converter f (_, _, Just bs) = case f bs of
  Left err -> throw $ ConversionError (show err)
  Right val -> val
converter f (_, _, Nothing) = throw $ ConversionError "Excepted non-null"

-------------------------------------------------------------------------------
-- Byte Shuffling
-------------------------------------------------------------------------------

--  * Postgres varlena array has the following internal structure:
--    7  *    <vl_len_>     - standard varlena header word
--    8  *    <ndim>        - number of dimensions of the array
--    9  *    <dataoffset>  - offset to stored data, or 0 if no nulls bitmap
--   10  *    <elemtype>    - element type OID
--   11  *    <dimensions>  - length of each array axis (C array of int)
--   12  *    <lower bnds>  - lower boundary of each dimension (C array of int)
--   13  *    <null bitmap> - bitmap showing locations of nulls (OPTIONAL)
--   14  *    <actual data> - whatever is the stored data

-- Binary cursor overhead is 20 bytes.
calculateSize :: PQ.Oid -> Int -> Int
calculateSize ty len =
  case ty of
    PQ.Oid 16   -> 1                  -- bool
    PQ.Oid 21   -> 2                  -- int2
    PQ.Oid 23   -> 4                  -- int4
    PQ.Oid 20   -> 8                  -- int8
    PQ.Oid 700  -> 4                  -- float4
    PQ.Oid 701  -> 8                  -- float8
    PQ.Oid 25   -> -1                 -- text
    PQ.Oid 2950 -> -1                 -- uuid
    PQ.Oid 1007 -> (len - 20) `div` 8 -- int4[]
    PQ.Oid 1021 -> (len - 20) `div` 8 -- float4[]
    _ -> error "Not yet implemented"

--    16   -> bool
--    17   -> bytea
--    18   -> char
--    19   -> name
--    20   -> int8
--    21   -> int2
--    23   -> int4
--    24   -> regproc
--    25   -> text
--    26   -> oid
--    27   -> tid
--    28   -> xid
--    29   -> cid
--    700  -> float4
--    701  -> float8
--    702  -> abstime
--    703  -> reltime
--    704  -> tinterval
--    705  -> unknown
--    790  -> money
--    1042 -> bpchar
--    1043 -> varchar
--    1082 -> date
--    1083 -> time
--    1114 -> timestamp
--    1184 -> timestamptz
--    1186 -> interval
--    1266 -> timetz
--    1560 -> bit
--    1562 -> varbit
--    1700 -> numeric
--    2249 -> record
--    2278 -> void
--    2950 -> uuid

-------------------------------------------------------------------------------
-- Vector Extraction
-------------------------------------------------------------------------------

{-# INLINE extractor #-}
extractor :: ExFun -> ByteString -> Int -> IO (V.Vector a)
extractor f bs len = do
  vec <- VM.new len
  let (fptr, _, _) = toForeignPtr bs
  let (aptr, _) = VM.unsafeToForeignPtr0 vec

  rc <- withForeignPtr fptr $ \iptr ->
          withForeignPtr aptr $ \optr ->
            f iptr optr (fromIntegral len)

  -- XXX better error handling
  when (rc /= len) $ throwIO $ ConversionError "Extraction kernel malfunctioned."
  ovec <- V.unsafeFreeze vec
  -- CInt and Int look the same in memory, so coercion is safe
  return (unsafeCoerce ovec)

{-# NOINLINE float32vector #-}
float32vector :: ByteString -> Int -> IO (V.Vector Float)
float32vector = extractor extract_numeric_array

{-# NOINLINE int4vector #-}
int4vector :: ByteString -> Int -> IO (V.Vector Int32)
int4vector = extractor extract_int4_array

-------------------------------------------------------------------------------
-- Field Conversion
-------------------------------------------------------------------------------

-- | Result set field
field :: FromField a => RowParser a
field = fieldWith fromField

newtype RowParser a = RP { unRP :: ReaderT Row (State PQ.Column) a }
   deriving ( Functor, Applicative, Monad )

runRowParser :: RowParser a -> Row -> a
runRowParser parser rw = evalState (runReaderT (unRP parser) rw) 0

fieldWith :: FieldParser a -> RowParser a
fieldWith fieldP = RP $ do
    r@Row{..} <- ask
    column <- lift get
    lift (put (column + 1))
    let
        !ncols = PQ.nfields rowresult
        !result = rowresult
        !typeOid = unsafeDupablePerformIO $ PQ.ftype result column
        !len = unsafeDupablePerformIO $ PQ.getlength result row column
        !buffer = unsafeDupablePerformIO $ PQ.getvalue result row column
    return $ fieldP (typeOid, calculateSize typeOid len, buffer)

-------------------------------------------------------------------------------
-- Result Sets
-------------------------------------------------------------------------------

rowLoop :: (Ord n, Num n) => n -> n -> (n -> IO a) -> IO [a]
rowLoop lo hi m = loop hi []
  where
    loop !n !as
      | n < lo = return as
      | otherwise = do
           a <- m n
           loop (n-1) (a:as)

parseRows :: FromRow r => Query -> PQ.Result -> IO [r]
parseRows q result = do
  status <- liftIO $ PQ.resultStatus result
  case status of
    PQ.EmptyQuery ->
       liftIO $ throwIO $ QueryError "query: Empty query" q

    PQ.CommandOk -> do
       liftIO $ throwIO $ QueryError "query resulted in a command response" q

    PQ.TuplesOk -> do
        nrows <- liftIO $ PQ.ntuples result
        ncols <- liftIO $ PQ.nfields result
        xs <- rowLoop 0 (nrows-1) $ \row -> do
            let rw = Row row result
            {-coltype <- liftIO $ PQ.ftype result c-}
            {-len <- liftIO $ PQ.getlength result row c-}
            {-size <- liftIO $ PQ.fsize result c-}
            {-buffer <- liftIO $ PQ.getvalue result row c-}
            return $ runRowParser fromRow rw
        return xs

    _ -> do
       err <- liftIO $ PQ.resultErrorMessage result
       liftIO $ throwIO $ QueryError (show err) q
