{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.PostgreSQL.Stream.FromRow (
  FromRow(..),
  HasPQType(..),
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

import Data.UUID (UUID)
import Data.Word (Word8)
import Data.Time.Clock
import Data.Time.LocalTime
import Data.Fixed
import Data.Time.Calendar
import Data.Scientific (Scientific)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BL
import Data.ByteString.Internal (toForeignPtr)
import qualified Data.Vector.Storable as V
import qualified Data.Vector.Storable.Mutable as VM

import Data.Text (Text)
import qualified Data.Text.Lazy as TL

import qualified PostgreSQL.Binary.Decoder as PD
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

instance (FromField a, FromField b, FromField c, FromField d, FromField e, FromField f) => FromRow (a,b,c,d,e,f) where
    fromRow = (,,,,,) <$> field <*> field <*> field <*> field <*> field <*> field

instance (FromField a, FromField b, FromField c, FromField d, FromField e, FromField f, FromField g) => FromRow (a,b,c,d,e,f,g) where
    fromRow = (,,,,,,) <$> field <*> field <*> field <*> field <*> field <*> field <*> field

-------------------------------------------------------------------------------
-- FieldConversion
-------------------------------------------------------------------------------

data Row = Row
  { row        :: {-# UNPACK #-} !PQ.Row
  , rowresult  :: !PQ.Result
  }

type FieldParser a = (PQ.Oid, Int, Maybe ByteString) -> a

-- sloww check for correct type. also this is too strict,
-- e.g. Integer should accept any kind of int.
-- TODO: Add a package-level `strict' to enable/disable this check.
checkTy :: HasPQType a => Query -> PQ.Oid -> a -> a
checkTy q oid a = if oid `elem` pqType a then a
  else throw $
    Incompatible $
         "Type error. Expected pq typeoid "
      ++ show (pqType a)
      ++ " but got "
      ++ show oid
      ++ ". Full query: "
      ++ show q

class HasPQType a where
  pqType :: a -> [PQ.Oid]

class HasPQType a => FromField a where
    -- conversion from raw postgres bytestring
    fromField :: (PQ.Oid, Int, Maybe ByteString) -> a

-- int2
instance HasPQType Int16 where
  pqType _ = PQ.Oid <$> [ 21 ] -- INT2OID
instance FromField Int16 where
    fromField (ty, length, Just bs) = case PD.run PD.int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null int2"

-- int4
instance HasPQType Int32 where
  pqType _ = PQ.Oid <$> [ 21, 23 ] -- INT2OID, INT4OID
instance FromField Int32 where
    fromField (ty, length, Just bs) = case PD.run PD.int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null int4"

-- int8
instance HasPQType Int64 where
  pqType _ = PQ.Oid <$> [ 21, 23, 20 ] -- INT2OID, INT4OID, INT8OID
instance FromField Int64 where
    fromField (ty, length, Just bs) = case PD.run PD.int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null int8"

-- int8
instance HasPQType Int where
  pqType _ = PQ.Oid <$> [ 21, 23, 20 ] -- INT2OID, INT4OID, INT8OID
instance FromField Int where
    fromField (ty, length, Just bs) = case PD.run PD.int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null int4"

-- float4
instance HasPQType Float where
  pqType _ = PQ.Oid <$> [ 700 ] -- FLOAT4OID
instance FromField Float where
    fromField (ty, length, Just bs) = case PD.run PD.float4 bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null float4"

-- float8
instance HasPQType Double where
  pqType _ = PQ.Oid <$> [ 701 ] -- FLOAT8OID
instance FromField Double where
    fromField (ty, length, Just bs) = case PD.run PD.float8 bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null float8"

-- integer
instance HasPQType Integer where
  pqType _ = PQ.Oid <$> [ 21, 23, 20 ] -- INT2OID, INT4OID, INT8OID
instance FromField Integer where
    fromField (ty, length, Just bs) = case PD.run PD.int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null integer"

-- numeric
instance HasPQType Scientific where
  pqType _ = PQ.Oid <$> [ 1700 ] -- NUMERICOID
instance FromField Scientific where
    fromField (ty, length, Just bs) = case PD.run PD.numeric bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null numeric"

-- uuid
instance HasPQType UUID where
  pqType _ = PQ.Oid <$> [ 2950 ] -- UUIDOID
instance FromField UUID where
    fromField (ty, length, Just bs) = case PD.run PD.uuid bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null uuid"

-- char
instance HasPQType Char where
  pqType _ = PQ.Oid <$> [ 1042 ] -- BPCHAROID
instance FromField Char where
    fromField (ty, length, Just bs) = case PD.run PD.char bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null char"

-- text
instance HasPQType Text where
  pqType _ = PQ.Oid <$> [ 25, 1043 ] -- TEXTOID, VARCHAROID
instance FromField Text where
    fromField (ty, length, Just bs) = case PD.run PD.text_strict bs of
      Left x -> throw $ ConversionError "Malformed bytestring."
      Right x -> x
    fromField _ = throw $ ConversionError "Excepted non-null text"

instance HasPQType TL.Text where
  pqType _ = PQ.Oid <$> [ 25, 1043 ] -- TEXTOID, VARCHAROID
instance FromField TL.Text where
    fromField (ty, length, Just bs) = case PD.run PD.text_lazy bs of
      Left x -> throw $ ConversionError "Malformed bytestring."
      Right x -> x
    fromField _ = throw $ ConversionError "Excepted non-null text"

-- bytea
instance HasPQType ByteString where
  pqType _ = PQ.Oid <$> [ 17 ] -- BYTEAOID
instance FromField ByteString where
    fromField (ty, length, Just bs) = case PD.run PD.bytea_strict bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null bytea"

instance HasPQType BL.ByteString where
  pqType _ = PQ.Oid <$> [ 17 ] -- BYTEAOID
instance FromField BL.ByteString where
    fromField (ty, length, Just bs) = case PD.run PD.bytea_lazy bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null bytea"

-- bool
instance HasPQType Bool where
  pqType _ = PQ.Oid <$> [ 16 ] -- BOOLOID
instance FromField Bool where
    fromField (ty, length, Just bs) = case PD.run PD.bool bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null bool"

-- date
instance HasPQType Day where
  pqType _ = PQ.Oid <$> [ 1082 ] -- DATEOID
instance FromField Day where
    fromField (ty, length, Just bs) = case PD.run PD.date bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null date"

-- time
-- the following cases only work when integer_datetimes is set to 'on'
-- TODO add a fallback case
instance HasPQType TimeOfDay where
  pqType _ = PQ.Oid <$> [ 1083 ] -- TIMEOID
instance FromField TimeOfDay where
    fromField (ty, length, Just bs) = case PD.run PD.time_int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null date"

instance HasPQType (TimeOfDay, TimeZone) where
  pqType _ = PQ.Oid <$> [ 1266 ] -- TIMETZOID
instance FromField (TimeOfDay, TimeZone) where
    fromField (ty, length, Just bs) = case PD.run PD.timetz_int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null date"

instance HasPQType UTCTime where
  pqType _ = PQ.Oid <$> [ 1184 ] -- TIMESTAMPTZOID
instance FromField UTCTime where
    fromField (ty, length, Just bs) = case PD.run PD.timestamptz_int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null date"

instance HasPQType NominalDiffTime where
  pqType _ = PQ.Oid <$> [ 20 ] -- INT8OID
instance FromField NominalDiffTime where
    fromField (ty, length, Just bs) = case PD.run PD.int bs of { Right x -> (fromIntegral (x :: Int)) }
    fromField _ = throw $ ConversionError "Excepted non-null date"

instance HasPQType DiffTime where
  pqType _ = PQ.Oid <$> [ 1186 ] -- INTERVALOID
instance FromField DiffTime where
    fromField (ty, length, Just bs) = case PD.run PD.interval_int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null date"

instance HasPQType LocalTime where
  pqType _ = PQ.Oid <$> [ 1114 ] -- TIMESTAMPOID
instance FromField LocalTime where
    fromField (ty, length, Just bs) = case PD.run PD.timestamp_int bs of { Right x -> x }
    fromField _ = throw $ ConversionError "Excepted non-null date"

-- money
instance HasPQType (Fixed E3) where
  -- cashoid?
  pqType _ = PQ.Oid <$> [ 20 ] -- INT8OID
instance FromField (Fixed E3) where
    fromField (ty, length, Just bs) = case PD.run PD.int bs of { Right x -> fromIntegral (x :: Int) / 100 }
    fromField _ = throw $ ConversionError "Excepted non-null money"

instance HasPQType (Fixed E2) where
  -- cashoid?
  pqType _ = PQ.Oid <$> [ 20 ] -- INT8OID
instance FromField (Fixed E2) where
    fromField (ty, length, Just bs) = case PD.run PD.int bs of { Right x -> fromIntegral (x :: Int) / 100 }
    fromField _ = throw $ ConversionError "Excepted non-null money"

-- nullable
instance HasPQType a => HasPQType (Maybe a) where
  -- PQ.Oid 705 can be returned for NULL values where the sql
  -- engine cannot determine its type, e.g.
  -- `select NULL`
  pqType (x :: Maybe a) = PQ.Oid 705 : pqType (undefined :: a)
instance FromField a => FromField (Maybe a) where
    fromField (_, _, Nothing) = Nothing
    fromField x = Just (fromField x)

-- int4[]
instance HasPQType (V.Vector Int32) where
  pqType _ = PQ.Oid <$> [ 1007 ] -- INT4ARRAYOID
instance FromField (V.Vector Int32) where
    fromField (ty, arrlength, Just bs) = unsafeDupablePerformIO $ int4vector bs arrlength
    fromField _ = throw $ ConversionError "Excepted non-null int4[]"

-- float4[]
instance HasPQType (V.Vector Float) where
  pqType _ = PQ.Oid <$> [ 1021 ] -- FLOAT4ARRAYOID
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
    PQ.Oid 1016 -> (len - 20) `div` 8 -- int8[]
    PQ.Oid 1021 -> (len - 20) `div` 8 -- float4[]
    _ -> error $ "Size not yet implemented" ++ (show ty)

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
field :: (HasPQType a, FromField a) => RowParser a
field = fieldWith checkTy fromField

newtype RowParser a = RP { unRP :: (Query -> Row -> PQ.Column -> (PQ.Column, a)) }

instance Functor RowParser where
  fmap f (RP p) = RP (\q r c -> let (c', a) = p q r c in (c', f a))

instance Applicative RowParser where
  pure = return
  (RP f) <*> (RP x) = RP $ \q r c -> let
    (!c',  f') = f q r c
    (!c'', x') = x q r c'
    in (c'', f' x')

instance Monad RowParser where
  return a = RP (\_ _ c -> (c, a))
  (RP x) >>= f = RP $ \q r c -> let
    (!c', a) = x q r c
    RP y     = f a
    in y q r c'

runRowParser :: RowParser a -> Query -> Row -> a
runRowParser (RP p) q r = snd (p q r 0)

fieldWith :: (Query -> PQ.Oid -> a -> a) -> FieldParser a -> RowParser a
fieldWith checkTy fieldP = RP $ \q (Row {..}) column ->
  let
    !result  = rowresult
    !typeOid = unsafeDupablePerformIO $ PQ.ftype result column
    !len     = unsafeDupablePerformIO $ PQ.getlength result row column
    !buffer  = unsafeDupablePerformIO $ PQ.getvalue result row column
    res      = fieldP (typeOid, calculateSize typeOid len, buffer)
  in (,) (succ column) $ case row of
    PQ.Row 0 -> checkTy q typeOid res
    _ -> res

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
        --ncols <- liftIO $ PQ.nfields result
        xs <- rowLoop 0 (nrows-1) $ \row -> do
            let rw = Row row result
            {-coltype <- liftIO $ PQ.ftype result c-}
            {-len <- liftIO $ PQ.getlength result row c-}
            {-size <- liftIO $ PQ.fsize result c-}
            {-buffer <- liftIO $ PQ.getvalue result row c-}
            return $ runRowParser fromRow q rw
        return xs

    _ -> do
       err <- liftIO $ PQ.resultErrorMessage result
       liftIO $ throwIO $ QueryError (show err) q
