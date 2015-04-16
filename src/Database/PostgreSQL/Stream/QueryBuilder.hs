{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeSynonymInstances #-}

module Database.PostgreSQL.Stream.QueryBuilder (
  -- ** Quasiquoter
   sql,

  -- ** Query formatting
  fmtQuery,
  fmtSQL,

  -- ** Typeclasses
  ToSQL(..),
  ToField(..),
) where

import Database.PostgreSQL.Stream.Types

import Data.Int
import Data.Monoid
import Data.UUID as UUID
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8)

import Language.Haskell.TH
import Language.Haskell.TH.Quote

import Data.ByteString
import Data.ByteString.Search
import Data.ByteString.Lazy (toStrict)
import qualified Data.ByteString.Char8 as B8

-------------------------------------------------------------------------------
-- Arguments
-------------------------------------------------------------------------------

class ToField a where
  toField :: a -> Action

instance ToField Int where
  toField = Plain . B8.pack . show

instance ToField Int32 where
  toField = Plain . B8.pack . show

instance ToField Float where
  toField = Plain . B8.pack . show

instance ToField Double where
  toField = Plain . B8.pack . show

instance ToField ByteString where
  toField = Plain

instance ToField Integer where
  toField = Plain . B8.pack . show

instance ToField Char where
  toField = Plain . inQuotes . B8.pack . show

instance ToField String where
  toField = Plain . inQuotes . B8.pack

instance ToField Text where
  toField = Plain . inQuotes . encodeUtf8

-- SQL Identifier
instance ToField Identifier where
  toField = Plain . unIdentifier

-- SQL Expression
instance ToField SQL where
  toField = Plain . unSQL

-- Subquery (without substuttion, discards parameters)
instance ToField Query where
  toField (Query a) = Plain a

instance ToField UUID where
  toField = Plain . inQuotes . UUID.toASCIIBytes

instance ToField Null where
  toField _ = Plain "null"

instance (ToField a) => ToField (Only a) where
  toField (Only a)  = toField a

instance (ToField a) => ToField (Maybe a) where
  toField Nothing  = toField Null
  toField (Just a) = toField a

instance ToField Bool where
    toField True  = Plain "true"
    toField False = Plain "false"

instance ToField Action where
  toField = id

inQuotes :: ByteString -> ByteString
inQuotes x = "\'" <> x <> "\'"

-------------------------------------------------------------------------------
-- ToSQL
-------------------------------------------------------------------------------

class ToSQL a where
  toSQL :: a -> (ByteString -> ByteString)

instance ToSQL () where
  toSQL _ = runFormatter []

instance (ToField a) => ToSQL (Only a) where
  toSQL (Only a) = runFormatter [toField a]

instance (ToField a) => ToSQL [a] where
  toSQL a = runFormatter (fmap toField a)

instance (ToField a) => ToSQL (Maybe a) where
  toSQL Nothing = runFormatter []
  toSQL (Just a) = runFormatter [toField a]

instance (ToField a, ToField b) => ToSQL (a,b) where
  toSQL (a,b) = runFormatter [toField a, toField b]

instance (ToField a, ToField b, ToField c) => ToSQL (a,b,c) where
  toSQL (a,b,c) = runFormatter [toField a, toField b, toField c]

instance (ToField a, ToField b, ToField c, ToField d) => ToSQL (a,b,c,d) where
  toSQL (a,b,c,d) = runFormatter [toField a, toField b, toField c, toField d]

instance (ToField a, ToField b, ToField c, ToField d, ToField e) => ToSQL (a,b,c,d,e) where
  toSQL (a,b,c,d,e) = runFormatter [toField a, toField b, toField c, toField d, toField e]

instance (ToField a, ToField b, ToField c, ToField d, ToField e, ToField f) => ToSQL (a,b,c,d,e,f) where
  toSQL (a,b,c,d,e,f) = runFormatter [toField a, toField b, toField c, toField d, toField e, toField f]

instance (ToField a, ToField b, ToField c, ToField d, ToField e, ToField f, ToField g) => ToSQL (a,b,c,d,e,f,g) where
  toSQL (a,b,c,d,e,f,g) = runFormatter [toField a, toField b, toField c, toField d, toField e, toField f, toField g]

instance (ToField a, ToField b, ToField c, ToField d, ToField e, ToField f, ToField g, ToField h) => ToSQL (a,b,c,d,e,f,g,h) where
  toSQL (a,b,c,d,e,f,g,h) = runFormatter [toField a, toField b, toField c, toField d, toField e, toField f, toField g, toField h]

instance (ToField a, ToField b, ToField c, ToField d, ToField e, ToField f, ToField g, ToField h, ToField i) => ToSQL (a,b,c,d,e,f,g,h,i) where
  toSQL (a,b,c,d,e,f,g,h,i) = runFormatter [toField a, toField b, toField c, toField d, toField e, toField f, toField g, toField h, toField i]

-------------------------------------------------------------------------------
-- Formatter
-------------------------------------------------------------------------------

render :: Action -> ByteString
render (Plain x) = x
render (Escape x) = error "Not implemented"
render (EscapeIdentifier x) = error "Not implemented"

sql :: QuasiQuoter
sql = QuasiQuoter
  { quotePat  = error "Patterns are not supported"
  , quoteType = error "Types are not supported"
  , quoteExp  = sqlExp
  , quoteDec  = error "Declarations are not supported"
  }

sqlExp :: String -> Q Exp
sqlExp = stringE

-- Run the substitutions over a bytestring
runFormatter :: [Action] -> ByteString -> ByteString
runFormatter args input = loop args 1 input
  where
    loop (x:xs) i s = loop xs (i+1) $ toStrict (replace ("{" <> ix i <> "}") (render x) s)
    loop [] _ s = s

    ix :: Int -> ByteString
    ix = B8.pack . show

-------------------------------------------------------------------------------
-- query
-------------------------------------------------------------------------------

fmtQuery :: ToSQL a => Query -> a -> ByteString
fmtQuery q args = toSQL args (fromQuery q)

fmtSQL :: ToSQL a => Query -> a -> SQL
fmtSQL q args = SQLExpr $ toSQL args (fromQuery q)
