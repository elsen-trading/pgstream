{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.PostgreSQL.Stream.Types (
  -- * Core types
  Identifier(..),
  SQL(..),
  Only(..),
  Null(..),
  Action(..),
  Query(..),

  -- * Exceptions
  QueryError(..),
  ConversionError(..),
) where

import Data.Monoid
import Data.String
import Data.Typeable
import Data.ByteString as B
import Data.ByteString.Char8 as B8

import Control.Exception

-------------------------------------------------------------------------------
-- Core Types
-------------------------------------------------------------------------------

-- | Literal SQL logic spliced in as a subexpression
newtype SQL = SQLExpr { unSQL :: ByteString }
  deriving (Eq, Ord, Show, IsString)

-- | Newtype for a singular result set or argument value.
newtype Only a = Only { unOnly :: a }
  deriving (Eq, Ord, Read, Show, Typeable, Functor)

-- | Literal SQL identifier (i.e. table field names), spliced into the SQL query
-- unquoted.
newtype Identifier = Identifier { unIdentifier :: ByteString }
  deriving (Eq, Ord, Show, IsString)

-- | SQL Null type
data Null = Null

-- | SQL Query subexpression
data Action
  = Plain ByteString
  | Escape ByteString
  | EscapeIdentifier ByteString
  deriving (Eq, Typeable, Show)

-- | SQL Query
newtype Query = Query { fromQuery :: ByteString }
  deriving (Eq, Ord, Typeable)

instance Show Query where
  show = show . fromQuery

instance IsString Query where
  fromString s = Query (B8.pack s)

instance Monoid Query where
  mempty = Query B.empty
  mappend (Query a) (Query b) = Query (B.append a b)
  {-# INLINE mappend #-}
  mconcat xs = Query (B.concat (fmap fromQuery xs))

-------------------------------------------------------------------------------
-- Failure
-------------------------------------------------------------------------------

data QueryError = QueryError
  { qeMessage :: String
  , qeQuery :: Query
  } deriving (Eq, Show, Typeable)

instance Exception QueryError

data ConversionError
  = ConversionError { ceMessage :: String }  -- ^ Bytestring is malformed
  | UnexpectedNull { ceMessage :: String }   -- ^ Unexpected null value
  | Incompatible { ceMessage :: String }     -- ^ Type is incompatible with data
  deriving (Eq, Show, Typeable)

instance Exception ConversionError
