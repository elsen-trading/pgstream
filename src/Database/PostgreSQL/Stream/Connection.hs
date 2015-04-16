{-# LANGUAGE OverloadedStrings #-}

module Database.PostgreSQL.Stream.Connection (
  PoolSettings(..),
  ConnSettings(..),
  defaultPoolSettings,

  pgPool,
  withPgConnection,

  connect,
  connect_alt,
) where

import Data.Monoid
import Data.Pool
import Control.Applicative
import Data.Time.Clock (NominalDiffTime)

import Data.ByteString (ByteString)
import qualified Database.PostgreSQL.LibPQ as PQ

-------------------------------------------------------------------------------
-- Connection Pools
-------------------------------------------------------------------------------

data PoolSettings = PoolSettings
  { _stripes   :: Int             -- ^ Stripe count. The number of distinct sub-pools to maintain. The smallest acceptable value is 1.
  , _keepalive :: NominalDiffTime -- ^ Amount of time for which an unused resource is kept open. The smallest acceptable value is 0.5 seconds.
  , _affinity  :: Int             -- ^ Maximum number of resources to keep open per stripe. The smallest acceptable value is 1.
  } deriving (Eq, Ord, Show)

defaultPoolSettings :: PoolSettings
defaultPoolSettings = PoolSettings { _stripes = 1, _keepalive = 10, _affinity = 10 }

pgPool :: PQ.Connection -> IO (Pool PQ.Connection)
pgPool conn = createPool (pure conn) PQ.finish 1 10 10

withPgConnection :: PQ.Connection -> (PQ.Connection -> IO b) -> IO b
withPgConnection conn action = do
  pool <- pgPool conn
  withResource pool action

data ConnSettings = ConnSettings
  { _host     :: ByteString
  , _dbname   :: ByteString
  , _user     :: ByteString
  , _password :: Maybe ByteString
  } deriving (Eq, Ord, Show, Read)

connect_alt :: ByteString -> IO PQ.Connection
connect_alt = PQ.connectdb

connect :: ConnSettings -> IO PQ.Connection
connect (ConnSettings host db user Nothing) = PQ.connectdb $
  mconcat [ "dbname=" <> db , " host=" <> host , " user=" <> user ]
connect (ConnSettings host db user (Just password)) = PQ.connectdb $
  mconcat [ "dbname=" <> db , " host=" <> host , " user=" <> user, "password" <> password ]
