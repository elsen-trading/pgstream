{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE OverloadedStrings #-}

module Example where

import Data.Int
import Data.Text
import Data.Monoid
import Data.ByteString
import Data.Vector.Storable

import Database.PostgreSQL.Stream
import Control.Monad.Trans.Resource (runResourceT)

import Data.Conduit hiding (connect)
import qualified Data.Conduit.List as CL

-------------------------------------------------------------------------------
-- Query Construction
-------------------------------------------------------------------------------

sample :: Query
sample = [sql|
    SELECT
      deltas.sid AS sid,
      EXTRACT(EPOCH FROM deltas.day) AS day,
      (ohlcs :: float4[])
    FROM deltas
    INNER JOIN security_groupings ON deltas.sid = security_groupings.sid
    INNER JOIN currentprice ON (
      deltas.sid = currentprice.sid
      AND deltas.DAY = currentprice.DAY
      AND currentprice.val BETWEEN 0 AND 500
    )
    WHERE security_groupings.name = 'SP900'
    AND deltas.day BETWEEN TO_TIMESTAMP({1}) AND TO_TIMESTAMP({2})
    ORDER BY deltas.sid,
             deltas.DAY ASC
    ;
|]

creds :: ConnSettings
creds = ConnSettings {_host = "localhost", _dbname="invest", _user="dbadmin"}

sampleFmt :: ByteString
sampleFmt = fmtQuery sample (1335855600 :: Int, 1338447600 :: Int, "LIMIT 50000" :: SQL)

type Row = (Text, Int32, Vector Float)

args :: (Int, Int, SQL)
args = (
    1335855600
  {-, 1338447600-}
  , 1336374000
  , "LIMIT 100000"
  )

-------------------------------------------------------------------------------
-- Singular Query
-------------------------------------------------------------------------------

runSample :: IO [Row]
runSample = do
  conn <- connect creds
  query conn sample args

-------------------------------------------------------------------------------
-- Streaming Query
-------------------------------------------------------------------------------

runStream :: IO [[Row]]
runStream = do
  conn <- connect creds
  res <- runResourceT $ (stream conn sample args 50000 $$ CL.consume)
  Prelude.putStrLn $ "Finisehd stream"
  Prelude.putStrLn $ "Batches: " <> (show $ Prelude.length res)
  return res

main = do
  runStream
  return ()
