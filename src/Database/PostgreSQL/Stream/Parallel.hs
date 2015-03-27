module Database.PostgreSQL.Stream.Parallel (
  parallelStream,
) where

import Control.Monad.Trans
import Control.Concurrent
import Control.Concurrent.STM

import Data.Conduit
import Data.Conduit.TMChan
import Data.Conduit.TQueue
import Control.Monad.Trans.Resource (runResourceT, ResourceT)

import Database.PostgreSQL.Stream.Connection
import qualified Database.PostgreSQL.LibPQ as PQ

parallelStream ::
  PQ.Connection
  -> (PQ.Connection -> Source (ResourceT IO) a)  -- Source
  -> Sink a (ResourceT IO) ()                    -- Sink
  -> IO ()
parallelStream conn producer consumer = do
  chan <- atomically $ newTBMChan 32
  withPgConnection conn $ \c -> do

    tid <- forkIO . runResourceT
         $ producer c
        $$ sinkTBMChan chan True

    res <- runResourceT
         $ sourceTBMChan chan
        $$ consumer

    print res
