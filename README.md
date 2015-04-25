<p align="center">
  <a href="http://elsen.co">
    <img src="https://elsen.co/img/apple-touch-icon-144x144.png"/>
  </a>
</p>

pgstream
========

Streaming of Postgres through the binary protocol into Haskell. Uses attoparsec
and some hand-written kernels for array extraction. Results are streamed into
vectors or batched into serial or parallel
[Conduit](https://hackage.haskell.org/package/conduit-1.2.4/docs/Data-Conduit.html)
pipelines for stream composition.

[![Build Status](https://magnum.travis-ci.com/elsen-trading/pgstream.svg?token=BpJfxk8kj7YxSxz44Sq9&branch=master)](https://magnum.travis-ci.com/elsen-trading/pgstream)

Installation
------------

```bash
$ cabal install pg_stream.cabal
```

Usage
-----

**Connections**

Connections to Postgres are established with the ``connect`` function yielding
the connection object.

```haskell
connect :: ConnSettings -> IO PQ.Connection
```

Connections are specified by a ConnSettings.

```haskell
creds :: ConnSettings
creds = ConnSettings {_host = "localhost", _dbname="invest", _user="dbadmin"}
```

Connections are pooled per process. Connection pooling is specified by three
parameters.

* **Stripes**: Stripe count. The number of distinct sub-pools to maintain. The smallest acceptable value is 1.
* **Keep Alive**: Amount of time for which an unused resource is kept open. The smallest acceptable value is 0.5 seconds.
* **Affinity**: Maximum number of resources to keep open per stripe. The smallest acceptable value is 1.

The default settings are:

```haskell
defaultPoolSettings :: PoolSettings
defaultPoolSettings = PoolSettings { _stripes = 1, _keepalive = 10, _affinity = 10 }
```


**Queries**

Queries are executed using ``query`` for statements that yield result sets or by
``execute`` for queries that return a status code.

```haskell
query :: (FromRow r, ToSQL a) => PQ.Connection -> Query -> a -> IO [r]
execute :: (ToSQL a) => PQ.Connection -> Query -> a -> IO ()
```

For example:

```haskell
run :: IO [Row]
run = do
  conn <- connect creds
  query conn sample args
```

SQL queries are constructed via quasiquoter (``[sql| ... |]``) which generates a
``Query`` (newtype around a bytestring). Values and SQL fragments can be spliced
into this template as arguments.

```haskell
{-# LANGUAGE QuasiQuotes #-}

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
    {3}
    ;
|]
```

**Arguments**

If the types of arguments are constrained by inference then no annotations are
necessary. Otherwise annotations are needed to refine the Num/String instances
into concrete types so they can be serialized and sent to Postgres.

```haskell
args :: (Int, Int, SQL)
args = ( 1335855600 , 1336374000 , "LIMIT 100000")
```

The conversion from Haskell to Postgres types is defined by the
FromField/ToField typeclasses with the mapping given by.

| Postgres      | Haskell       |
| ------------- |---------------|
| int2          | Int8          |
| int4          | Int32         |
| int8          | Int64         |
| float4        | Float         |
| float8        | Double        |
| numeric       | Scientific    |
| uuid          | UUID          |
| char          | Char          |
| text          | Text          |
| bytea         | ByteString    |
| bool          | Bool          |
| int4[]        | Vector Int32  |
| float4[]      | Vector Float  |
| nullable a    | Maybe a       |

If the result set type is given as ``Maybe a`` then any missing value are
manifest as ``Nothing``  values. And all concrete values are ``Just``.
Effectively makes errors from null values used in unchecked logic
unrepresentable as any function which consumes a potentially nullable field is
forced by the type system to handle both cases.

**Streaming**

```haskell
stream :: (FromRow r, ToSQL a, MonadBaseControl IO m, MonadIO m) =>
     PQ.Connection       -- ^ Connection
  -> Query               -- ^ Query
  -> a                   -- ^ Query arguments
  -> Int                 -- ^ Batch size
  -> C.Source m [r]      -- ^ Source conduit
```

Parallel streams can be composed together Software Transactional Memory (STM)
threads to synchronize the polling.

```haskell
import Database.PostgreSQL.Stream.Parallel

parallelStream ::
  PQ.Connection
  -> (PQ.Connection -> Source (ResourceT IO) a)  -- Source
  -> Sink a (ResourceT IO) ()                    -- Sink
  -> IO ()
```

Development
-----------

```bash
$ cabal sandbox init
$ cabal install --only-dependencies
$ cabal build
```

To attach to the Elsen compute engine:

```bash
$ cabal sandbox add-source path_to_tree
```

Documentation
--------------

```bash
$ cabal haddock
```

Legal
-----

Copyright (c) 2015 Elsen Inc. All rights reserved.
