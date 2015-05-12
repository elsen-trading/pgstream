PgStream
========

0.1.0.2
-------

Exposes connection status information for errors.

0.1.0.1
-------

Adds:

Generalizes ``stream`` and ``stream_`` interface to have
[MonadBaseControl](https://hackage.haskell.org/package/resourcet-1.1.4.1/docs/Control-Monad-Trans-Resource.html)
in context to generalize over hardcoded ``ResourceT IO``.

0.1.0.0
-------

Initial stable commit.

Adds:

* Basic Postgres connection pooling.
* Postgres query construction.
* Postgres type conversion.
* Efficient conversion for Postgres vectors (limited to float/int types).
* Streaming interface through Conduit.
* Parallel streaming interface through STM.

Fixes:

NA

Deprecates:

NA
