# LiveDB built on top of foundationdb

> This is heavily experimental, and probably broken.

This is an implementation of [livedb](http://github.com/share/livedb) built on
top of [foundationdb](https://foundationdb.com) (which I really like).
Initially I wanted to implement foundationdb as simply a storage backend, but
its `watch` API can be used to replace redis too. Until I have a pluggable way
to replace redis, this will exist as a standalone drop-in replacement for
livedb.



