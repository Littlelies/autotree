autotree
=====

automatically manage publishes on a tree of topics and their subscribers.
Local instance, in memory tables, high performance (~25k inserts per second on MacBook Pro i5 2,6GHz).


Abstract
-----

Features a [topic based publish–subscribe pattern](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern), each topic being a list of terms, so subscribers get update events about any sub tree, a bit like AMQT.
Uses [CmRDT](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type) for merging updates.

[Spread](https://github.com/Littlelies/spread) is built on top of it, adding persistence and distribution.


Build
-----

    $ rebar3 compile
