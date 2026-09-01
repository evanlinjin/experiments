# bdk_electrum_streaming

An async/blocking Electrum client for [BDK](https://bitcoindevkit.org), built as an explicit
state machine on top of [`electrum_streaming_client`](https://docs.rs/electrum_streaming_client).

Tracks a set of descriptors, subscribes to their script hashes, and streams `FullScanResponse`
updates as history, transactions and anchors resolve, instead of blocking until an entire scan
completes. Handles reorgs by refetching whichever anchors they affect.
