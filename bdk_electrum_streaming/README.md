# Experimental BDK Electrum

Redo `bdk_electrum` using `e_electrum_client`.

The server is not trusted for chain data. You hand [`HeaderChain`] a set of trusted block headers;
everything above the highest one is downloaded and checked for linkage, proof-of-work, and the
difficulty consensus requires before it becomes part of the chain. A reorg is only accepted if it
brings more work than the blocks it replaces, and never if it would drop a trusted block.
Transactions are merkle-proved against those headers, and the proof travels with the anchor
([`ProvenAnchor`]).

The highest trusted block must sit on a difficulty-adjustment boundary (`height % 2016 == 0`) on
networks where difficulty moves, so every retarget above it can be recomputed rather than taken on
faith.

A transaction confirmed below the sync start triggers a backfill: headers are fetched from just
above the highest trusted block below it, up to where the chain already begins.
