### ark
a sovereign archive.

ark is an experimental, distributed storage backend for jujutsu (jj). in other words, a strange file system.

inspired by [ink & switch's local-first principles](https://www.inkandswitch.com/essay/local-first/#seven-ideals-for-local-first-software), it's an attempt at a p2p fs that lives locally but syncs globally.

in short, we have [jj](https://github.com/jj-vcs/jj) manage conflict resolution with its version graph logic and our glue code handles the persistence and distribution.

when jj writes a commit, ark serializes the object using [rkyv](https://github.com/rkyv/rkyv) and pushes the raw bytes into [iroh](https://github.com/n0-computer/iroh). iroh returns a hash, which ark records in [redb](https://github.com/cberner/redb) alongside of the commit metadata.

by decoupling blob stores and the index, we're able to separate the repo history from the local fs:
- querying the op log requires only the local index (redb)
- reading files pulls in verified streams from the blob store (iroh)

notes:
- i built ark around the invariant that the _existence of metadata_ implies the _existence of data_. so, we have atomic operations to ensure an index is unable to point to... well, nothing.
- nodes gossip to discover new commits. syncing is pull based. unlike git, we don't push to a central remote.
- we don't trust the disk. rkyv validates every byte before it touches memory.
- in ark, _everything is a blob_. md files or large multimedia assets, it doesn't really matter. iroh will auto-chunk large files -> which means we get deduping + the ability to stream large assets w/o a full download out of the box.
