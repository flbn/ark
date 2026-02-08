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

#### runtime

ark is a config-driven daemon. the binary reads `ark.toml` and runs.

a client is a listener. it joins gossip topics, subscribes to heaed update messages and fetches referenced blobs from the announcing peer. there's no polling loop, syncing is reactive. when a writer commits, it broadcasts the update via gossip. every subscribed client receives the event and pulls the missing blob.

range based set reconciliation exists for two cases only: cold start catch up (new client joining an existing network) and recovery after network partition. it is **not** part of the normal sync flow.

there is no `sync` command, no `add-remote`, no imperative interface. remotes and topics are declared in config. a reasonable cli is deferred until there's a reason to build one. for now, its an extremely limited surface.

#### usage

get your node's endpoint id:

```
ark id
```

give this to peers so they can add you as a remote in their `ark.toml`.

the endpoint id is also written to `<store.path>/endpoint_id` on every startup.

#### configuration

```toml
[store]
path = "./data"

[[repos]]
id = "my-project"
remotes = [
    { name = "peer-a", node_id = "ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6" },
]

[sync]
enumerate_threshold = 8   # optional, default 8
```

- `store.path` — where blobs and the redb index live on disk.
- `repos[].id` — identifier for the repo. peers must use the same id to sync.
- `repos[].remotes[].node_id` — hex-encoded endpoint id of a peer (from `ark id`).
- `sync.enumerate_threshold` — item count below which reconciliation enumerates instead of splitting ranges.

run `ark` to start the daemon. use `--config path/to/ark.toml` to specify a different config path.
