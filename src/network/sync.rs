// tldr: range-based set reconciliation over the sorted 256-bit hash space.
// two peers exchange fingerprints for progressively smaller ranges until they
// can enumerate the differences. transport-agnostic — callers feed messages in,
// get response messages out. after reconciliation, targeted fetch pulls missing
// blobs from remote peers via iroh-blobs downloader.
//
// algorithm (Aljoscha Meyer style):
//   1. each peer holds a sorted set of BlobHash values
//   2. fingerprint for a range = XOR of all hashes in that range
//   3. if fingerprints match, range is identical — skip
//   4. if they mismatch, split in half and recurse
//   5. below a threshold, just enumerate items and diff
//   6. targeted fetch: download missing blobs from remote peer

use std::collections::BTreeSet;

use bytecheck::CheckBytes;
use iroh::EndpointId;
use iroh_blobs::api::downloader::Downloader;
use iroh_blobs::store::fs::FsStore;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use thiserror::Error;

use crate::domain::BlobHash;

// ---------------------------------------------------------------------------
// Fingerprint
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize, CheckBytes)]
#[bytecheck(crate = bytecheck)]
pub struct Fingerprint(pub [u8; 32]);

impl Fingerprint {
    pub const ZERO: Self = Self([0u8; 32]);

    pub fn xor(self, other: Self) -> Self {
        let mut out = self.0;
        let b = other.0;
        for i in 0..32 {
            out[i] ^= b[i];
        }
        Self(out)
    }

    // @todo(o11y): hashing before XOR prevents structured collisions (e.g. sets
    //   with identical raw XOR but different elements). uses blake3 for speed.
    //   if profiling shows this is hot, consider caching the hashed values.
    fn xor_hash(self, hash: BlobHash) -> Self {
        let derived = blake3::hash(&hash.0);
        let mut out = self.0;
        let b = derived.as_bytes();
        for i in 0..32 {
            out[i] ^= b[i];
        }
        Self(out)
    }
}

// ---------------------------------------------------------------------------
// HashRange — inclusive [lower, upper] over the full 256-bit space
// ---------------------------------------------------------------------------

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Archive, RkyvSerialize, RkyvDeserialize,
    CheckBytes,
)]
#[bytecheck(crate = bytecheck)]
pub struct HashRange {
    pub lower: [u8; 32],
    pub upper: [u8; 32],
}

const MIN_HASH: [u8; 32] = [0u8; 32];
const MAX_HASH: [u8; 32] = [0xFFu8; 32];

impl HashRange {
    pub fn full() -> Self {
        Self {
            lower: MIN_HASH,
            upper: MAX_HASH,
        }
    }

    #[allow(dead_code)]
    fn contains(&self, hash: &BlobHash) -> bool {
        hash.0 >= self.lower && hash.0 <= self.upper
    }

    // @todo(o11y): split uses big-endian u256 midpoint arithmetic — correct but
    //   allocates via the carry loop. if profiling shows this is hot, switch to
    //   a branchless SIMD approach.
    pub fn split(&self) -> Option<(Self, Self)> {
        if self.lower >= self.upper {
            return None;
        }

        let mid = midpoint(&self.lower, &self.upper);

        if mid >= self.upper {
            return None;
        }

        let right_lower = increment(&mid)?;

        if right_lower > self.upper {
            return None;
        }

        Some((
            Self {
                lower: self.lower,
                upper: mid,
            },
            Self {
                lower: right_lower,
                upper: self.upper,
            },
        ))
    }
}

fn midpoint(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut sum = [0u16; 32];
    for i in 0..32 {
        sum[i] = a[i] as u16 + b[i] as u16;
    }
    let mut result = [0u8; 32];
    let mut carry = 0u16;
    for i in (0..32).rev() {
        let v = sum[i] + carry;
        result[i] = (v / 2) as u8;
        carry = if v % 2 == 1 { 256 } else { 0 };
    }
    result
}

fn increment(v: &[u8; 32]) -> Option<[u8; 32]> {
    let mut result = *v;
    for i in (0..32).rev() {
        let (new, overflow) = result[i].overflowing_add(1);
        result[i] = new;
        if !overflow {
            return Some(result);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Protocol messages
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize, CheckBytes)]
#[bytecheck(crate = bytecheck)]
pub struct RangeSummary {
    pub range: HashRange,
    pub count: u32,
    pub fingerprint: Fingerprint,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize, CheckBytes)]
#[bytecheck(crate = bytecheck)]
#[repr(u8)]
pub enum SyncMessage {
    Summary(RangeSummary),
    Items {
        range: HashRange,
        items: Vec<[u8; 32]>,
    },
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("Invalid range: lower > upper")]
    InvalidRange,
    #[error("Received item outside claimed range")]
    ItemOutOfRange,
    #[error("Fetch error: {0}")]
    Fetch(#[from] anyhow::Error),
}

// ---------------------------------------------------------------------------
// Reconciler — the state machine
// ---------------------------------------------------------------------------

// @todo(o11y): enumerate_threshold is hardcoded at construction — no way to
//   tune it per-range based on latency vs bandwidth tradeoff. acceptable for
//   now; revisit if reconciliation rounds become the bottleneck.
pub struct Reconciler {
    local: Vec<BlobHash>,
    prefix_xor: Vec<Fingerprint>,
    enumerate_threshold: usize,
    need_from_remote: BTreeSet<BlobHash>,
    have_for_remote: BTreeSet<BlobHash>,
    sent_items_for: BTreeSet<HashRange>,
}

impl Reconciler {
    pub fn new(mut local: Vec<BlobHash>, enumerate_threshold: usize) -> Self {
        local.sort();
        local.dedup();

        let mut prefix_xor = Vec::with_capacity(local.len() + 1);
        prefix_xor.push(Fingerprint::ZERO);
        for (i, h) in local.iter().enumerate() {
            prefix_xor.push(prefix_xor[i].xor_hash(*h));
        }

        Self {
            local,
            prefix_xor,
            enumerate_threshold,
            need_from_remote: BTreeSet::new(),
            have_for_remote: BTreeSet::new(),
            sent_items_for: BTreeSet::new(),
        }
    }

    pub fn initial_message(&self) -> SyncMessage {
        SyncMessage::Summary(self.summary_for(HashRange::full()))
    }

    pub fn need_from_remote(&self) -> &BTreeSet<BlobHash> {
        &self.need_from_remote
    }

    pub fn have_for_remote(&self) -> &BTreeSet<BlobHash> {
        &self.have_for_remote
    }

    pub fn process(&mut self, msg: SyncMessage) -> Result<Vec<SyncMessage>, SyncError> {
        match msg {
            SyncMessage::Summary(remote) => self.on_summary(remote),
            SyncMessage::Items { range, items } => self.on_items(range, items),
        }
    }

    fn on_summary(&mut self, remote: RangeSummary) -> Result<Vec<SyncMessage>, SyncError> {
        if remote.range.lower > remote.range.upper {
            return Err(SyncError::InvalidRange);
        }

        let local = self.summary_for(remote.range);

        if local.count == remote.count && local.fingerprint == remote.fingerprint {
            return Ok(Vec::new());
        }

        let max_n = (local.count as usize).max(remote.count as usize);

        if max_n <= self.enumerate_threshold {
            return Ok(self.send_items(remote.range));
        }

        match remote.range.split() {
            Some((left, right)) => Ok(vec![
                SyncMessage::Summary(self.summary_for(left)),
                SyncMessage::Summary(self.summary_for(right)),
            ]),
            None => Ok(self.send_items(remote.range)),
        }
    }

    fn on_items(
        &mut self,
        range: HashRange,
        items: Vec<[u8; 32]>,
    ) -> Result<Vec<SyncMessage>, SyncError> {
        if range.lower > range.upper {
            return Err(SyncError::InvalidRange);
        }

        for h in &items {
            if *h < range.lower || *h > range.upper {
                return Err(SyncError::ItemOutOfRange);
            }
        }

        let remote_set: BTreeSet<BlobHash> = items.iter().map(|h| BlobHash(*h)).collect();
        let local_items = self.items_in_range(range);

        for h in &remote_set {
            if !local_items.contains(h) {
                self.need_from_remote.insert(*h);
            }
        }

        for h in &local_items {
            if !remote_set.contains(h) {
                self.have_for_remote.insert(*h);
            }
        }

        Ok(self.send_items(range))
    }

    fn summary_for(&self, range: HashRange) -> RangeSummary {
        let start = self
            .local
            .partition_point(|h| h.0 < range.lower);
        let end = self
            .local
            .partition_point(|h| h.0 <= range.upper);

        let count = (end - start) as u32;
        let fingerprint = self.prefix_xor[end].xor(self.prefix_xor[start]);

        RangeSummary {
            range,
            count,
            fingerprint,
        }
    }

    fn items_in_range(&self, range: HashRange) -> Vec<BlobHash> {
        let start = self.local.partition_point(|h| h.0 < range.lower);
        let end = self.local.partition_point(|h| h.0 <= range.upper);
        self.local[start..end].to_vec()
    }

    fn send_items(&mut self, range: HashRange) -> Vec<SyncMessage> {
        if self.sent_items_for.contains(&range) {
            return Vec::new();
        }
        self.sent_items_for.insert(range);

        let items: Vec<[u8; 32]> = self
            .items_in_range(range)
            .iter()
            .map(|h| h.0)
            .collect();

        vec![SyncMessage::Items { range, items }]
    }
}

// ---------------------------------------------------------------------------
// simulate a full reconciliation between two peers (for testing / offline use)
// ---------------------------------------------------------------------------

// @todo(o11y): simulate_sync is bounded to MAX_ROUNDS to prevent infinite loops
//   on malformed state. if this fires in practice, it means the fingerprint
//   or split logic has a bug.
const MAX_ROUNDS: usize = 256;

pub struct SyncResult {
    pub a_needs: BTreeSet<BlobHash>,
    pub b_needs: BTreeSet<BlobHash>,
    pub rounds: usize,
}

pub fn simulate_sync(
    set_a: Vec<BlobHash>,
    set_b: Vec<BlobHash>,
    threshold: usize,
) -> Result<SyncResult, SyncError> {
    let mut a = Reconciler::new(set_a, threshold);
    let mut b = Reconciler::new(set_b, threshold);

    let mut pending_for_b = vec![a.initial_message()];
    let mut rounds = 0;

    while !pending_for_b.is_empty() && rounds < MAX_ROUNDS {
        rounds += 1;

        let mut next_for_a = Vec::new();
        for msg in pending_for_b {
            next_for_a.extend(b.process(msg)?);
        }

        let mut next_for_b = Vec::new();
        for msg in next_for_a {
            next_for_b.extend(a.process(msg)?);
        }

        pending_for_b = next_for_b;
    }

    Ok(SyncResult {
        a_needs: a.need_from_remote,
        b_needs: b.need_from_remote,
        rounds,
    })
}

// ---------------------------------------------------------------------------
// step 4: targeted fetch — download missing blobs from a remote peer
// ---------------------------------------------------------------------------

// @todo(o11y): fetch_missing_blobs downloads all missing hashes in a single
//   batch request. if the set is very large, this could be slow or OOM. consider
//   chunking into batches of N hashes if reconciliation diffs exceed a threshold.
pub async fn fetch_missing_blobs(
    store: &FsStore,
    endpoint: &iroh::Endpoint,
    missing: &BTreeSet<BlobHash>,
    providers: Vec<iroh::EndpointAddr>,
) -> Result<FetchResult, SyncError> {
    if missing.is_empty() {
        return Ok(FetchResult {
            fetched: 0,
            failed: Vec::new(),
        });
    }

    // @todo(o11y): seeding address info by connecting once per provider is
    //   heavy-handed. a lighter approach would be to use StaticProvider or
    //   add_endpoint_addr directly, but that API is pub(crate) in iroh 0.95.
    //   revisit when iroh exposes a public address registration method.
    for addr in &providers {
        let _ = endpoint.connect(addr.clone(), iroh_blobs::ALPN).await;
    }

    let provider_ids: Vec<EndpointId> = providers.iter().map(|a| a.id).collect();
    let downloader = Downloader::new(store, endpoint);
    let hashes: Vec<iroh_blobs::Hash> =
        missing.iter().map(|h| iroh_blobs::Hash::from(*h)).collect();

    let mut fetched = 0u64;
    let mut failed: Vec<BlobHash> = Vec::new();

    // @todo(o11y): downloading one-by-one is simple but suboptimal — the Downloader
    //   supports GetMany for batch requests. switching to batch would reduce round trips
    //   but complicates per-hash error tracking. acceptable for now.
    for hash in &hashes {
        let progress = downloader.download(*hash, provider_ids.clone());
        match progress.await {
            Ok(()) => {
                fetched += 1;
            }
            Err(_) => {
                failed.push(BlobHash(*hash.as_bytes()));
            }
        }
    }

    Ok(FetchResult { fetched, failed })
}

pub struct FetchResult {
    pub fetched: u64,
    pub failed: Vec<BlobHash>,
}
