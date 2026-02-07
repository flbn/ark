use std::collections::BTreeSet;

use ark::domain::{BlobHash, BlobMetadata, BlobType};
use ark::network::sync::{
    fetch_missing_blobs, simulate_sync, Fingerprint, HashRange, Reconciler, SyncMessage,
};
use ark::storage::BlobStore;
use camino::Utf8Path;

fn hash(seed: u8) -> BlobHash {
    BlobHash([seed; 32])
}

fn hashes(seeds: &[u8]) -> Vec<BlobHash> {
    seeds.iter().map(|s| hash(*s)).collect()
}

// --- Fingerprint ---

#[test]
fn fingerprint_xor_identity() {
    let a = Fingerprint([42u8; 32]);
    assert_eq!(a.xor(Fingerprint::ZERO), a);
}

#[test]
fn fingerprint_xor_self_is_zero() {
    let a = Fingerprint([0xAB; 32]);
    assert_eq!(a.xor(a), Fingerprint::ZERO);
}

#[test]
fn fingerprint_xor_commutative() {
    let a = Fingerprint([1u8; 32]);
    let b = Fingerprint([2u8; 32]);
    assert_eq!(a.xor(b), b.xor(a));
}

#[test]
fn fingerprint_xor_associative() {
    let a = Fingerprint([1u8; 32]);
    let b = Fingerprint([2u8; 32]);
    let c = Fingerprint([3u8; 32]);
    assert_eq!(a.xor(b).xor(c), a.xor(b.xor(c)));
}

// --- HashRange ---

#[test]
fn full_range_splits() {
    let (left, right) = HashRange::full().split().expect("split failed");
    assert_eq!(left.lower, [0u8; 32]);
    assert!(left.upper < right.lower);
    assert_eq!(right.upper, [0xFF; 32]);
}

#[test]
fn single_point_range_cannot_split() {
    let range = HashRange {
        lower: [5u8; 32],
        upper: [5u8; 32],
    };
    assert!(range.split().is_none());
}

#[test]
fn adjacent_range_cannot_split() {
    let mut upper = [0u8; 32];
    upper[31] = 1; // differs by 1 in the least significant byte (big-endian)
    let range = HashRange {
        lower: [0u8; 32],
        upper,
    };
    assert!(range.split().is_none());
}

// --- Reconciler basics ---

#[test]
fn initial_message_is_summary() {
    let r = Reconciler::new(hashes(&[1, 2, 3]), 8);
    match r.initial_message() {
        SyncMessage::Summary(s) => {
            assert_eq!(s.range, HashRange::full());
            assert_eq!(s.count, 3);
        }
        _ => panic!("expected Summary"),
    }
}

#[test]
fn identical_sets_produce_no_messages() {
    let set = hashes(&[10, 20, 30]);
    let mut a = Reconciler::new(set.clone(), 8);
    let mut b = Reconciler::new(set, 8);

    let msgs = b.process(a.initial_message()).expect("process failed");

    let mut total_responses = 0;
    for msg in msgs {
        let responses = a.process(msg).expect("process failed");
        total_responses += responses.len();
    }

    assert!(a.need_from_remote().is_empty());
    assert!(a.have_for_remote().is_empty());
    assert_eq!(total_responses, 0);
}

#[test]
fn empty_sets_produce_no_messages() {
    let a = Reconciler::new(Vec::new(), 8);
    let mut b = Reconciler::new(Vec::new(), 8);

    let msgs = b.process(a.initial_message()).expect("process failed");
    assert!(msgs.is_empty());
}

// --- simulate_sync: disjoint sets ---

#[test]
fn disjoint_sets_finds_all_missing() {
    let a_set = hashes(&[1, 2, 3]);
    let b_set = hashes(&[4, 5, 6]);

    let result = simulate_sync(a_set.clone(), b_set.clone(), 8).expect("sync failed");

    for h in &b_set {
        assert!(result.a_needs.contains(h), "a should need {h:?}");
    }
    for h in &a_set {
        assert!(result.b_needs.contains(h), "b should need {h:?}");
    }
    assert_eq!(result.a_needs.len(), 3);
    assert_eq!(result.b_needs.len(), 3);
}

// --- simulate_sync: overlapping sets ---

#[test]
fn overlapping_sets_finds_only_missing() {
    let shared = hashes(&[10, 20, 30]);
    let a_only = hashes(&[1, 2]);
    let b_only = hashes(&[40, 50]);

    let mut a_set = shared.clone();
    a_set.extend(&a_only);
    let mut b_set = shared;
    b_set.extend(&b_only);

    let result = simulate_sync(a_set, b_set, 8).expect("sync failed");

    for h in &b_only {
        assert!(result.a_needs.contains(h), "a should need {h:?}");
    }
    for h in &a_only {
        assert!(result.b_needs.contains(h), "b should need {h:?}");
    }
    assert_eq!(result.a_needs.len(), 2);
    assert_eq!(result.b_needs.len(), 2);
}

// --- simulate_sync: one empty ---

#[test]
fn one_empty_set() {
    let a_set = hashes(&[1, 2, 3, 4, 5]);
    let b_set = Vec::new();

    let result = simulate_sync(a_set.clone(), b_set, 8).expect("sync failed");

    assert!(result.a_needs.is_empty());
    for h in &a_set {
        assert!(result.b_needs.contains(h));
    }
    assert_eq!(result.b_needs.len(), 5);
}

#[test]
fn other_empty_set() {
    let a_set = Vec::new();
    let b_set = hashes(&[10, 20]);

    let result = simulate_sync(a_set, b_set.clone(), 8).expect("sync failed");

    for h in &b_set {
        assert!(result.a_needs.contains(h));
    }
    assert!(result.b_needs.is_empty());
}

// --- simulate_sync: identical sets ---

#[test]
fn identical_sets_nothing_missing() {
    let set = hashes(&[5, 10, 15, 20]);

    let result = simulate_sync(set.clone(), set, 8).expect("sync failed");

    assert!(result.a_needs.is_empty());
    assert!(result.b_needs.is_empty());
    assert_eq!(result.rounds, 1);
}

// --- simulate_sync: single item difference ---

#[test]
fn single_item_difference() {
    let common: Vec<BlobHash> = (0..20u8).map(hash).collect();
    let mut a_set = common.clone();
    let extra_a = hash(200);
    a_set.push(extra_a);

    let mut b_set = common;
    let extra_b = hash(201);
    b_set.push(extra_b);

    let result = simulate_sync(a_set, b_set, 8).expect("sync failed");

    assert_eq!(result.a_needs.len(), 1);
    assert!(result.a_needs.contains(&extra_b));
    assert_eq!(result.b_needs.len(), 1);
    assert!(result.b_needs.contains(&extra_a));
}

// --- simulate_sync: large sets ---

#[test]
fn large_overlapping_sets() {
    let common: Vec<BlobHash> = (0..100u8).map(hash).collect();
    let a_extra: Vec<BlobHash> = (200..210u8).map(hash).collect();
    let b_extra: Vec<BlobHash> = (220..230u8).map(hash).collect();

    let mut a_set = common.clone();
    a_set.extend(&a_extra);
    let mut b_set = common;
    b_set.extend(&b_extra);

    let result = simulate_sync(a_set, b_set, 8).expect("sync failed");

    for h in &b_extra {
        assert!(result.a_needs.contains(h));
    }
    for h in &a_extra {
        assert!(result.b_needs.contains(h));
    }
    assert_eq!(result.a_needs.len(), 10);
    assert_eq!(result.b_needs.len(), 10);
}

// --- simulate_sync: threshold affects rounds ---

#[test]
fn higher_threshold_fewer_rounds() {
    let a_set: Vec<BlobHash> = (0..50u8).map(hash).collect();
    let b_set: Vec<BlobHash> = (25..75u8).map(hash).collect();

    let r_small = simulate_sync(a_set.clone(), b_set.clone(), 4).expect("sync failed");
    let r_large = simulate_sync(a_set, b_set, 64).expect("sync failed");

    assert!(r_large.rounds <= r_small.rounds);

    assert_eq!(r_small.a_needs, r_large.a_needs);
    assert_eq!(r_small.b_needs, r_large.b_needs);
}

// --- simulate_sync: duplicates in input ---

#[test]
fn duplicates_in_input_handled() {
    let a_set = vec![hash(1), hash(1), hash(2), hash(2), hash(3)];
    let b_set = vec![hash(2), hash(3), hash(4)];

    let result = simulate_sync(a_set, b_set, 8).expect("sync failed");

    assert_eq!(result.a_needs.len(), 1);
    assert!(result.a_needs.contains(&hash(4)));
    assert_eq!(result.b_needs.len(), 1);
    assert!(result.b_needs.contains(&hash(1)));
}

// --- message serialization round-trip ---

#[test]
fn sync_message_summary_rkyv_roundtrip() {
    let msg = SyncMessage::Summary(ark::network::sync::RangeSummary {
        range: HashRange::full(),
        count: 42,
        fingerprint: Fingerprint([0xAB; 32]),
    });

    let bytes = rkyv::to_bytes::<rancor::Error>(&msg).expect("serialize failed");
    let decoded =
        rkyv::from_bytes::<SyncMessage, rancor::Error>(&bytes).expect("deserialize failed");
    assert_eq!(decoded, msg);
}

#[test]
fn sync_message_items_rkyv_roundtrip() {
    let msg = SyncMessage::Items {
        range: HashRange::full(),
        items: vec![[1u8; 32], [2u8; 32]],
    };

    let bytes = rkyv::to_bytes::<rancor::Error>(&msg).expect("serialize failed");
    let decoded =
        rkyv::from_bytes::<SyncMessage, rancor::Error>(&bytes).expect("deserialize failed");
    assert_eq!(decoded, msg);
}

// --- error cases ---

#[test]
fn process_invalid_range_errors() {
    let mut r = Reconciler::new(hashes(&[1, 2, 3]), 8);
    let bad_msg = SyncMessage::Summary(ark::network::sync::RangeSummary {
        range: HashRange {
            lower: [0xFF; 32],
            upper: [0x00; 32],
        },
        count: 1,
        fingerprint: Fingerprint::ZERO,
    });
    assert!(r.process(bad_msg).is_err());
}

#[test]
fn process_item_out_of_range_errors() {
    let mut r = Reconciler::new(hashes(&[1, 2, 3]), 8);
    let bad_msg = SyncMessage::Items {
        range: HashRange {
            lower: [0x10; 32],
            upper: [0x20; 32],
        },
        items: vec![[0xFF; 32]],
    };
    assert!(r.process(bad_msg).is_err());
}

// --- targeted fetch ---

async fn tmp_store() -> (tempfile::TempDir, BlobStore) {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let root = Utf8Path::from_path(dir.path())
        .expect("non-utf8 temp path")
        .to_owned();
    let store = BlobStore::new(&root)
        .await
        .expect("failed to create store");
    (dir, store)
}

#[tokio::test]
async fn fetch_empty_set_is_noop() {
    let (_dir, store) = tmp_store().await;
    let empty = BTreeSet::new();

    let result = fetch_missing_blobs(&store.blobs.store, &store.blobs.endpoint, &empty, vec![])
        .await
        .expect("fetch failed");

    assert_eq!(result.fetched, 0);
    assert!(result.failed.is_empty());

    store.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
async fn fetch_from_local_peer() {
    let (_dir_a, store_a) = tmp_store().await;
    let (_dir_b, store_b) = tmp_store().await;

    let data = b"blob to transfer via fetch";
    let meta = BlobMetadata {
        blob_type: BlobType::File,
        created_at: 1,
        local_only: false,
    };
    let hash = store_a
        .put_object(data, meta)
        .await
        .expect("put failed");

    let mut missing = BTreeSet::new();
    missing.insert(hash);

    let provider_addr = store_a.blobs.endpoint.addr();

    let result = fetch_missing_blobs(
        &store_b.blobs.store,
        &store_b.blobs.endpoint,
        &missing,
        vec![provider_addr],
    )
    .await
    .expect("fetch failed");

    assert_eq!(result.fetched, 1);
    assert!(result.failed.is_empty());

    let fetched_bytes = store_b
        .blobs
        .get_bytes(hash)
        .await
        .expect("read fetched blob failed");
    assert_eq!(fetched_bytes, data);

    store_a.shutdown().await.expect("shutdown a failed");
    store_b.shutdown().await.expect("shutdown b failed");
}

#[tokio::test]
async fn fetch_multiple_blobs() {
    let (_dir_a, store_a) = tmp_store().await;
    let (_dir_b, store_b) = tmp_store().await;

    let mut missing = BTreeSet::new();
    let mut expected: Vec<(BlobHash, Vec<u8>)> = Vec::new();

    for i in 0..5u8 {
        let data = format!("blob {i}").into_bytes();
        let meta = BlobMetadata {
            blob_type: BlobType::File,
            created_at: i as u64,
            local_only: false,
        };
        let hash = store_a
            .put_object(&data, meta)
            .await
            .expect("put failed");
        missing.insert(hash);
        expected.push((hash, data));
    }

    let provider_addr = store_a.blobs.endpoint.addr();

    let result = fetch_missing_blobs(
        &store_b.blobs.store,
        &store_b.blobs.endpoint,
        &missing,
        vec![provider_addr],
    )
    .await
    .expect("fetch failed");

    assert_eq!(result.fetched, 5);
    assert!(result.failed.is_empty());

    for (hash, data) in &expected {
        let fetched = store_b
            .blobs
            .get_bytes(*hash)
            .await
            .expect("read failed");
        assert_eq!(fetched, *data);
    }

    store_a.shutdown().await.expect("shutdown a failed");
    store_b.shutdown().await.expect("shutdown b failed");
}
