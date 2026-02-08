use std::collections::BTreeSet;
use std::time::Duration;

use ark::domain::{BlobHash, BlobMetadata, BlobType, RefName};
use ark::network::sync::{
    fetch_missing_blobs, simulate_sync, Fingerprint, HashRange, Reconciler, SyncMessage,
};
use ark::network::{derive_topic_id, HeadUpdate};
use ark::storage::BlobStore;
use camino::Utf8Path;
use iroh::discovery::static_provider::StaticProvider;
use tokio::sync::broadcast;

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

fn commit_meta(ts: u64) -> BlobMetadata {
    BlobMetadata {
        blob_type: BlobType::Commit,
        created_at: ts,
        local_only: false,
    }
}

fn seed_addresses(stores: &[&BlobStore]) {
    for (i, store_i) in stores.iter().enumerate() {
        let provider = StaticProvider::new();
        for (j, store_j) in stores.iter().enumerate() {
            if i != j {
                provider.add_endpoint_info(store_j.blobs.endpoint.addr());
            }
        }
        store_i.blobs.endpoint.discovery().add(provider);
    }
}

async fn wait_for_updates(
    rx: &mut broadcast::Receiver<HeadUpdate>,
    want: &[BlobHash],
    timeout: Duration,
) {
    let mut seen = BTreeSet::new();
    let target = want.len();

    let result = tokio::time::timeout(timeout, async {
        while seen.len() < target {
            match rx.recv().await {
                Ok(u) => {
                    let h = u.hash();
                    if want.contains(&h) {
                        seen.insert(h);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    continue;
                }
                Err(e) => {
                    panic!("gossip recv failed: {e:?}");
                }
            }
        }
    })
    .await;

    result.expect("timed out waiting for gossip propagation");
}

async fn local_blob_set(store: &BlobStore, candidates: &[BlobHash]) -> Vec<BlobHash> {
    let mut set = Vec::new();
    for h in candidates {
        if store.blobs.get_bytes(*h).await.is_ok() {
            set.push(*h);
        }
    }
    set
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

// --- 5-node reconciliation (offline) ---

#[test]
fn five_node_reconciliation_offline() {
    let common: Vec<BlobHash> = (0..10u8).map(|s| BlobHash([s; 32])).collect();
    let extra_a = BlobHash([200u8; 32]);
    let extra_b = BlobHash([201u8; 32]);

    let mut sets: Vec<Vec<BlobHash>> = Vec::new();

    // node A: common + extra_a
    let mut a = common.clone();
    a.push(extra_a);
    sets.push(a);

    // node B: common + extra_b
    let mut b = common.clone();
    b.push(extra_b);
    sets.push(b);

    // nodes C, D, E: only common
    for _ in 0..3 {
        sets.push(common.clone());
    }

    let max_rounds = 10;
    for _round in 0..max_rounds {
        let mut progress = false;

        for i in 0..5 {
            for j in (i + 1)..5 {
                let result = simulate_sync(sets[i].clone(), sets[j].clone(), 8)
                    .expect("simulate_sync failed");

                for h in &result.a_needs {
                    if !sets[i].contains(h) {
                        sets[i].push(*h);
                        progress = true;
                    }
                }

                for h in &result.b_needs {
                    if !sets[j].contains(h) {
                        sets[j].push(*h);
                        progress = true;
                    }
                }
            }
        }

        if !progress {
            break;
        }
    }

    for (idx, set) in sets.iter().enumerate() {
        let unique: BTreeSet<BlobHash> = set.iter().copied().collect();
        assert_eq!(
            unique.len(),
            12,
            "node {idx} should have 12 blobs, has {}",
            unique.len()
        );
        assert!(unique.contains(&extra_a), "node {idx} missing extra_a");
        assert!(unique.contains(&extra_b), "node {idx} missing extra_b");
    }
}

// --- 5-node convergence (test protocol 2.1) ---

#[tokio::test]
async fn five_node_convergence() {
    let (_dir_a, store_a) = tmp_store().await;
    let (_dir_b, store_b) = tmp_store().await;
    let (_dir_c, store_c) = tmp_store().await;
    let (_dir_d, store_d) = tmp_store().await;
    let (_dir_e, store_e) = tmp_store().await;

    let stores = [&store_a, &store_b, &store_c, &store_d, &store_e];

    seed_addresses(&stores);

    let ids: Vec<_> = stores.iter().map(|s| s.blobs.endpoint.id()).collect();
    let addrs: Vec<_> = stores.iter().map(|s| s.blobs.endpoint.addr()).collect();

    let topic = derive_topic_id(b"convergence-5-node-test");

    store_a
        .gossip
        .join_topic(topic, ids[1..].to_vec())
        .await
        .expect("A join_topic failed");

    for store in &stores[1..] {
        store
            .gossip
            .join_topic(topic, vec![ids[0]])
            .await
            .expect("join_topic failed");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut rx_c = store_c
        .gossip
        .subscribe_updates(topic)
        .expect("C subscribe failed");
    let mut rx_d = store_d
        .gossip
        .subscribe_updates(topic)
        .expect("D subscribe failed");
    let mut rx_e = store_e
        .gossip
        .subscribe_updates(topic)
        .expect("E subscribe failed");

    let data_x = b"commit: feature x - distributed archival";
    let hash_x = store_a
        .put_object(data_x, commit_meta(100))
        .await
        .expect("A put_object failed");

    let data_y = b"commit: feature y - peer discovery";
    let hash_y = store_b
        .put_object(data_y, commit_meta(200))
        .await
        .expect("B put_object failed");

    let ref_x = RefName::new("feature/x").expect("bad ref name");
    let ref_y = RefName::new("feature/y").expect("bad ref name");

    store_a
        .update_reference_and_announce(ref_x, hash_x, topic)
        .await
        .expect("A announce failed");

    store_b
        .update_reference_and_announce(ref_y, hash_y, topic)
        .await
        .expect("B announce failed");

    let timeout = Duration::from_secs(10);
    let want = [hash_x, hash_y];

    tokio::join!(
        wait_for_updates(&mut rx_c, &want, timeout),
        wait_for_updates(&mut rx_d, &want, timeout),
        wait_for_updates(&mut rx_e, &want, timeout),
    );

    // @todo(o11y): pairwise O(n^2) reconciliation is fine for 5 nodes and 2 blobs.
    //   in production, reconciliation would be triggered by gossip events, not polled.
    let max_rounds = 5;
    for _round in 0..max_rounds {
        let mut progress = false;

        for i in 0..5 {
            for j in (i + 1)..5 {
                let set_i = local_blob_set(stores[i], &[hash_x, hash_y]).await;
                let set_j = local_blob_set(stores[j], &[hash_x, hash_y]).await;

                let result =
                    simulate_sync(set_i, set_j, 8).expect("simulate_sync failed");

                if !result.a_needs.is_empty() {
                    let fr = fetch_missing_blobs(
                        &stores[i].blobs.store,
                        &stores[i].blobs.endpoint,
                        &result.a_needs,
                        vec![addrs[j].clone()],
                    )
                    .await
                    .expect("fetch for i failed");
                    progress |= fr.fetched > 0;
                }

                if !result.b_needs.is_empty() {
                    let fr = fetch_missing_blobs(
                        &stores[j].blobs.store,
                        &stores[j].blobs.endpoint,
                        &result.b_needs,
                        vec![addrs[i].clone()],
                    )
                    .await
                    .expect("fetch for j failed");
                    progress |= fr.fetched > 0;
                }
            }
        }

        if !progress {
            break;
        }
    }

    for (idx, store) in stores.iter().enumerate() {
        let bytes_x = store
            .blobs
            .get_bytes(hash_x)
            .await
            .unwrap_or_else(|e| panic!("node {idx} missing feature X blob: {e}"));
        assert_eq!(bytes_x, data_x, "node {idx} has wrong data for feature X");

        let bytes_y = store
            .blobs
            .get_bytes(hash_y)
            .await
            .unwrap_or_else(|e| panic!("node {idx} missing feature Y blob: {e}"));
        assert_eq!(bytes_y, data_y, "node {idx} has wrong data for feature Y");
    }

    for store in &stores {
        store.shutdown().await.expect("shutdown failed");
    }
}
