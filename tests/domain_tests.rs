use ark::domain::{BlobHash, BlobMetadata, BlobType, EmptyRefName, EmptyRemoteTicket, RefName, RemoteTicket};

// --- BlobHash ---

#[test]
fn blob_hash_iroh_roundtrip() {
    let original = BlobHash([0xAB; 32]);
    let iroh_hash: iroh_blobs::Hash = original.into();
    let back: BlobHash = iroh_hash.into();
    assert_eq!(original, back);
}

#[test]
fn blob_hash_ordering() {
    let a = BlobHash([0x00; 32]);
    let b = BlobHash([0xFF; 32]);
    assert!(a < b);
}

// --- RefName ---

#[test]
fn refname_valid() {
    let r = RefName::new("main");
    assert!(r.is_ok());
    assert_eq!(r.expect("checked above").as_str(), "main");
}

#[test]
fn refname_empty_rejected() {
    let r = RefName::new("");
    assert!(r.is_err());
    let err = r.unwrap_err();
    assert!(matches!(err, EmptyRefName));
}

// --- RemoteTicket ---

#[test]
fn remote_ticket_valid() {
    let t = RemoteTicket::new(vec![1, 2, 3]);
    assert!(t.is_ok());
    assert_eq!(t.expect("checked above").as_bytes(), &[1, 2, 3]);
}

#[test]
fn remote_ticket_empty_rejected() {
    let t = RemoteTicket::new(vec![]);
    assert!(t.is_err());
    let err = t.unwrap_err();
    assert!(matches!(err, EmptyRemoteTicket));
}

// --- BlobMetadata rkyv roundtrip ---

#[test]
fn blob_metadata_rkyv_roundtrip() {
    let meta = BlobMetadata {
        blob_type: BlobType::Commit,
        created_at: 1735689600,
        local_only: false,
    };

    let bytes =
        rkyv::to_bytes::<rancor::Error>(&meta).expect("serialize failed");

    let deserialized: BlobMetadata =
        rkyv::from_bytes::<BlobMetadata, rancor::Error>(&bytes)
            .expect("from_bytes failed");

    assert_eq!(deserialized.created_at, 1735689600);
    assert!(matches!(deserialized.blob_type, BlobType::Commit));
}

#[test]
fn blob_metadata_rkyv_all_types() {
    for bt in [BlobType::Commit, BlobType::Tree, BlobType::File] {
        let meta = BlobMetadata {
            blob_type: bt,
            created_at: 0,
            local_only: false,
        };
        let bytes =
            rkyv::to_bytes::<rancor::Error>(&meta).expect("serialize failed");
        let _: BlobMetadata =
            rkyv::from_bytes::<BlobMetadata, rancor::Error>(&bytes)
                .expect("from_bytes failed");
    }
}
