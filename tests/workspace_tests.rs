use ark::backend::ArkBackend;
use ark::workspace::{
    ark_store_factories, ark_working_copy_factories, default_user_settings, init_workspace,
    load_workspace, BACKEND_NAME,
};
use jj_lib::object_id::ObjectId;
use jj_lib::repo::Repo;

fn temp_workspace_root() -> tempfile::TempDir {
    tempfile::tempdir().expect("failed to create temp dir")
}

#[test]
fn backend_name_matches_factory_key() {
    assert_eq!(BACKEND_NAME, "ark");
    assert_eq!(ArkBackend::BACKEND_NAME, BACKEND_NAME);
}

#[test]
fn default_settings_are_valid() {
    let settings = default_user_settings().expect("failed to create default settings");
    assert_eq!(settings.user_name(), "Ark");
    assert_eq!(settings.user_email(), "ark@localhost");
}

#[test]
fn store_factories_include_ark_backend() {
    let factories = ark_store_factories();
    let settings = default_user_settings().expect("failed to create settings");

    let dir = temp_workspace_root();
    let store_path = dir.path().join("store");
    fs_err::create_dir_all(&store_path).expect("failed to create store dir");

    fs_err::write(store_path.join("type"), BACKEND_NAME).expect("failed to write type file");

    let result = factories.load_backend(&settings, &store_path);
    assert!(result.is_ok(), "ark backend should load from factory");

    let backend = result.expect("just asserted Ok");
    assert_eq!(backend.name(), BACKEND_NAME);
}

#[tokio::test(flavor = "multi_thread")]
async fn init_creates_jj_directory_structure() {
    let dir = temp_workspace_root();
    let settings = default_user_settings().expect("failed to create settings");

    let (workspace, _repo) =
        init_workspace(dir.path(), &settings).expect("failed to init workspace");

    let jj_dir = dir.path().join(".jj");
    assert!(jj_dir.is_dir(), ".jj directory should exist");
    assert!(jj_dir.join("repo").is_dir(), ".jj/repo should exist");
    assert!(
        jj_dir.join("repo").join("store").is_dir(),
        ".jj/repo/store should exist"
    );

    let store_type =
        fs_err::read_to_string(jj_dir.join("repo").join("store").join("type"))
            .expect("failed to read store type");
    assert_eq!(store_type, BACKEND_NAME);

    assert!(
        jj_dir.join("working_copy").is_dir(),
        ".jj/working_copy should exist"
    );

    assert_eq!(workspace.repo_path(), jj_dir.join("repo"));
}

// @todo(o11y): load_round_trips_after_init requires shutting down the iroh
//   router to release the redb lock before reopening. ArkBackend doesn't
//   expose a shutdown path through jj's Backend trait (Drop is sync, shutdown
//   is async). tested via store_type file check and init_creates_jj_directory_structure
//   instead. will revisit once ArkBackend gets proper lifecycle management.
#[tokio::test(flavor = "multi_thread")]
async fn load_round_trips_after_init() {
    let dir = temp_workspace_root();
    let settings = default_user_settings().expect("failed to create settings");

    let (_workspace, repo) =
        init_workspace(dir.path(), &settings).expect("failed to init workspace");

    let store = repo.store().backend().downcast_ref::<ArkBackend>()
        .expect("backend should be ArkBackend");
    store.store().shutdown().await.expect("failed to shutdown store");

    drop(repo);
    drop(_workspace);

    let loaded = load_workspace(dir.path(), &settings).expect("failed to load workspace");

    let store_type = fs_err::read_to_string(
        loaded.repo_path().join("store").join("type"),
    )
    .expect("failed to read store type");
    assert_eq!(store_type, BACKEND_NAME);
}

#[tokio::test(flavor = "multi_thread")]
async fn repo_has_root_commit() {
    let dir = temp_workspace_root();
    let settings = default_user_settings().expect("failed to create settings");

    let (_workspace, repo) =
        init_workspace(dir.path(), &settings).expect("failed to init workspace");

    let root_commit_id = repo.store().root_commit_id();
    assert_eq!(root_commit_id.as_bytes().len(), 32);
}

#[tokio::test(flavor = "multi_thread")]
async fn repo_backend_is_ark() {
    let dir = temp_workspace_root();
    let settings = default_user_settings().expect("failed to create settings");

    let (_workspace, repo) =
        init_workspace(dir.path(), &settings).expect("failed to init workspace");

    assert_eq!(repo.store().backend().name(), BACKEND_NAME);
}

#[tokio::test(flavor = "multi_thread")]
async fn working_copy_factories_are_valid() {
    let factories = ark_working_copy_factories();
    assert!(
        !factories.is_empty(),
        "should have at least local working copy factory"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn init_and_load_share_same_root_commit() {
    let dir = temp_workspace_root();
    let settings = default_user_settings().expect("failed to create settings");

    let (_workspace, repo) =
        init_workspace(dir.path(), &settings).expect("failed to init workspace");
    let init_root_id = repo.store().root_commit_id().clone();

    let ark = repo.store().backend().downcast_ref::<ArkBackend>()
        .expect("backend should be ArkBackend");
    ark.store().shutdown().await.expect("failed to shutdown store");

    drop(repo);
    drop(_workspace);

    let loaded = load_workspace(dir.path(), &settings).expect("failed to load workspace");
    let loaded_repo = loaded
        .repo_loader()
        .load_at_head()
        .expect("failed to load repo at head");

    let load_root_id = loaded_repo.store().root_commit_id().clone();
    assert_eq!(init_root_id, load_root_id);
}
