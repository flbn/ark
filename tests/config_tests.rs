use ark::config::Config;
use camino::Utf8Path;

fn write_toml(dir: &tempfile::TempDir, content: &str) -> camino::Utf8PathBuf {
    let path = Utf8Path::from_path(dir.path())
        .expect("non-utf8 temp path")
        .join("ark.toml");
    fs_err::write(&path, content).expect("write failed");
    path
}

#[test]
fn minimal_config() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = write_toml(
        &dir,
        r#"
[store]
path = "./data"
"#,
    );

    let config = Config::load(&path).expect("load failed");
    assert_eq!(config.store.path.as_str(), "./data");
    assert!(config.repos.is_empty());
    assert_eq!(config.sync.enumerate_threshold, 8);
}

#[test]
fn full_config() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = write_toml(
        &dir,
        r#"
[store]
path = "/var/ark"

[[repos]]
id = "my-project"
remotes = [
    { name = "peer-a", node_id = "abc123" },
    { name = "peer-b", node_id = "def456" },
]

[[repos]]
id = "other-project"

[sync]
enumerate_threshold = 16
"#,
    );

    let config = Config::load(&path).expect("load failed");
    assert_eq!(config.store.path.as_str(), "/var/ark");
    assert_eq!(config.repos.len(), 2);

    assert_eq!(config.repos[0].id, "my-project");
    assert_eq!(config.repos[0].remotes.len(), 2);
    assert_eq!(config.repos[0].remotes[0].name, "peer-a");
    assert_eq!(config.repos[0].remotes[0].node_id, "abc123");
    assert_eq!(config.repos[0].remotes[1].name, "peer-b");

    assert_eq!(config.repos[1].id, "other-project");
    assert!(config.repos[1].remotes.is_empty());

    assert_eq!(config.sync.enumerate_threshold, 16);
}

#[test]
fn missing_file_errors() {
    let result = Config::load(Utf8Path::new("/nonexistent/ark.toml"));
    assert!(result.is_err());
}

#[test]
fn invalid_toml_errors() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = write_toml(&dir, "not valid { toml [[[");

    let result = Config::load(&path);
    assert!(result.is_err());
}

#[test]
fn missing_store_section_errors() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = write_toml(
        &dir,
        r#"
[[repos]]
id = "orphan"
"#,
    );

    let result = Config::load(&path);
    assert!(result.is_err());
}

#[test]
fn sync_defaults_applied() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = write_toml(
        &dir,
        r#"
[store]
path = "."
"#,
    );

    let config = Config::load(&path).expect("load failed");
    assert_eq!(config.sync.enumerate_threshold, 8);
}

#[test]
fn repos_default_empty() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = write_toml(
        &dir,
        r#"
[store]
path = "."

[sync]
enumerate_threshold = 4
"#,
    );

    let config = Config::load(&path).expect("load failed");
    assert!(config.repos.is_empty());
    assert_eq!(config.sync.enumerate_threshold, 4);
}
