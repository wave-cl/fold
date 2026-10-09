//! The offline commands, driven as a user would: `schema check`, `schema
//! fmt` and `init`.

use assert_cmd::Command;
use predicates::prelude::*;

const SCHEMA: &str = r#"/// Orders, in short.
context C {
  /// money, roughly
  value Money { amount: decimal, currency: string }

  event E v1 { k: uuid, m: Money }

  aggregate A {
    key k: uuid
    stream "a-{k}"
    events E
    state { m: Money? }
    evolve wasm "a.wasm"
    commands Do { m: Money } -> wasm "a.wasm"
  }

  projection P {
    from E
    fold wasm "a.wasm"
    table t { key k: uuid, n: int }
  }
}
"#;

fn fold() -> Command {
    Command::cargo_bin("fold").unwrap()
}

#[test]
fn schema_check_ok_and_json() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("s.fold");
    std::fs::write(&file, SCHEMA).unwrap();
    fold()
        .args(["schema", "check"])
        .arg(&file)
        .assert()
        .success()
        .stdout(predicate::str::contains("ok:"))
        .stdout(predicate::str::contains("context C  -- Orders, in short."))
        .stdout(predicate::str::contains("aggregate  A"));
    let out = fold()
        .args(["--json", "schema", "check"])
        .arg(&file)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["contexts"][0]["name"], "C");
    assert_eq!(v["contexts"][0]["docs"], "Orders, in short.");
}

#[test]
fn schema_check_reports_diagnostics_and_exits_1() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("s.fold");
    std::fs::write(&file, SCHEMA.replace("m: Money }", "m: Nope }")).unwrap();
    fold()
        .args(["schema", "check"])
        .arg(&file)
        .assert()
        .code(1)
        .stderr(predicate::str::contains("S011"));
}

#[test]
fn schema_fmt_check_reports_and_write_reformats() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("s.fold");
    let messy = "context C {   value V {a:int, // note\n b: string}\n enum E{A,B}}\n";
    std::fs::write(&file, messy).unwrap();
    fold()
        .args(["schema", "fmt", "--check"])
        .arg(&file)
        .assert()
        .code(1)
        .stdout(predicate::str::contains("would reformat"));
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        messy,
        "--check writes nothing"
    );
    fold()
        .args(["schema", "fmt"])
        .arg(&file)
        .assert()
        .success()
        .stdout(predicate::str::contains("formatted "));
    let written = std::fs::read_to_string(&file).unwrap();
    assert_eq!(written, fold_schema::fmt::format_source(messy).unwrap());
    assert!(written.contains("// note"), "{written}");
    assert!(
        written.starts_with("context C {\n  value V {\n"),
        "{written}"
    );
    fold()
        .args(["schema", "fmt", "--check"])
        .arg(&file)
        .assert()
        .success();
    fold()
        .args(["schema", "fmt"])
        .arg(&file)
        .assert()
        .success()
        .stdout(predicate::str::contains("unchanged "));
}

#[test]
fn schema_fmt_syntax_error_exits_1() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("s.fold");
    std::fs::write(&file, "context C { value V { a: } }\n").unwrap();
    fold()
        .args(["schema", "fmt"])
        .arg(&file)
        .assert()
        .code(1)
        .stderr(predicate::str::contains("P001"));
    assert!(
        std::fs::read_to_string(&file).unwrap().contains("a: }"),
        "nothing written"
    );
}

#[test]
fn init_creates_log_config_and_stores_schema() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("s.fold");
    std::fs::write(&file, SCHEMA).unwrap();
    let target = dir.path().join("db");
    fold()
        .args(["init"])
        .arg(&target)
        .arg("--schema")
        .arg(&file)
        .assert()
        .success()
        .stdout(predicate::str::contains("created log in"));
    let config = std::fs::read_to_string(target.join("foldd.toml")).unwrap();
    assert!(config.contains("listen = \"127.0.0.1:4141\""), "{config}");
    let stored = std::fs::read_to_string(target.join("data/default/schema/current.fold")).unwrap();
    assert_eq!(stored, SCHEMA, "the schema is stored verbatim");
}

const ROOT_WITH_IMPORT: &str = "import \"shared/money.fold\"\n\ncontext C {\n  event E v1 { k: uuid, m: Shared.Money }\n  projection P {\n    from E\n    fold wasm \"p.wasm\"\n    table t { key k: uuid, n: int }\n  }\n}\n";
const SHARED_MONEY: &str =
    "context Shared {\n  value Money { amount: decimal, currency: string }\n}\n";

#[test]
fn schema_check_follows_imports_and_names_the_file_in_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("shared")).unwrap();
    let root = dir.path().join("s.fold");
    std::fs::write(&root, ROOT_WITH_IMPORT).unwrap();
    std::fs::write(dir.path().join("shared/money.fold"), SHARED_MONEY).unwrap();
    fold()
        .args(["schema", "check"])
        .arg(&root)
        .assert()
        .success()
        .stdout(predicate::str::contains("files: s.fold, shared/money.fold"))
        .stdout(predicate::str::contains("context Shared"));
    let out = fold()
        .args(["--json", "schema", "check"])
        .arg(&root)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(
        v["files"],
        serde_json::json!(["s.fold", "shared/money.fold"])
    );
    // A diagnostic in the imported file names it.
    std::fs::write(
        dir.path().join("shared/money.fold"),
        SHARED_MONEY.replace("currency: string", "currency: Nope"),
    )
    .unwrap();
    fold()
        .args(["schema", "check"])
        .arg(&root)
        .assert()
        .code(1)
        .stderr(predicate::str::contains("shared/money.fold:2:44: S011"));
    // A missing import is S046 at the import.
    std::fs::remove_file(dir.path().join("shared/money.fold")).unwrap();
    fold()
        .args(["schema", "check"])
        .arg(&root)
        .assert()
        .code(1)
        .stderr(predicate::str::contains("1:8: S046"));
}

#[test]
fn init_stores_the_bundle_for_a_multi_file_schema() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("shared")).unwrap();
    let root = dir.path().join("s.fold");
    std::fs::write(&root, ROOT_WITH_IMPORT).unwrap();
    std::fs::write(dir.path().join("shared/money.fold"), SHARED_MONEY).unwrap();
    let target = dir.path().join("db");
    fold()
        .args(["init"])
        .arg(&target)
        .arg("--schema")
        .arg(&root)
        .assert()
        .success();
    let stored = std::fs::read_to_string(target.join("data/default/schema/current.fold")).unwrap();
    assert_eq!(
        stored,
        format!(
            "// ---- file: s.fold\n{ROOT_WITH_IMPORT}// ---- file: shared/money.fold\n{SHARED_MONEY}"
        )
    );
    // The bundle compiles on its own to the same schema.
    let from_bundle = fold_schema::Sources::from_bundle(&stored)
        .compile()
        .unwrap();
    let from_disk = fold_schema::Sources::load(&root)
        .unwrap()
        .compile()
        .unwrap();
    assert_eq!(from_bundle.contexts, from_disk.contexts);
}
