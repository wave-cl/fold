//! The offline commands, driven as a user would: `schema check`, `schema
//! fmt` and `init`.

use assert_cmd::Command;
use predicates::prelude::*;

/// A two-file schema: `s.fold` (the derivation root) and `domain.fold`.
const DOMAIN: &str = r#"layer domain

/// Orders, in short.
context C {
  /// money, roughly
  value Money { amount: decimal, currency: string }

  event E v1 { k: uuid, m: Money }

  aggregate A {
    key k: uuid
    stream "a-{k}"
    events E
  }
}
"#;

const DERIVE: &str = r#"layer derivation

import "domain.fold"

state C.A { m: Money? }
  evolve wasm "a.wasm"

projection C.P {
  from E
  fold wasm "a.wasm"
  table t { key k: uuid, n: int }
}
"#;

/// The bundle `fold init` stores for the two files.
fn bundle_of(derive: &str, domain: &str) -> String {
    format!("// ---- file: s.fold\n{derive}// ---- file: domain.fold\n{domain}")
}

/// Writes the two files under `dir`, `domain` possibly edited; the root
/// `s.fold`.
fn write_schema(dir: &std::path::Path, domain: &str) -> std::path::PathBuf {
    std::fs::write(dir.join("domain.fold"), domain).unwrap();
    let root = dir.join("s.fold");
    std::fs::write(&root, DERIVE).unwrap();
    root
}

fn fold() -> Command {
    Command::cargo_bin("fold").unwrap()
}

#[test]
fn schema_check_ok_and_json() {
    let dir = tempfile::tempdir().unwrap();
    let file = write_schema(dir.path(), DOMAIN);
    fold()
        .args(["schema", "check"])
        .arg(&file)
        .assert()
        .success()
        .stdout(predicate::str::contains("ok:"))
        .stdout(predicate::str::contains("layer derivation"))
        .stdout(predicate::str::contains("context C  -- Orders, in short."))
        .stdout(predicate::str::contains("aggregate  A"))
        .stdout(predicate::str::contains("state       C.A  1 field(s)"))
        .stdout(predicate::str::contains(
            "projection  C.P  from C.E  tables t",
        ));
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
    assert_eq!(v["layer"], "derivation");
    assert_eq!(v["contexts"][0]["name"], "C");
    assert_eq!(v["contexts"][0]["docs"], "Orders, in short.");
    assert_eq!(v["states"], serde_json::json!(["C.A"]));
    assert_eq!(v["projections"], serde_json::json!(["C.P"]));
    // A lower-layer root checks on its own and says its layer.
    fold()
        .args(["schema", "check"])
        .arg(dir.path().join("domain.fold"))
        .assert()
        .success()
        .stdout(predicate::str::contains("layer domain"))
        .stdout(predicate::str::contains("aggregate  A"))
        .stdout(predicate::str::contains("state ").not());
}

#[test]
fn schema_check_reports_diagnostics_and_exits_1() {
    let dir = tempfile::tempdir().unwrap();
    let file = write_schema(dir.path(), &DOMAIN.replace("m: Money }", "m: Nope }"));
    fold()
        .args(["schema", "check"])
        .arg(&file)
        .assert()
        .code(1)
        .stderr(predicate::str::contains("domain.fold:8:28: S011"));
    // A declaration outside its layer is S058 in the file that holds it.
    let file = write_schema(
        dir.path(),
        &DOMAIN.replace(
            "context C {",
            "state C.A {} evolve wasm \"a.wasm\"\ncontext C {",
        ),
    );
    fold()
        .args(["schema", "check"])
        .arg(&file)
        .assert()
        .code(1)
        .stderr(predicate::str::contains("domain.fold:4:1: S058"));
}

#[test]
fn schema_fmt_check_reports_and_write_reformats() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("s.fold");
    let messy = "layer domain\ncontext C {   value V {a:int, // note\n b: string}\n enum E{A,B}}\n";
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
        written.starts_with("layer domain\n\ncontext C {\n  value V {\n"),
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
    std::fs::write(&file, "layer domain\ncontext C { value V { a: } }\n").unwrap();
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
    let file = write_schema(dir.path(), DOMAIN);
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
    assert_eq!(
        stored,
        bundle_of(DERIVE, DOMAIN),
        "the schema is stored as its bundle"
    );
    // `init` runs the derivation node too: a domain root is refused (S061).
    fold()
        .args(["init"])
        .arg(dir.path().join("db2"))
        .arg("--schema")
        .arg(dir.path().join("domain.fold"))
        .assert()
        .failure()
        .stderr(predicate::str::contains("S061"));
}

/// A domain root importing a shared file from a subdirectory.
const ROOT_WITH_IMPORT: &str = "layer domain\nimport \"shared/money.fold\"\n\ncontext C {\n  event E v1 { k: uuid, m: Shared.Money }\n  aggregate A {\n    key k: uuid\n    stream \"a-{k}\"\n    events E\n  }\n}\n";
const SHARED_MONEY: &str =
    "layer domain\ncontext Shared {\n  value Money { amount: decimal, currency: string }\n}\n";

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
        .stderr(predicate::str::contains("shared/money.fold:3:44: S011"));
    // A missing import is S046 at the import.
    std::fs::remove_file(dir.path().join("shared/money.fold")).unwrap();
    fold()
        .args(["schema", "check"])
        .arg(&root)
        .assert()
        .code(1)
        .stderr(predicate::str::contains("2:8: S046"));
}

#[test]
fn init_stores_the_bundle_for_a_multi_file_schema() {
    // The domain itself imports a file: three files in the bundle, in load
    // order (root, its imports depth-first).
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("shared")).unwrap();
    let root = write_schema(dir.path(), ROOT_WITH_IMPORT);
    // Money lives in Shared here.
    let derive = DERIVE.replace("m: Money?", "m: Shared.Money?");
    std::fs::write(&root, &derive).unwrap();
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
            "{}// ---- file: shared/money.fold\n{SHARED_MONEY}",
            bundle_of(&derive, ROOT_WITH_IMPORT)
        )
    );
    // The bundle compiles on its own to the same schema.
    let from_bundle = fold_schema::Sources::from_bundle(&stored)
        .compile_derivation()
        .unwrap();
    let from_disk = fold_schema::Sources::load(&root)
        .unwrap()
        .compile_derivation()
        .unwrap();
    assert_eq!(from_bundle.contexts, from_disk.contexts);
    assert_eq!(from_bundle.states, from_disk.states);
}

#[test]
fn schema_diff_classifies_changes_and_exits_1_on_breaking() {
    // Both sides as bundles (the form `schema show` prints).
    let dir = tempfile::tempdir().unwrap();
    let old = dir.path().join("old.fold");
    let new = dir.path().join("new.fold");
    std::fs::write(&old, bundle_of(DERIVE, DOMAIN)).unwrap();
    std::fs::write(
        &new,
        bundle_of(
            &DERIVE.replace(
                "table t { key k: uuid, n: int }",
                "table t { key k: uuid, n: int, m: int? }",
            ),
            DOMAIN,
        ),
    )
    .unwrap();
    fold()
        .args(["schema", "diff"])
        .arg(&old)
        .arg(&new)
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "[compatible] C.P.t.m: column `m` added (optional)",
        ))
        .stdout(predicate::str::contains(
            "1 change(s): 0 breaking, 0 rebuild, 1 compatible",
        ));
    std::fs::write(
        &new,
        bundle_of(
            DERIVE,
            &DOMAIN.replace(
                "value Money { amount: decimal, currency: string }",
                "value Money { amount: decimal }",
            ),
        ),
    )
    .unwrap();
    let out = fold()
        .args(["--json", "schema", "diff"])
        .arg(&old)
        .arg(&new)
        .assert()
        .code(1)
        .get_output()
        .stdout
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["breaking"], true);
    assert_eq!(v["changes"][0]["kind"], "field_removed");
    assert_eq!(v["changes"][0]["path"], "C.Money.currency");
    // Either side may be a root file on disk; a lower layer diffs on its
    // own, and the layers must match.
    let root = write_schema(dir.path(), DOMAIN);
    std::fs::write(
        &old,
        bundle_of(
            DERIVE,
            &DOMAIN.replace("context C {", "context X {}\ncontext C {"),
        ),
    )
    .unwrap();
    fold()
        .args(["schema", "diff"])
        .arg(&old)
        .arg(&root)
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "[compatible] X: context `X` removed",
        ));
    std::fs::write(
        &old,
        DOMAIN.replace("context C {", "context X {}\ncontext C {"),
    )
    .unwrap();
    fold()
        .args(["schema", "diff"])
        .arg(&old)
        .arg(dir.path().join("domain.fold"))
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "[compatible] X: context `X` removed",
        ));
    fold()
        .args(["schema", "diff"])
        .arg(&old)
        .arg(&root)
        .assert()
        .failure()
        .stderr(predicate::str::contains("different layers"));
}
