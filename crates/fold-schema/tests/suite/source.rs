use std::collections::HashMap;
use std::path::Path;

use fold_schema::{MapLoader, Sources, compile};

fn loader(files: &[(&str, &str)]) -> MapLoader {
    MapLoader(
        files
            .iter()
            .map(|(p, t)| (p.to_string(), t.to_string()))
            .collect::<HashMap<_, _>>(),
    )
}

const ROOT: &str = r#"//! The root.
import "shared.fold"
import "sub/b.fold"

context Orders {
  event Placed v1 { id: uuid, total: Shared.Money }
  aggregate Order {
    key id: uuid
    stream "order-{id}"
    events Placed
    state { total: Shared.Money }
    evolve wasm "orders.wasm"
    commands Place { total: Shared.Money } -> wasm "orders.wasm"
  }
}
"#;

const SHARED: &str = r#"context Shared {
  value Money { amount: decimal, currency: string }
}
"#;

const SUB_B: &str = r#"import "c.fold"
import "../shared.fold"
context B {
  event Hit v1 { k: uuid }
  projection Hits {
    from Hit
    fold wasm "b.wasm"
    table hits { key k: uuid }
  }
}
"#;

const SUB_C: &str = r#"context C {
  event Ping v1 { k: uuid }
  process Pinger {
    key k: uuid
    from Ping by k
    state { n: int }
    react wasm "deep/c.wasm" export "react"
  }
}
"#;

fn load(files: &[(&str, &str)]) -> Sources {
    Sources::load_with(Path::new("schema.fold"), &loader(files)).unwrap_or_else(|e| panic!("{e}"))
}

fn tree() -> Vec<(&'static str, &'static str)> {
    vec![
        ("schema.fold", ROOT),
        ("shared.fold", SHARED),
        (
            "sub/b.fold",
            SUB_B.replace("import \"../shared.fold\"\n", "").leak(),
        ),
        ("sub/c.fold", SUB_C),
    ]
}

#[test]
fn imports_merge_contexts_root_first_then_depth_first() {
    let s = load(&tree());
    assert_eq!(
        s.files()
            .iter()
            .map(|f| f.path.as_str())
            .collect::<Vec<_>>(),
        ["schema.fold", "shared.fold", "sub/b.fold", "sub/c.fold"]
    );
    let schema = s.compile().unwrap_or_else(|d| panic!("{d}"));
    assert_eq!(
        schema.contexts.keys().collect::<Vec<_>>(),
        ["Orders", "Shared", "B", "C"]
    );
    assert_eq!(schema.docs, ["The root."]);
    assert!(schema.dir().is_some(), "loaded from a path");
}

#[test]
fn imports_load_once_and_tolerate_cycles_and_diamonds() {
    // Root → shared, sub/b; sub/b → sub/c, shared (diamond); sub/c → b (cycle).
    let mut files = tree();
    files[2] = ("sub/b.fold", SUB_B);
    files[3] = (
        "sub/c.fold",
        SUB_C
            .replace("context C {", "import \"b.fold\"\ncontext C {")
            .leak(),
    );
    // `..` is not allowed in an import path; use a diamond without it.
    files[2] = (
        "sub/b.fold",
        SUB_B.replace("../shared.fold", "c.fold").leak(),
    );
    let s = load(&files);
    assert_eq!(
        s.files()
            .iter()
            .map(|f| f.path.as_str())
            .collect::<Vec<_>>(),
        ["schema.fold", "shared.fold", "sub/b.fold", "sub/c.fold"]
    );
    s.compile().unwrap_or_else(|d| panic!("{d}"));
}

#[test]
fn s010_duplicate_context_across_files_names_the_file() {
    let mut files = tree();
    files[1] = (
        "shared.fold",
        SHARED
            .replace("context Shared {", "context Orders {}\ncontext Shared {")
            .leak(),
    );
    let d = load(&files).compile().unwrap_err();
    assert_eq!(d.codes(), ["S010"], "{d}");
    assert_eq!(d.file_of(&d[0]), Some("shared.fold"));
    assert_eq!(d.line_col_of(&d[0]), (1, 9));
    let text = d.to_string();
    assert!(text.starts_with("shared.fold:1:9: S010:"), "{text}");
    assert!(text.contains("| context Orders {}"), "{text}");
}

#[test]
fn s046_unreadable_import() {
    let mut files = tree();
    files.remove(3);
    let d = load(&files).compile().unwrap_err();
    assert_eq!(d.codes(), ["S046"], "{d}");
    assert_eq!(d.file_of(&d[0]), Some("sub/b.fold"));
    assert_eq!(d.line_col_of(&d[0]), (1, 8));
    assert!(
        d.to_string().contains("cannot read import \"sub/c.fold\""),
        "{d}"
    );
}

#[test]
fn s047_import_path_rules() {
    for bad in ["", "/abs.fold", "c:x.fold", "../up.fold", "sub/../x.fold"] {
        let root = ROOT.replace("import \"shared.fold\"", &format!("import {bad:?}"));
        let mut files = tree();
        files[0] = ("schema.fold", root.leak());
        let d = load(&files).compile().unwrap_err();
        assert_eq!(d.codes(), ["S047"], "{bad}: {d}");
        assert_eq!(d.file_of(&d[0]), Some("schema.fold"), "{bad}");
    }
}

#[test]
fn imported_wasm_paths_are_rebased_onto_the_root_directory() {
    let schema = load(&tree()).compile().unwrap_or_else(|d| panic!("{d}"));
    assert_eq!(
        schema.contexts["Orders"].aggregates["Order"].evolve.module,
        "orders.wasm"
    );
    assert_eq!(
        schema.contexts["B"].projections["Hits"].fold.module,
        "sub/b.wasm"
    );
    let react = &schema.contexts["C"].processes["Pinger"].react;
    assert_eq!(react.module, "sub/deep/c.wasm");
    assert_eq!(react.export.as_deref(), Some("react"));
    // A bad path in an imported file is reported as written.
    let mut files = tree();
    files[3] = (
        "sub/c.fold",
        SUB_C.replace("\"deep/c.wasm\"", "\"../c.wasm\"").leak(),
    );
    let d = load(&files).compile().unwrap_err();
    assert_eq!(d.codes(), ["S024"], "{d}");
    assert_eq!(d.file_of(&d[0]), Some("sub/c.fold"));
    assert!(d.to_string().contains("\"../c.wasm\""), "{d}");
}

#[test]
fn compile_from_text_refuses_imports() {
    let d = compile(ROOT).unwrap_err();
    assert_eq!(d.codes(), ["S046", "S046"], "{d}");
    assert_eq!(d.file_of(&d[0]), None);
    assert_eq!(d[0].span.line_col(ROOT), (2, 1));
    // A text without imports compiles as before, with no sections.
    compile(SHARED).unwrap_or_else(|d| panic!("{d}"));
}

#[test]
fn diagnostics_in_imported_files_name_the_file() {
    let mut files = tree();
    files[3] = (
        "sub/c.fold",
        SUB_C
            .replace("state { n: int }", "state { n: Nope }")
            .leak(),
    );
    let d = load(&files).compile().unwrap_err();
    assert_eq!(d.file_of(&d[0]), Some("sub/c.fold"), "{d}");
    assert_eq!(d.line_col_of(&d[0]), (6, 16), "{d}");
    assert!(d.to_string().starts_with("sub/c.fold:6:16:"), "{d}");
    // A syntax error in an import is P001 in that file.
    let mut files = tree();
    files[1] = ("shared.fold", "context Shared {\n");
    let d = load(&files).compile().unwrap_err();
    assert_eq!(d.codes(), ["P001"], "{d}");
    assert_eq!(d.file_of(&d[0]), Some("shared.fold"));
}

#[test]
fn bundle_is_verbatim_for_one_file_and_sectioned_for_many() {
    let one = Sources::load_with(
        Path::new("schema.fold"),
        &loader(&[("schema.fold", SHARED)]),
    )
    .unwrap();
    assert_eq!(one.bundle(), SHARED);
    let many = load(&tree()).bundle();
    let no_trailing_newline = "context Z {}";
    let mut files = tree();
    files.push(("z.fold", no_trailing_newline));
    files[0] = (
        "schema.fold",
        ROOT.replace(
            "import \"sub/b.fold\"",
            "import \"sub/b.fold\"\nimport \"z.fold\"",
        )
        .leak(),
    );
    let with_z = load(&files).bundle();
    assert!(
        many.starts_with("// ---- file: schema.fold\n//! The root.\n"),
        "{many}"
    );
    assert!(
        many.contains("\n// ---- file: shared.fold\ncontext Shared {\n"),
        "{many}"
    );
    assert!(many.contains("\n// ---- file: sub/b.fold\n"), "{many}");
    assert!(many.contains("\n// ---- file: sub/c.fold\n"), "{many}");
    assert!(
        with_z.ends_with("// ---- file: z.fold\ncontext Z {}\n"),
        "{with_z}"
    );
}

#[test]
fn from_bundle_compiles_to_the_same_model_as_load() {
    let loaded = load(&tree());
    let from_disk = loaded.compile().unwrap_or_else(|d| panic!("{d}"));
    let bundled = Sources::from_bundle(&loaded.bundle());
    assert_eq!(
        bundled
            .files()
            .iter()
            .map(|f| f.path.as_str())
            .collect::<Vec<_>>(),
        ["schema.fold", "shared.fold", "sub/b.fold", "sub/c.fold"]
    );
    let from_bundle = bundled.compile().unwrap_or_else(|d| panic!("{d}"));
    assert_eq!(from_disk.contexts, from_bundle.contexts);
    assert_eq!(from_disk.docs, from_bundle.docs);
    assert!(from_bundle.dir().is_none());
    assert_eq!(bundled.bundle(), loaded.bundle(), "a bundle round-trips");
    // A plain text is one file.
    let single = Sources::from_bundle(SHARED);
    assert_eq!(single.files().len(), 1);
    assert_eq!(single.bundle(), SHARED);
}
