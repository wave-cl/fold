use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use fold_schema::{AggRef, DerivationSchema, Layer, MapLoader, Sources, compile, compile_any};

use super::common::line_of;

fn loader(files: &[(&str, &str)]) -> MapLoader {
    MapLoader(
        files
            .iter()
            .map(|(p, t)| (p.to_string(), t.to_string()))
            .collect::<HashMap<_, _>>(),
    )
}

/// The root: a derivation file over `derive.fold`, plus a second
/// derivation file in a subdirectory.
const ROOT: &str = r#"//! The root.
layer derivation

import "derive.fold"
import "sub/c.fold"

projection Orders.Totals {
  from Placed
  fold wasm "orders.wasm"
  table totals { key id: uuid, total: Shared.Money }
}
"#;

const DERIVE: &str = r#"layer derivation
import "domain.fold"
import "sub/b.fold"

state Orders.Order { total: Shared.Money }
  evolve wasm "orders.wasm"
"#;

const DOMAIN: &str = r#"layer domain
import "shared.fold"

context Orders {
  event Placed v1 { id: uuid, total: Shared.Money }
  aggregate Order {
    key id: uuid
    stream "order-{id}"
    events Placed
  }
}
"#;

/// Shared types, plus the contexts the subdirectory files build on: a file
/// need not import what it names, only the root must reach it.
const SHARED: &str = r#"layer domain
context Shared {
  value Money { amount: decimal, currency: string }
}
context B {
  event Hit v1 { k: uuid }
}
context C {
  event Ping v1 { k: uuid }
}
"#;

const SUB_B: &str = r#"layer derivation
projection B.Hits {
  from Hit
  fold wasm "b.wasm"
  table hits { key k: uuid }
}
"#;

const SUB_C: &str = r#"layer derivation
projection C.Pings {
  from Ping
  fold wasm "deep/c.wasm" export "fold"
  table pings { key k: uuid, n: int }
}
"#;

fn load(files: &[(&str, &str)]) -> Sources {
    Sources::load_with(Path::new("schema.fold"), &loader(files)).unwrap_or_else(|e| panic!("{e}"))
}

fn derived(s: &Sources) -> Arc<DerivationSchema> {
    s.compile_derivation().unwrap_or_else(|d| panic!("{d}"))
}

fn tree() -> Vec<(&'static str, &'static str)> {
    vec![
        ("schema.fold", ROOT),
        ("derive.fold", DERIVE),
        ("domain.fold", DOMAIN),
        ("shared.fold", SHARED),
        ("sub/b.fold", SUB_B),
        ("sub/c.fold", SUB_C),
    ]
}

const LOAD_ORDER: [&str; 6] = [
    "schema.fold",
    "derive.fold",
    "domain.fold",
    "shared.fold",
    "sub/b.fold",
    "sub/c.fold",
];

fn paths(s: &Sources) -> Vec<&str> {
    s.files().iter().map(|f| f.path.as_str()).collect()
}

#[test]
fn imports_merge_declarations_root_first_then_depth_first() {
    let s = load(&tree());
    assert_eq!(paths(&s), LOAD_ORDER);
    assert_eq!(
        s.files().iter().map(|f| f.layer).collect::<Vec<_>>(),
        [
            Some(Layer::Derivation),
            Some(Layer::Derivation),
            Some(Layer::Domain),
            Some(Layer::Domain),
            Some(Layer::Derivation),
            Some(Layer::Derivation),
        ]
    );
    assert_eq!(s.layer(), Some(Layer::Derivation));
    let schema = derived(&s);
    assert_eq!(
        schema.contexts.keys().collect::<Vec<_>>(),
        ["Orders", "Shared", "B", "C"]
    );
    assert_eq!(schema.docs, ["The root."]);
    assert!(schema.dir().is_some(), "loaded from a path");
    assert!(schema.state(&AggRef::new("Orders", "Order")).is_some());
    assert!(schema.projection("B", "Hits").is_some());
    assert!(schema.projection("C", "Pings").is_some());
    assert_eq!(
        schema
            .projection("Orders", "Totals")
            .map(|p| p.tables.len()),
        Some(1)
    );
}

#[test]
fn imports_load_once_and_tolerate_cycles_and_diamonds() {
    // derive → sub/b; sub/c → sub/b (diamond); sub/b ↔ sub/b2 (cycle).
    let mut files = tree();
    files[4] = (
        "sub/b.fold",
        SUB_B
            .replace(
                "layer derivation\n",
                "layer derivation\nimport \"b2.fold\"\n",
            )
            .leak(),
    );
    files.push(("sub/b2.fold", "layer derivation\nimport \"b.fold\"\n"));
    files[5] = (
        "sub/c.fold",
        SUB_C
            .replace(
                "layer derivation\n",
                "layer derivation\nimport \"b.fold\"\n",
            )
            .leak(),
    );
    let s = load(&files);
    assert_eq!(
        paths(&s),
        [
            "schema.fold",
            "derive.fold",
            "domain.fold",
            "shared.fold",
            "sub/b.fold",
            "sub/b2.fold",
            "sub/c.fold"
        ]
    );
    derived(&s);
}

#[test]
fn s010_duplicate_context_across_files_names_the_file() {
    let mut files = tree();
    files[3] = (
        "shared.fold",
        SHARED
            .replace("context Shared {", "context Orders {}\ncontext Shared {")
            .leak(),
    );
    let d = load(&files).compile().unwrap_err();
    assert_eq!(d.codes(), ["S010"], "{d}");
    assert_eq!(d.file_of(&d[0]), Some("shared.fold"));
    assert_eq!(d.line_col_of(&d[0]), (2, 9));
    let text = d.to_string();
    assert!(text.starts_with("shared.fold:2:9: S010:"), "{text}");
    assert!(text.contains("| context Orders {}"), "{text}");
}

#[test]
fn s046_unreadable_import() {
    let mut files = tree();
    files.remove(4);
    let d = load(&files).compile().unwrap_err();
    assert_eq!(d.codes(), ["S046"], "{d}");
    assert_eq!(d.file_of(&d[0]), Some("derive.fold"));
    assert_eq!(
        d.line_col_of(&d[0]),
        (line_of(DERIVE, "import \"sub/b.fold\""), 8)
    );
    assert!(
        d.to_string().contains("cannot read import \"sub/b.fold\""),
        "{d}"
    );
}

#[test]
fn s047_import_path_rules() {
    for bad in ["", "/abs.fold", "c:x.fold", "../up.fold", "sub/../x.fold"] {
        let root = ROOT.replace("import \"derive.fold\"", &format!("import {bad:?}"));
        let mut files = tree();
        files[0] = ("schema.fold", root.leak());
        let d = load(&files).compile().unwrap_err();
        assert_eq!(d.codes(), ["S047"], "{bad}: {d}");
        assert_eq!(d.file_of(&d[0]), Some("schema.fold"), "{bad}");
    }
}

#[test]
fn s058_a_declaration_outside_its_layer_names_both_layers() {
    // A context in a derivation file.
    let mut files = tree();
    files[1] = (
        "derive.fold",
        DERIVE
            .replace("state Orders.Order", "context Extra {}\nstate Orders.Order")
            .leak(),
    );
    let d = load(&files).compile().unwrap_err();
    assert_eq!(d.codes(), ["S058"], "{d}");
    assert_eq!(d.file_of(&d[0]), Some("derive.fold"));
    assert!(
        d.to_string()
            .contains("`context` belongs to the domain layer; this is a `layer derivation` file"),
        "{d}"
    );
    // A state in a domain file, a projection in another domain file.
    let mut files = tree();
    files[2] = (
        "domain.fold",
        DOMAIN
            .replace(
                "context Orders {",
                "state Orders.Order {} evolve wasm \"w\"\ncontext Orders {",
            )
            .leak(),
    );
    files[3] = (
        "shared.fold",
        SHARED
            .replace(
                "context Shared {",
                "projection B.P { from Hit fold wasm \"w\" table t { key k: uuid } }\ncontext Shared {",
            )
            .leak(),
    );
    let d = load(&files).compile().unwrap_err();
    assert_eq!(d.codes(), ["S058", "S058"], "{d}");
    let text = d.to_string();
    assert!(
        text.contains("`state` belongs to the derivation layer; this is a `layer domain` file"),
        "{text}"
    );
    assert!(
        text.contains(
            "`projection` belongs to the derivation layer; this is a `layer domain` file"
        ),
        "{text}"
    );
    // Control: the tree as given has every declaration in its layer.
    derived(&load(&tree()));
}

#[test]
fn s060_a_file_imports_its_own_layer_or_a_lower_one() {
    // domain → derivation
    let mut files = tree();
    files[2] = (
        "domain.fold",
        DOMAIN
            .replace(
                "import \"shared.fold\"",
                "import \"shared.fold\"\nimport \"sub/c.fold\"",
            )
            .leak(),
    );
    let d = load(&files).compile().unwrap_err();
    assert_eq!(d.codes(), ["S060"], "{d}");
    assert_eq!(d.file_of(&d[0]), Some("domain.fold"));
    assert!(
        d.to_string().contains(
            "a `layer domain` file cannot import \"sub/c.fold\", a `layer derivation` file"
        ),
        "{d}"
    );
    // domain → derivation, where the import was already loaded by another file
    let mut files = tree();
    files[3] = (
        "shared.fold",
        SHARED
            .replace("layer domain\n", "layer domain\nimport \"sub/b.fold\"\n")
            .leak(),
    );
    let d = load(&files).compile().unwrap_err();
    assert_eq!(d.codes(), ["S060"], "{d}");
    assert_eq!(d.file_of(&d[0]), Some("shared.fold"));
    // Control: same-layer and downward imports are fine (the tree has both).
    derived(&load(&tree()));
}

#[test]
fn s061_each_compile_entry_needs_a_root_of_its_layer() {
    let tree = tree();
    let derive_root = Sources::load_with(Path::new("derive.fold"), &loader(&tree)).unwrap();
    assert_eq!(derive_root.layer(), Some(Layer::Derivation));
    let derivation = derive_root
        .compile_derivation()
        .unwrap_or_else(|d| panic!("{d}"));
    assert!(derivation.projection("B", "Hits").is_some());
    assert!(
        derivation.projection("C", "Pings").is_none(),
        "sub/c.fold is the root's import, not derive.fold's"
    );

    let domain_root = Sources::load_with(Path::new("domain.fold"), &loader(&tree)).unwrap();
    let d = domain_root.compile_derivation().unwrap_err();
    assert_eq!(d.codes(), ["S061"], "{d}");
    assert!(
        d.to_string().contains(
            "this is a `layer domain` file; a derivation schema needs a `layer derivation` root"
        ),
        "{d}"
    );
    let domain = domain_root
        .compile_domain()
        .unwrap_or_else(|d| panic!("{d}"));
    assert_eq!(
        domain.contexts.keys().collect::<Vec<_>>(),
        ["Orders", "Shared", "B", "C"]
    );

    // A higher root yields the same lower layers as their own files.
    let full = load(&tree);
    let via_root = full.compile_domain().unwrap();
    assert_eq!(via_root.contexts, domain.contexts);
    let via_root = full.compile_derivation().unwrap();
    assert_eq!(via_root.states, derivation.states);
    assert_eq!(
        via_root.projection("B", "Hits"),
        derivation.projection("B", "Hits")
    );
    assert_eq!(full.compile().unwrap().layer(), Layer::Derivation);
}

#[test]
fn imported_wasm_paths_are_rebased_onto_the_root_directory() {
    let schema = derived(&load(&tree()));
    assert_eq!(
        schema
            .state(&AggRef::new("Orders", "Order"))
            .unwrap()
            .evolve
            .module,
        "orders.wasm"
    );
    assert_eq!(
        schema.projection("B", "Hits").unwrap().fold.module,
        "sub/b.wasm"
    );
    let fold = &schema.projection("C", "Pings").unwrap().fold;
    assert_eq!(fold.module, "sub/deep/c.wasm");
    assert_eq!(fold.export.as_deref(), Some("fold"));
    // A bad path in an imported file is reported as written.
    let mut files = tree();
    files[5] = (
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
    assert_eq!(
        d[0].span.line_col(ROOT),
        (line_of(ROOT, "import \"derive.fold\""), 1)
    );
    // A text without imports compiles as before, with no sections.
    compile_any(SHARED).unwrap_or_else(|d| panic!("{d}"));
    // `compile` wants a derivation root.
    let d = compile(SHARED).unwrap_err();
    assert_eq!(d.codes(), ["S061"], "{d}");
    assert_eq!(d[0].span.line_col(SHARED), (1, 1));
}

#[test]
fn diagnostics_in_imported_files_name_the_file() {
    let mut files = tree();
    files[5] = ("sub/c.fold", SUB_C.replace("n: int }", "n: Nope }").leak());
    let d = load(&files).compile().unwrap_err();
    assert_eq!(d.codes(), ["S011"], "{d}");
    assert_eq!(d.file_of(&d[0]), Some("sub/c.fold"), "{d}");
    let line = line_of(SUB_C, "n: int }");
    let col = SUB_C
        .lines()
        .nth(line - 1)
        .and_then(|l| l.find("int"))
        .map(|at| at + 1)
        .unwrap();
    assert_eq!(d.line_col_of(&d[0]), (line, col), "{d}");
    assert!(
        d.to_string()
            .starts_with(&format!("sub/c.fold:{line}:{col}:")),
        "{d}"
    );
    // A syntax error in an import is P001 in that file.
    let mut files = tree();
    files[3] = ("shared.fold", "layer domain\ncontext Shared {\n");
    let d = load(&files).compile().unwrap_err();
    assert_eq!(d.codes(), ["P001"], "{d}");
    assert_eq!(d.file_of(&d[0]), Some("shared.fold"));
}

#[test]
fn bundle_is_verbatim_for_one_file_and_sectioned_for_many() {
    let one = Sources::load_with(
        Path::new("shared.fold"),
        &loader(&[("shared.fold", SHARED)]),
    )
    .unwrap();
    assert_eq!(one.bundle(), SHARED);
    let many = load(&tree()).bundle();
    let no_trailing_newline = "layer domain\ncontext Z {}";
    let mut files = tree();
    files.push(("z.fold", no_trailing_newline));
    files[0] = (
        "schema.fold",
        ROOT.replace(
            "import \"sub/c.fold\"",
            "import \"sub/c.fold\"\nimport \"z.fold\"",
        )
        .leak(),
    );
    let with_z = load(&files).bundle();
    assert!(
        many.starts_with("// ---- file: schema.fold\n//! The root.\nlayer derivation\n"),
        "{many}"
    );
    assert!(
        many.contains("\n// ---- file: shared.fold\nlayer domain\ncontext Shared {\n"),
        "{many}"
    );
    assert!(many.contains("\n// ---- file: sub/b.fold\n"), "{many}");
    assert!(many.contains("\n// ---- file: sub/c.fold\n"), "{many}");
    assert!(
        with_z.ends_with("// ---- file: z.fold\nlayer domain\ncontext Z {}\n"),
        "{with_z}"
    );
}

#[test]
fn from_bundle_compiles_to_the_same_model_as_load() {
    let loaded = load(&tree());
    let from_disk = derived(&loaded);
    let bundled = Sources::from_bundle(&loaded.bundle());
    assert_eq!(paths(&bundled), LOAD_ORDER);
    let from_bundle = derived(&bundled);
    assert_eq!(from_disk.contexts, from_bundle.contexts);
    assert_eq!(from_disk.states, from_bundle.states);
    assert_eq!(from_disk.projections, from_bundle.projections);
    assert_eq!(from_disk.docs, from_bundle.docs);
    assert!(from_bundle.dir().is_none());
    assert_eq!(bundled.bundle(), loaded.bundle(), "a bundle round-trips");
    // A plain text is one file.
    let single = Sources::from_bundle(SHARED);
    assert_eq!(single.files().len(), 1);
    assert_eq!(single.bundle(), SHARED);
    // `compile` takes a bundle too.
    let text = loaded.bundle();
    let compiled = compile(&text).unwrap_or_else(|d| panic!("{d}"));
    assert_eq!(compiled.contexts, from_disk.contexts);
}
