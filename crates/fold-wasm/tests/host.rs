//! Host tests written against hand-made WebAssembly text modules, so no
//! wasm32 toolchain is needed to prove the ABI plumbing and the limits.

use std::sync::Arc;

use fold_wasm::{
    CheckInput, CheckReply, CommandInput, CommandReply, Engine, Event, EvolveInput, Guest, InvCtx,
    Limits, ModuleCache, ProjectionInput, RowReader, WasmError,
};
use serde_json::json;

const MUTATIONS_OK: &str = r#"{"mutations":[]}"#;
const STATE_OK: &str = r#"{"state":{"n":1}}"#;
const REJECTED: &str = r#"{"rejected":{"code":"NOPE","message":"no"}}"#;
const GUEST_ERROR: &str = r#"{"error":"boom"}"#;
const CHECK_OK: &str = r#"{"ok":true}"#;
const CHECK_VIOLATION: &str = r#"{"violation":{"code":"TOO_MANY","message":"limit"}}"#;

/// A module with a bump allocator, constant replies, and misbehaving exports.
fn fixture_wat() -> String {
    format!(
        r#"(module
  (import "fold" "get_row" (func $get_row (param i32 i32 i32 i32 i32 i32) (result i64)))
  (import "fold" "log" (func $log (param i32 i32 i32)))
  (memory (export "memory") 1)
  (global $heap (mut i32) (i32.const 16384))
  (data (i32.const 1024) "{m}")
  (data (i32.const 1100) "{s}")
  (data (i32.const 1200) "{r}")
  (data (i32.const 1300) "{e}")
  (data (i32.const 1400) "t")
  (data (i32.const 1410) "{{}}")
  (data (i32.const 1500) "{ok}")
  (data (i32.const 1600) "{vio}")
  (func (export "fold_abi_version") (result i32) i32.const 1)
  (func (export "fold_alloc") (param $len i32) (result i32)
    (local $p i32)
    global.get $heap
    local.set $p
    global.get $heap
    local.get $len
    i32.add
    global.set $heap
    local.get $p)
  (func (export "fold_free") (param i32 i32))
  (func $pack (param $ptr i32) (param $len i32) (result i64)
    local.get $ptr
    i64.extend_i32_u
    i64.const 32
    i64.shl
    local.get $len
    i64.extend_i32_u
    i64.or)
  (func (export "project_ok") (param i32 i32) (result i64) (call $pack (i32.const 1024) (i32.const {ml})))
  (func (export "evolve_ok") (param i32 i32) (result i64) (call $pack (i32.const 1100) (i32.const {sl})))
  (func (export "handle_rejects") (param i32 i32) (result i64) (call $pack (i32.const 1200) (i32.const {rl})))
  (func (export "guest_error") (param i32 i32) (result i64) (call $pack (i32.const 1300) (i32.const {el})))
  (func (export "check_ok") (param i32 i32) (result i64) (call $pack (i32.const 1500) (i32.const {okl})))
  (func (export "check_violation") (param i32 i32) (result i64) (call $pack (i32.const 1600) (i32.const {viol})))
  (func (export "echo") (param $p i32) (param $l i32) (result i64) (call $pack (local.get $p) (local.get $l)))
  (func (export "spin") (param i32 i32) (result i64) (loop $l br $l) i64.const 0)
  (func (export "huge") (param i32 i32) (result i64) (call $pack (i32.const 0) (i32.const 0x7fffffff)))
  (func (export "wild") (param i32 i32) (result i64) (call $pack (i32.const 0x7ffffff0) (i32.const 16)))
  (func (export "read_row") (param i32 i32) (result i64) (local $n i64)
    (call $get_row (i32.const 1400) (i32.const 1) (i32.const 1410) (i32.const 2) (i32.const 8192) (i32.const 1024))
    local.set $n
    (if (result i64) (i64.lt_s (local.get $n) (i64.const 0))
      (then (call $pack (i32.const 1300) (i32.const {el})))
      (else (call $pack (i32.const 8192) (i32.wrap_i64 (local.get $n))))))
  (func (export "logs") (param i32 i32) (result i64)
    (call $log (i32.const 3) (i32.const 1400) (i32.const 1))
    (call $pack (i32.const 1024) (i32.const {ml})))
  (func (export "wrong_shape") (param i32) (result i32) i32.const 0)
)"#,
        m = MUTATIONS_OK.replace('"', "\\\""),
        s = STATE_OK.replace('"', "\\\""),
        r = REJECTED.replace('"', "\\\""),
        e = GUEST_ERROR.replace('"', "\\\""),
        ok = CHECK_OK.replace('"', "\\\""),
        vio = CHECK_VIOLATION.replace('"', "\\\""),
        okl = CHECK_OK.len(),
        viol = CHECK_VIOLATION.len(),
        ml = MUTATIONS_OK.len(),
        sl = STATE_OK.len(),
        rl = REJECTED.len(),
        el = GUEST_ERROR.len(),
    )
}

fn engine() -> Engine {
    Engine::new().expect("engine")
}

fn guest_with(limits: Limits) -> Guest {
    let engine = engine();
    let cache = ModuleCache::new(engine.clone());
    let module = cache
        .load_bytes("fixture", wat::parse_str(fixture_wat()).expect("wat"))
        .expect("compiles");
    Guest::new(&engine, &module, limits).expect("links")
}

fn guest() -> Guest {
    guest_with(Limits::default())
}

fn event() -> Event {
    Event {
        stream: "order-1".into(),
        r#type: "Orders.OrderPlaced@v1".into(),
        version: 0,
        position: 0,
        payload: json!({"order_id": "x"}),
        metadata: json!({}),
    }
}

fn projection_input() -> ProjectionInput {
    ProjectionInput {
        abi: 1,
        projection: "Orders.OrderTotals".into(),
        event: event(),
    }
}

struct Rows(Option<Vec<u8>>);
impl RowReader for Rows {
    fn get_row(&self, _table: &str, _key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        Ok(self.0.clone())
    }
}

struct FailingRows;
impl RowReader for FailingRows {
    fn get_row(&self, table: &str, _key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        Err(format!("disk on fire while reading {table}"))
    }
}

#[test]
fn projection_reply_is_decoded() {
    let g = guest();
    let muts = g
        .apply("project_ok", &projection_input(), Arc::new(Rows(None)))
        .expect("ok");
    assert!(muts.is_empty());
}

#[test]
fn evolve_reply_is_decoded() {
    let g = guest();
    let state = g
        .evolve(
            "evolve_ok",
            &EvolveInput {
                abi: 1,
                aggregate: "Orders.Order".into(),
                stream: "order-1".into(),
                key: json!("x"),
                version: None,
                state: None,
                event: event(),
            },
        )
        .expect("ok");
    assert_eq!(state, json!({"n": 1}));
}

#[test]
fn a_rejection_is_a_successful_reply() {
    let g = guest();
    let reply = g
        .handle(
            "handle_rejects",
            &CommandInput {
                abi: 1,
                aggregate: "Orders.Order".into(),
                stream: "order-1".into(),
                key: json!("x"),
                version: Some(0),
                state: Some(json!({})),
                now: "2026-10-07T00:00:00Z".into(),
                command: fold_wasm::Command {
                    r#type: "Orders.Order.PlaceOrder".into(),
                    payload: json!({}),
                },
            },
        )
        .expect("ok");
    match reply {
        CommandReply::Rejected(r) => assert_eq!(r.code, "NOPE"),
        other => panic!("expected a rejection, got {other:?}"),
    }
}

#[test]
fn a_guest_error_reply_becomes_guest_error() {
    let g = guest();
    let err = g
        .apply("guest_error", &projection_input(), Arc::new(Rows(None)))
        .unwrap_err();
    assert!(
        matches!(err, WasmError::GuestError(ref m) if m == "boom"),
        "{err}"
    );
}

#[test]
fn input_bytes_arrive_intact() {
    let g = guest();
    let input = br#"{"any":"thing","n":[1,2,3]}"#;
    let out = g.call_raw("echo", input, Arc::new(Rows(None))).expect("ok");
    assert_eq!(out, input);
}

#[test]
fn an_echo_is_not_a_valid_projection_reply() {
    // Negative control for the decode path: the bytes round-trip, but they
    // are not a ProjectionOutput, so apply must say so rather than invent one.
    let g = guest();
    let err = g
        .apply("echo", &projection_input(), Arc::new(Rows(None)))
        .unwrap_err();
    assert!(matches!(err, WasmError::BadOutput(_)), "{err}");
}

#[test]
fn an_infinite_loop_runs_out_of_fuel() {
    let g = guest_with(Limits {
        fuel: 100_000,
        ..Limits::default()
    });
    let err = g.call_raw("spin", b"{}", Arc::new(Rows(None))).unwrap_err();
    assert!(
        matches!(err, WasmError::OutOfFuel { fuel: 100_000 }),
        "{err}"
    );
}

#[test]
fn an_infinite_loop_with_unlimited_fuel_times_out() {
    let g = guest_with(Limits {
        fuel: u64::MAX,
        epoch_ticks: 3,
        ..Limits::default()
    });
    let started = std::time::Instant::now();
    let err = g.call_raw("spin", b"{}", Arc::new(Rows(None))).unwrap_err();
    assert!(matches!(err, WasmError::Timeout { ticks: 3 }), "{err}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "epoch deadline was not enforced promptly"
    );
}

#[test]
fn a_module_above_the_memory_limit_cannot_start() {
    // The fixture declares one 64 KiB page; allow less than that.
    let engine = engine();
    let cache = ModuleCache::new(engine.clone());
    let module = cache
        .load_bytes("fixture", wat::parse_str(fixture_wat()).expect("wat"))
        .expect("compiles");
    let err = Guest::new(
        &engine,
        &module,
        Limits {
            memory_bytes: 4096,
            ..Limits::default()
        },
    )
    .err()
    .expect("must fail");
    assert!(
        matches!(err, WasmError::MemoryLimit { bytes: 4096 }),
        "{err}"
    );
}

#[test]
fn an_oversized_reply_is_refused() {
    let g = guest_with(Limits {
        max_output_bytes: 1024,
        ..Limits::default()
    });
    let err = g.call_raw("huge", b"{}", Arc::new(Rows(None))).unwrap_err();
    assert!(
        matches!(err, WasmError::OutputTooLarge { max: 1024, .. }),
        "{err}"
    );
}

#[test]
fn a_reply_outside_memory_is_refused() {
    let g = guest();
    let err = g.call_raw("wild", b"{}", Arc::new(Rows(None))).unwrap_err();
    assert!(matches!(err, WasmError::BadPointer), "{err}");
}

#[test]
fn an_oversized_input_is_refused_before_running() {
    let g = guest_with(Limits {
        max_input_bytes: 8,
        ..Limits::default()
    });
    let err = g
        .call_raw("echo", b"0123456789", Arc::new(Rows(None)))
        .unwrap_err();
    assert!(
        matches!(err, WasmError::InputTooLarge { len: 10, max: 8 }),
        "{err}"
    );
}

#[test]
fn get_row_delivers_the_row_and_reports_absence() {
    let g = guest();
    let out = g
        .call_raw(
            "read_row",
            b"{}",
            Arc::new(Rows(Some(MUTATIONS_OK.as_bytes().to_vec()))),
        )
        .expect("ok");
    assert_eq!(out, MUTATIONS_OK.as_bytes());

    let out = g
        .call_raw("read_row", b"{}", Arc::new(Rows(None)))
        .expect("ok");
    assert_eq!(
        out,
        GUEST_ERROR.as_bytes(),
        "the guest saw -1 and took its absent branch"
    );
}

#[test]
fn a_row_reader_failure_surfaces_with_its_message() {
    let g = guest();
    let err = g
        .call_raw("read_row", b"{}", Arc::new(FailingRows))
        .unwrap_err();
    assert!(
        matches!(err, WasmError::RowRead(ref m) if m.contains("disk on fire")),
        "{err}"
    );
}

#[test]
fn evolve_and_handle_may_not_read_rows() {
    let g = guest();
    let err = g
        .evolve(
            "read_row",
            &EvolveInput {
                abi: 1,
                aggregate: "a".into(),
                stream: "s".into(),
                key: json!("x"),
                version: None,
                state: None,
                event: event(),
            },
        )
        .unwrap_err();
    assert!(matches!(err, WasmError::RowRead(_)), "{err}");
}

#[test]
fn guest_log_does_not_disturb_the_reply() {
    let g = guest();
    let muts = g
        .apply("logs", &projection_input(), Arc::new(Rows(None)))
        .expect("ok");
    assert!(muts.is_empty());
}

#[test]
fn has_export_checks_the_signature_too() {
    let g = guest();
    assert!(g.has_export("project_ok"));
    assert!(
        !g.has_export("wrong_shape"),
        "an (i32) -> i32 export is not an entry point"
    );
    assert!(!g.has_export("nope"));
}

#[test]
fn a_missing_export_is_named() {
    let g = guest();
    let err = g.call_raw("nope", b"{}", Arc::new(Rows(None))).unwrap_err();
    assert!(
        matches!(err, WasmError::MissingExport(ref n) if n == "nope"),
        "{err}"
    );
}

#[test]
fn a_wasi_import_is_refused_at_link_time() {
    let wat = r#"(module
      (import "wasi_snapshot_preview1" "proc_exit" (func (param i32)))
      (memory (export "memory") 1)
      (func (export "fold_abi_version") (result i32) i32.const 1)
      (func (export "fold_alloc") (param i32) (result i32) i32.const 1024)
      (func (export "fold_free") (param i32 i32)))"#;
    let engine = engine();
    let cache = ModuleCache::new(engine.clone());
    let module = cache
        .load_bytes("wasi", wat::parse_str(wat).unwrap())
        .unwrap();
    let err = Guest::new(&engine, &module, Limits::default())
        .err()
        .expect("refused");
    assert!(
        matches!(err, WasmError::UnsupportedImport { ref module, ref name } if module == "wasi_snapshot_preview1" && name == "proc_exit"),
        "{err}"
    );
}

#[test]
fn a_module_without_the_abi_exports_is_refused() {
    let wat = r#"(module (memory (export "memory") 1) (func (export "fold_abi_version") (result i32) i32.const 1))"#;
    let engine = engine();
    let cache = ModuleCache::new(engine.clone());
    let module = cache
        .load_bytes("bare", wat::parse_str(wat).unwrap())
        .unwrap();
    let err = Guest::new(&engine, &module, Limits::default())
        .err()
        .expect("refused");
    assert!(
        matches!(err, WasmError::MissingExport(ref n) if n == "fold_alloc"),
        "{err}"
    );
}

#[test]
fn a_wrong_abi_version_is_refused() {
    let wat = r#"(module
      (memory (export "memory") 1)
      (func (export "fold_abi_version") (result i32) i32.const 2)
      (func (export "fold_alloc") (param i32) (result i32) i32.const 1024)
      (func (export "fold_free") (param i32 i32)))"#;
    let engine = engine();
    let cache = ModuleCache::new(engine.clone());
    let module = cache
        .load_bytes("v2", wat::parse_str(wat).unwrap())
        .unwrap();
    let err = Guest::new(&engine, &module, Limits::default())
        .err()
        .expect("refused");
    assert!(matches!(err, WasmError::BadAbiVersion(2)), "{err}");
}

#[test]
fn the_cache_resolves_relative_to_the_schema_and_refuses_escapes() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = wat::parse_str(fixture_wat()).unwrap();
    std::fs::write(dir.path().join("orders.wasm"), &bytes).unwrap();
    let cache = ModuleCache::new(engine());

    let a = cache.load(dir.path(), "orders.wasm").expect("loads");
    let b = cache.load(dir.path(), "orders.wasm").expect("cached");
    assert!(Arc::ptr_eq(&a, &b), "second load must come from the cache");
    assert_eq!(a.hash, sha2_digest(&bytes));

    for bad in ["../orders.wasm", "/etc/passwd", "a/../../b.wasm"] {
        let err = cache.load(dir.path(), bad).unwrap_err();
        assert!(matches!(err, WasmError::PathEscapes(_)), "{bad}: {err}");
    }
    let err = cache.load(dir.path(), "missing.wasm").unwrap_err();
    assert!(matches!(err, WasmError::Io { .. }), "{err}");
}

fn sha2_digest(bytes: &[u8]) -> [u8; 32] {
    use std::process::Command;
    // Independent oracle: the system shasum, so the test does not restate
    // the implementation.
    let out = Command::new("shasum")
        .arg("-a")
        .arg("256")
        .arg("-")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            use std::io::Write;
            c.stdin.take().unwrap().write_all(bytes)?;
            c.wait_with_output()
        })
        .expect("shasum available");
    let hex = String::from_utf8(out.stdout).unwrap();
    let hex = hex.split_whitespace().next().unwrap();
    let mut arr = [0u8; 32];
    for (i, b) in arr.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap();
    }
    arr
}

fn check_input() -> CheckInput {
    CheckInput {
        abi: 1,
        ctx: InvCtx {
            invariant: "Orders.MaxOpenOrders".into(),
            aggregate: "Orders.Order".into(),
            stream: "order-1".into(),
            key: json!("1"),
            version: 0,
            projection: Some("Orders.CustomerOrders".into()),
            scope: Some(json!("c1")),
        },
        state: json!({"status": "Pending"}),
        events: vec![],
    }
}

#[test]
fn an_invariant_check_passes_or_reports_a_violation() {
    let g = guest();
    assert_eq!(
        g.check("check_ok", &check_input(), Guest::no_rows())
            .unwrap(),
        CheckReply::Ok
    );
    match g
        .check("check_violation", &check_input(), Guest::no_rows())
        .unwrap()
    {
        CheckReply::Violation(v) => assert_eq!(v.code, "TOO_MANY"),
        other => panic!("{other:?}"),
    }
    // Negative control: a projection reply is not a check reply.
    let err = g
        .check("project_ok", &check_input(), Guest::no_rows())
        .unwrap_err();
    assert!(matches!(err, WasmError::BadOutput(_)), "{err}");
}
