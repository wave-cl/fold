//! The online commands against a running composite, and the per-layer
//! address flags against the same composite named three times: `fold`
//! reaches the right service for each command.

use std::path::{Path, PathBuf};
use std::process::Command as Process;

use assert_cmd::Command;
use predicates::prelude::*;

fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

/// Builds `orders-guest` for wasm32 into its own target dir and copies the
/// module to `dest`, under the file lock the other suites share.
fn copy_orders_guest(dest: &Path) {
    let workspace = workspace();
    let target_dir = workspace.join("target/guest");
    std::fs::create_dir_all(&target_dir).unwrap();
    let lock = std::fs::File::create(target_dir.join(".guest.lock")).unwrap();
    lock.lock().expect("guest build lock");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let status = Process::new(cargo)
        .current_dir(&workspace)
        .args([
            "build",
            "-p",
            "orders-guest",
            "--target",
            "wasm32-unknown-unknown",
            "--release",
            "--target-dir",
        ])
        .arg(&target_dir)
        .status()
        .expect("cargo runs");
    assert!(status.success(), "building orders-guest for wasm32 failed");
    std::fs::copy(
        target_dir.join("wasm32-unknown-unknown/release/orders_guest.wasm"),
        dest,
    )
    .unwrap();
    lock.unlock().expect("guest build unlock");
}

/// A composite over the example with the orders application embedded,
/// started in this process.
struct Composite {
    _dir: tempfile::TempDir,
    rt: tokio::runtime::Runtime,
    running: Option<foldd::Running>,
    url: String,
}

impl Composite {
    fn start() -> Composite {
        let dir = tempfile::tempdir().unwrap();
        let example = workspace().join("examples/orders");
        for f in ["domain.fold", "derive.fold"] {
            std::fs::copy(example.join(f), dir.path().join(f)).unwrap();
        }
        copy_orders_guest(&dir.path().join("orders.wasm"));
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut opts = foldd::Options::new(
            dir.path().join("data"),
            dir.path().join("derive.fold"),
            "127.0.0.1:0".parse().unwrap(),
        );
        opts.fsync = false;
        opts.limits.epoch_ticks = 3_000;
        let running = rt
            .block_on(foldd::start_with_app(opts, orders_app::app()))
            .expect("the composite starts");
        let url = format!("http://{}", running.local_addr);
        Composite {
            _dir: dir,
            rt,
            running: Some(running),
            url,
        }
    }

    fn stop(mut self) {
        if let Some(r) = self.running.take() {
            self.rt.block_on(r.shutdown()).unwrap();
        }
    }

    /// `fold` pointed at the composite.
    fn fold(&self) -> Command {
        let mut c = Command::cargo_bin("fold").unwrap();
        c.env("FOLD_ADDR", &self.url);
        c.env_remove("FOLD_DB_ADDR");
        c.env_remove("FOLD_DERIVE_ADDR");
        c.env_remove("FOLD_APP_ADDR");
        c
    }
}

fn uuid(prefix: char, n: u32) -> String {
    format!("{prefix}0000000-0000-0000-0000-{n:012}")
}

#[test]
fn the_online_commands_reach_each_layer() {
    let d = Composite::start();
    let c = uuid('c', 1);
    let a = uuid('a', 1);

    // health: all three layers answer on the one address.
    d.fold()
        .arg("health")
        .assert()
        .success()
        .stdout(predicate::str::contains("database: ok"))
        .stdout(predicate::str::contains("derivation: ok"))
        .stdout(predicate::str::contains("application: ok"))
        .stdout(predicate::str::contains("layer check ok"));

    // exec: the application node.
    d.fold()
        .args([
            "exec",
            "Customers.Customer.Register",
            &format!("customer-{c}"),
            "-d",
            r#"{"name":"Ada"}"#,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("1 event(s), stream at version 0"));
    let out = d
        .fold()
        .args([
            "--json",
            "exec",
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            "-d",
            &json_line(&c),
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let placed: serde_json::Value = serde_json::from_slice(&out).unwrap();
    let last = placed["last_position"].as_u64().unwrap();
    assert!(!placed["token"].as_str().unwrap().is_empty());

    // query: the derivation node, waiting for the write's position.
    d.fold()
        .args([
            "query",
            "get",
            "Orders.CustomerOrders",
            "customer_orders",
            &format!(r#"{{"customer_id":"{c}"}}"#),
            "--after",
            &last.to_string(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"name\": \"Ada\""))
        .stdout(predicate::str::contains(&a));

    // log aggregate: the derivation node; log read: the database.
    d.fold()
        .args(["log", "aggregate", &format!("order-{a}")])
        .assert()
        .success()
        .stdout(predicate::str::contains("aggregate: Orders.Order"))
        .stdout(predicate::str::contains("version:   0"));
    d.fold()
        .args(["log", "read", &format!("order-{a}")])
        .assert()
        .success()
        .stdout(predicate::str::contains("Orders.OrderPlaced@v1"));

    // process list and projection list: the application and derivation
    // nodes' admin services.
    d.fold()
        .args(["process", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Orders.Fulfilment"));
    d.fold()
        .args(["projection", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Orders.CustomerOrders"))
        .stdout(predicate::str::contains("customer_orders"));

    // append: guarded on the application node; --unguarded on the
    // database, with a warning.
    let c2 = uuid('c', 2);
    d.fold()
        .args([
            "append",
            &format!("customer-{c2}"),
            "Customers.CustomerRegistered",
            "-d",
            &format!(r#"{{"customer_id":"{c2}","name":"Bob"}}"#),
            "--expect",
            "none",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("stream now at version 0"));
    let c3 = uuid('c', 3);
    d.fold()
        .args([
            "append",
            &format!("customer-{c3}"),
            "Customers.CustomerRegistered",
            "-d",
            &format!(r#"{{"customer_id":"{c3}","name":"Cy"}}"#),
            "--expect",
            "none",
            "--unguarded",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("stream now at version 0"))
        .stderr(predicate::str::contains("invariants are not checked"));

    // schema show, per layer.
    d.fold()
        .args(["schema", "show", "--layer", "domain"])
        .assert()
        .success()
        .stdout(predicate::str::contains("(domain layer, sha256 "))
        .stdout(predicate::str::contains("context Orders {"));
    d.fold()
        .args(["schema", "show"])
        .assert()
        .success()
        .stdout(predicate::str::contains("(application layer, sha256 "))
        .stdout(predicate::str::contains("\"Orders.Fulfilment\""));

    // A rejection exits 1 with the code; an unreachable address exits 2.
    d.fold()
        .args([
            "exec",
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            "-d",
            &json_line(&c),
        ])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("ALREADY_PLACED"));
    d.fold()
        .args(["--addr", "http://127.0.0.1:1", "log", "read", "x"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("cannot reach fold at"));

    // The per-layer flags: --addr points nowhere, each layer is named.
    d.fold()
        .args([
            "--addr",
            "http://127.0.0.1:1",
            "--db",
            &d.url,
            "--derive",
            &d.url,
            "--app",
            &d.url,
            "health",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("application: ok"));
    // One layer missing: the others print, the exit code says so.
    d.fold()
        .args(["--derive", "http://127.0.0.1:1", "health"])
        .assert()
        .code(2)
        .stdout(predicate::str::contains("database: ok"))
        .stdout(predicate::str::contains("derivation: "))
        .stdout(predicate::str::contains("application: ok"))
        .stderr(predicate::str::contains("derivation did not answer"));
    d.stop();
}

fn json_line(customer: &str) -> String {
    format!(
        r#"{{"customer_id":"{customer}","lines":[{{"line_id":"10000000-0000-0000-0000-000000000001","sku":"SKU","qty":2,"price":{{"amount":"7.50","currency":"EUR"}}}}]}}"#
    )
}
