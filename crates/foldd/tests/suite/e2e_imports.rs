//! A schema split over files: the daemon loads the root and its imports,
//! rebases the imported file's wasm path onto the root's directory, and
//! stores (and serves) the bundle.

use fold_proto::v1::GetSchemaRequest;
use serde_json::json;

use crate::common::{Daemon, line, settle, state_of, uuid, workspace};

/// The example schema with its Shipping context moved to `sub/shipping.fold`.
fn split_layout() -> Vec<(&'static str, String)> {
    let s = std::fs::read_to_string(workspace().join("examples/orders/schema.fold")).unwrap();
    let start = s.find("context Shipping {").expect("Shipping context");
    let end = start + s[start..].find("\n}\n").expect("its end") + 3;
    let shipping = s[start..end].to_string();
    let root = format!("{}{}", &s[..start], &s[end..]).replace(
        "/// Types every context shares.\ncontext Shared {",
        "import \"sub/shipping.fold\"\n\n/// Types every context shares.\ncontext Shared {",
    );
    assert!(root.contains("import \"sub/shipping.fold\""), "{root}");
    vec![("schema.fold", root), ("sub/shipping.fold", shipping)]
}

#[tokio::test]
async fn a_daemon_runs_a_schema_split_over_files() {
    let mut d = Daemon::start_layout(&split_layout(), &["sub"]).await;
    // The root's contexts work as before, and the process that spans both
    // files (Fulfilment prepares a shipment through the imported context)
    // reaches the shipment.
    let a = uuid('a', 1);
    let c = uuid('c', 1);
    let stream = format!("shipment-{a}");
    d.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{a}"),
        json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 1, "1.00")] }),
    )
    .await
    .unwrap();
    settle(&d, std::slice::from_ref(&stream)).await;
    assert_eq!(
        state_of(&d.aggregate(&stream).await.unwrap())["stage"],
        "Prepared"
    );
    // A command in the imported context runs its (rebased) module.
    d.exec("Shipping.Shipment.Ship", &stream, json!({}))
        .await
        .expect("ship runs through sub/orders.wasm");
    assert_eq!(
        state_of(&d.aggregate(&stream).await.unwrap())["stage"],
        "Shipped"
    );
    // The stored and served schema is the bundle.
    let got = d
        .admin()
        .await
        .get_schema(GetSchemaRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(
        got.source.starts_with("// ---- file: schema.fold\n"),
        "{}",
        &got.source[..60]
    );
    assert!(
        got.source
            .contains("\n// ---- file: sub/shipping.fold\ncontext Shipping {\n")
    );
    let stored =
        std::fs::read_to_string(d.data_dir().join("data/default/schema/current.fold")).unwrap();
    assert_eq!(stored, got.source);
    // It compiles to the same model as the files.
    let from_bundle = fold_schema::Sources::from_bundle(&stored)
        .compile()
        .unwrap();
    let from_disk = fold_schema::Sources::load(d.data_dir().join("schema.fold"))
        .unwrap()
        .compile()
        .unwrap();
    assert_eq!(from_bundle.contexts, from_disk.contexts);
    assert_eq!(
        from_bundle.contexts["Shipping"].aggregates["Shipment"]
            .evolve
            .module,
        "sub/orders.wasm"
    );
    // A restart loads the same split schema again.
    d.restart().await;
    assert_eq!(
        state_of(&d.aggregate(&stream).await.unwrap())["stage"],
        "Shipped"
    );
    d.shutdown().await;
}
