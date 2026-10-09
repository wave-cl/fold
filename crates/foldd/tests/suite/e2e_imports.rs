//! A schema split over files: the daemon loads the root and its imports,
//! rebases the imported file's wasm path onto the root's directory, and
//! stores (and serves) the bundle.

use fold_proto::v1::GetSchemaRequest;
use serde_json::json;

use crate::common::{Daemon, line, settle, state_of, uuid, workspace};

/// The example schema with its Shipping context, state and commands moved
/// to `sub/` files of their layers, each imported by the example file of
/// the same layer.
fn split_layout() -> Vec<(&'static str, String)> {
    let dir = workspace().join("examples/orders");
    let read = |name: &str| std::fs::read_to_string(dir.join(name)).unwrap();
    // A block from `start` to the first line that is just `}`.
    fn cut(text: &str, start: &str) -> (String, String) {
        let s = text.find(start).unwrap_or_else(|| panic!("{start} moved"));
        let e = s + text[s..].find("\n}\n").expect("its end") + 3;
        (
            text[s..e].to_string(),
            format!("{}{}", &text[..s], &text[e..]),
        )
    }
    let (shipping_ctx, domain) = cut(&read("domain.fold"), "context Shipping {");
    let domain = domain.replace(
        "layer domain\n",
        "layer domain\n\nimport \"sub/shipping.fold\"\n",
    );
    let (shipping_cmds, app) = cut(&read("app.fold"), "commands Shipping.Shipment {");
    let app = app.replace(
        "import \"derive.fold\"\n",
        "import \"derive.fold\"\nimport \"sub/shipping_app.fold\"\n",
    );
    let derive = read("derive.fold");
    let state_line = "state Shipping.Shipment { order_id: uuid, stage: Stage }\n  evolve wasm \"orders.wasm\" export \"evolve_shipment\"\n";
    assert!(derive.contains(state_line), "{derive}");
    let derive = derive.replace(state_line, "").replace(
        "import \"domain.fold\"\n",
        "import \"domain.fold\"\nimport \"sub/shipping_derive.fold\"\n",
    );
    vec![
        ("app.fold", app),
        ("derive.fold", derive),
        ("domain.fold", domain),
        (
            "sub/shipping.fold",
            format!("layer domain\n\n{shipping_ctx}"),
        ),
        (
            "sub/shipping_derive.fold",
            format!("layer derivation\n\n{state_line}"),
        ),
        (
            "sub/shipping_app.fold",
            format!("layer application\n\n{shipping_cmds}"),
        ),
    ]
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
        got.source.starts_with("// ---- file: app.fold\n"),
        "{}",
        &got.source[..60]
    );
    assert!(
        got.source
            .contains("\n// ---- file: sub/shipping.fold\nlayer domain\n\ncontext Shipping {\n"),
        "{}",
        got.source
    );
    assert!(
        got.source
            .contains("\n// ---- file: sub/shipping_derive.fold\nlayer derivation\n"),
        "{}",
        got.source
    );
    let stored =
        std::fs::read_to_string(d.data_dir().join("data/default/schema/current.fold")).unwrap();
    assert_eq!(stored, got.source);
    // It compiles to the same model as the files.
    let from_bundle = fold_schema::Sources::from_bundle(&stored)
        .compile_application()
        .unwrap();
    let from_disk = fold_schema::Sources::load(d.schema_path())
        .unwrap()
        .compile_application()
        .unwrap();
    assert_eq!(from_bundle.contexts, from_disk.contexts);
    assert_eq!(from_bundle.states, from_disk.states);
    assert_eq!(from_bundle.commands, from_disk.commands);
    assert_eq!(
        from_bundle
            .state_of("Shipping", "Shipment")
            .unwrap()
            .evolve
            .module,
        "sub/orders.wasm"
    );
    assert_eq!(
        from_bundle
            .command(&fold_schema::AggRef::new("Shipping", "Shipment"), "Ship")
            .unwrap()
            .handler
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
