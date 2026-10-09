//! The domain the log is written under: `Schema.GetSchema`, the start-up
//! check against the stored text, and what a higher-layer root yields.

use fold_proto::common::v1::GetSchemaRequest;

use crate::common::{DbNode, register, uuid};

#[tokio::test]
async fn get_schema_is_the_domain_bundle_with_its_hash() {
    let mut d = DbNode::start().await;
    let s = d
        .schema()
        .await
        .get_schema(GetSchemaRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(s.layer, "domain");
    assert_eq!(s.source, crate::common::example_domain());
    assert_eq!(s.sha256, fold_db::domain::sha256_hex(&s.source));
    assert!(s.path.ends_with("domain.fold"), "{}", s.path);
    assert_eq!(
        fold_schema::compile_any(&s.source).unwrap().layer(),
        fold_schema::Layer::Domain
    );
    d.shutdown().await;
}

#[tokio::test]
async fn a_breaking_domain_change_is_refused_unless_forced() {
    let mut d = DbNode::start().await;
    register(&d, &uuid('c', 1)).await;
    // A required field on a stored event: breaking.
    d.rewrite_schema(|s| {
        s.replace(
            "event CustomerRegistered v1 { customer_id: uuid, name: string }",
            "event CustomerRegistered v1 { customer_id: uuid, name: string, email: string }",
        )
    });
    let err = d.try_restart().await.unwrap_err();
    let text = format!("{err:#}");
    assert!(text.contains("breaks data in the log"), "{text}");
    assert!(
        text.contains("[breaking] Customers.CustomerRegistered@v1.email"),
        "{text}"
    );
    // Compatible: an optional field; the note says so.
    d.rewrite_schema(|s| s.replace("email: string }", "email: string? }"));
    d.restart().await;
    let note = d.health().await.last_schema_change;
    assert!(note.starts_with("applied: 1 change(s)"), "{note}");
    // Textual only.
    d.rewrite_schema(|s| format!("// a comment\n{s}"));
    d.restart().await;
    assert_eq!(
        d.health().await.last_schema_change,
        "textual change only: stored the new text"
    );
    // Forced over a breaking change.
    d.rewrite_schema(|s| s.replace("email: string? }", "email: string }"));
    d.configure = std::sync::Arc::new(|o| o.force_schema = true);
    d.restart().await;
    assert!(
        d.health()
            .await
            .last_schema_change
            .starts_with("forced over a breaking change"),
    );
    d.shutdown().await;
}

#[tokio::test]
async fn an_application_root_yields_its_domain_and_stores_the_whole_bundle() {
    // The composite's case: the database is given the application file.
    let mut d = DbNode::start().await;
    let dir = crate::common::workspace().join("examples/orders");
    for f in ["domain.fold", "derive.fold", "app.fold"] {
        std::fs::copy(dir.join(f), d.data_dir().join(f)).unwrap();
    }
    let app = d.data_dir().join("app.fold");
    d.configure = std::sync::Arc::new(move |o| o.schema = app.clone());
    // The stored text changes (the domain alone → the whole bundle) with
    // no domain change.
    d.restart().await;
    assert_eq!(
        d.health().await.last_schema_change,
        "textual change only: stored the new text"
    );
    let s = d
        .schema()
        .await
        .get_schema(GetSchemaRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(s.layer, "domain", "the database reports what it keeps");
    assert!(
        s.source.starts_with("// ---- file: app.fold\n"),
        "{}",
        &s.source[..40]
    );
    assert_eq!(
        fold_schema::compile_any(&s.source).unwrap().layer(),
        fold_schema::Layer::Application
    );
    // A change in the derivation file is not the database's concern.
    std::fs::write(
        d.data_dir().join("derive.fold"),
        std::fs::read_to_string(dir.join("derive.fold"))
            .unwrap()
            .replace("snapshot every 100", "snapshot every 7"),
    )
    .unwrap();
    d.restart().await;
    assert_eq!(
        d.health().await.last_schema_change,
        "textual change only: stored the new text"
    );
    d.shutdown().await;
}
