use fold_schema::{Scalar, StreamTemplate, TemplateError};
use serde_json::{Value, json};

use super::common::{U1, orders};

#[test]
fn parse_splits_prefix_placeholder_suffix() {
    let t = StreamTemplate::parse("order-{order_id}").unwrap();
    assert_eq!(t.prefix(), "order-");
    assert_eq!(t.placeholder(), "order_id");
    assert_eq!(t.suffix(), "");
    assert_eq!(t.key_type(), Scalar::String);
    assert_eq!(t.to_string(), "order-{order_id}");
    let t = StreamTemplate::parse("{id}").unwrap();
    assert_eq!((t.prefix(), t.suffix()), ("", ""));
    let t = StreamTemplate::parse("a/{id}/b").unwrap();
    assert_eq!((t.prefix(), t.suffix()), ("a/", "/b"));
}

#[test]
fn parse_errors() {
    assert_eq!(
        StreamTemplate::parse("order").unwrap_err(),
        TemplateError::NoPlaceholder
    );
    assert_eq!(
        StreamTemplate::parse("{a}-{b}").unwrap_err(),
        TemplateError::MultiplePlaceholders
    );
    assert_eq!(
        StreamTemplate::parse("{a}-{a}").unwrap_err(),
        TemplateError::MultiplePlaceholders
    );
    assert_eq!(
        StreamTemplate::parse("order-{id").unwrap_err(),
        TemplateError::Unclosed
    );
    assert_eq!(
        StreamTemplate::parse("order-{{id}").unwrap_err(),
        TemplateError::Unclosed
    );
    assert_eq!(
        StreamTemplate::parse("order-id}").unwrap_err(),
        TemplateError::UnmatchedClose
    );
    assert_eq!(
        StreamTemplate::parse("order-{}").unwrap_err(),
        TemplateError::EmptyPlaceholder
    );
    assert_eq!(
        StreamTemplate::parse("order-{1x}").unwrap_err(),
        TemplateError::BadPlaceholder { name: "1x".into() }
    );
}

#[test]
fn render_and_match_uuid() {
    let t = StreamTemplate::parse("order-{order_id}")
        .unwrap()
        .with_key_type(Scalar::Uuid);
    let id = format!("order-{U1}");
    assert_eq!(t.render(&json!(U1)).unwrap(), id);
    assert_eq!(t.matches(&id), Some(json!(U1)));
    assert_eq!(t.matches("order-"), None);
    assert_eq!(t.matches("order-not-a-uuid"), None);
    assert_eq!(t.matches(&format!("customer-{U1}")), None);
    let hex = "0a1b2c3d-0a1b-0a1b-0a1b-0a1b2c3d4e5f";
    assert_eq!(t.matches(&format!("order-{hex}")), Some(json!(hex)));
    assert_eq!(
        t.matches(&format!("order-{}", hex.to_uppercase())),
        None,
        "non-canonical uuid"
    );
    assert_eq!(
        t.matches(&format!("order-{}", hex.replace('-', ""))),
        None,
        "non-canonical uuid"
    );
    assert!(matches!(
        t.render(&json!("nope")),
        Err(TemplateError::BadKey { .. })
    ));
    assert!(matches!(
        t.render(&json!(42)),
        Err(TemplateError::BadKey { .. })
    ));
}

#[test]
fn render_and_match_int_and_uint() {
    let t = StreamTemplate::parse("acct-{n}")
        .unwrap()
        .with_key_type(Scalar::Int);
    assert_eq!(t.render(&json!(-7)).unwrap(), "acct--7");
    assert_eq!(t.matches("acct--7"), Some(json!(-7)));
    assert_eq!(t.matches("acct-042"), None, "non-canonical int");
    assert_eq!(t.matches("acct-x"), None);
    assert!(matches!(
        t.render(&json!("7")),
        Err(TemplateError::BadKey { .. })
    ));
    let t = StreamTemplate::parse("acct-{n}")
        .unwrap()
        .with_key_type(Scalar::Uint);
    assert_eq!(t.render(&json!(7)).unwrap(), "acct-7");
    assert_eq!(t.matches("acct-7"), Some(json!(7)));
    assert_eq!(t.matches("acct--7"), None);
    assert!(matches!(
        t.render(&json!(-1)),
        Err(TemplateError::BadKey { .. })
    ));
}

#[test]
fn render_and_match_string() {
    let t = StreamTemplate::parse("tenant/{slug}/x").unwrap();
    assert_eq!(t.render(&json!("acme")).unwrap(), "tenant/acme/x");
    assert_eq!(t.matches("tenant/acme/x"), Some(json!("acme")));
    assert_eq!(t.matches("tenant//x"), None, "empty key never matches");
    assert_eq!(t.matches("tenant/acme/y"), None);
    assert_eq!(t.render(&json!("")).unwrap_err(), TemplateError::EmptyKey);
    assert!(matches!(
        t.render(&Value::Null),
        Err(TemplateError::BadKey { .. })
    ));
}

#[test]
fn aggregate_for_stream_tries_every_aggregate() {
    let s = orders();
    let (ctx, agg, key) = s.aggregate_for_stream(&format!("order-{U1}")).unwrap();
    assert_eq!((ctx.name.as_str(), agg.name.as_str()), ("Orders", "Order"));
    assert_eq!(key, json!(U1));
    let (ctx, agg, key) = s.aggregate_for_stream(&format!("customer-{U1}")).unwrap();
    assert_eq!(
        (ctx.name.as_str(), agg.name.as_str()),
        ("Customers", "Customer")
    );
    assert_eq!(key, json!(U1));
    assert!(s.aggregate_for_stream("order-abc").is_none());
    assert!(s.aggregate_for_stream("invoice-1").is_none());
    // and the template on the model renders back
    assert_eq!(agg.stream.render(&key).unwrap(), format!("customer-{U1}"));
}
