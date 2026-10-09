//! Single integration-test binary; one module per area.

mod suite {
    pub mod common;
    mod fmt;
    mod parser;
    mod resolve;
    mod rows;
    mod template;
    mod upcast;
    mod validate;
}
