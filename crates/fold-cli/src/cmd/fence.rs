use fold_proto::v1::FenceRequest;
use serde_json::json;

use crate::client;
use crate::output::Format;

pub async fn run(epoch: u64, addr: &str, format: Format) -> anyhow::Result<()> {
    let r = client::admin(addr)
        .await?
        .fence(FenceRequest { epoch })
        .await?
        .into_inner();
    match format {
        Format::Json => println!("{}", json!({ "role": r.role, "epoch": r.epoch })),
        Format::Human => println!(
            "{}: role {}, its own epoch {}",
            if r.role == "fenced" {
                "fenced"
            } else {
                "not taking writes anyway"
            },
            r.role,
            r.epoch
        ),
    }
    Ok(())
}
