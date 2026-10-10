//! The registrations this store's process tables were derived under,
//! against the application's: a process whose key or sources changed
//! starts over, one no longer registered has its tables dropped, a timer
//! no longer declared has its rows removed.

use fold_store::DerivedStore;

use crate::app::Manifest;

pub fn check(
    store: &DerivedStore,
    new: &Manifest,
    new_text: &str,
) -> anyhow::Result<Option<String>> {
    let Some(stored) = store.schema_source()? else {
        store.set_schema_source(new_text)?;
        return Ok(None);
    };
    if stored == new_text {
        return Ok(None);
    }
    let old: Manifest = match serde_json::from_str(&stored) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(error = %e, "the stored manifest is unreadable; replaced without a comparison");
            store.set_schema_source(new_text)?;
            return Ok(Some(
                "replaced an unreadable manifest; nothing was rebuilt".to_string(),
            ));
        }
    };
    let mut reset = 0;
    let mut dropped = 0;
    let mut timers_removed = 0;
    for old_proc in &old.processes {
        let Some(new_proc) = new.process(&old_proc.name) else {
            tracing::info!(process = %old_proc.name, "no longer registered; dropping its tables and checkpoint");
            store.reset(&old_proc.name, &crate::process::TABLES)?;
            dropped += 1;
            continue;
        };
        if old_proc.derivation_changed(new_proc) {
            tracing::info!(process = %old_proc.name, "its key or sources changed; starting over");
            store.reset(&old_proc.name, &crate::process::TABLES)?;
            reset += 1;
            continue;
        }
        for timer in &old_proc.timers {
            if new_proc.timers.contains(timer) {
                continue;
            }
            let rows = store.snapshot()?.scan(
                &old_proc.name,
                crate::process::TIMERS_TABLE,
                &[],
                1 << 20,
            )?;
            let mut doomed = Vec::new();
            for (key, bytes) in rows {
                if let Ok(row) = serde_json::from_slice::<crate::process::TimerRow>(&bytes)
                    && row.name == *timer
                {
                    doomed.push((crate::process::TIMERS_TABLE.to_string(), key));
                }
            }
            if !doomed.is_empty()
                && let Some(cp) = store.checkpoint(&old_proc.name)?
            {
                timers_removed += doomed.len();
                store.commit(&old_proc.name, cp, vec![], doomed)?;
            }
        }
    }
    store.set_schema_source(new_text)?;
    let note = if reset == 0 && dropped == 0 && timers_removed == 0 {
        "registrations changed without a rebuild: stored the new manifest".to_string()
    } else {
        format!(
            "applied: {reset} process(es) reset, {dropped} dropped, {timers_removed} timer row(s) removed"
        )
    };
    Ok(Some(note))
}
