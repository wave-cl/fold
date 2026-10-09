//! Linked wasm modules, one `Guest` per module path named by a schema.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context as _;
use fold_wasm::{Engine, Guest, Limits, ModuleCache};

/// Hands out the linked guest for a module path.
pub trait GuestSource: Send + Sync {
    /// The guest for `module`; every module a schema names was linked at
    /// start, so a miss is a programming error.
    fn guest(&self, module: &str) -> Arc<Guest>;
}

/// Module path as written in the schema → linked guest.
#[derive(Default)]
pub struct Guests {
    map: HashMap<String, Arc<Guest>>,
}

impl Guests {
    /// Compiles and links every module in `wants` (`(module, export)`
    /// pairs) from `dir`, checking that each export exists, so a bad module
    /// fails start-up rather than the first call that needs it.
    pub fn link(
        engine: &Engine,
        modules: &ModuleCache,
        dir: &Path,
        wants: &[(String, String)],
        limits: Limits,
    ) -> anyhow::Result<Guests> {
        let mut map: HashMap<String, Arc<Guest>> = HashMap::new();
        for (module, export) in wants {
            if !map.contains_key(module) {
                let loaded = modules
                    .load(dir, module)
                    .with_context(|| format!("cannot load wasm module {module}"))?;
                let guest = Guest::new(engine, &loaded, limits)
                    .with_context(|| format!("cannot link wasm module {module}"))?;
                map.insert(module.clone(), Arc::new(guest));
            }
            let guest = &map[module];
            anyhow::ensure!(
                guest.has_export(export),
                "wasm module {module} does not export `{export}` as (i32, i32) -> i64"
            );
        }
        Ok(Guests { map })
    }

    pub fn get(&self, module: &str) -> Option<Arc<Guest>> {
        self.map.get(module).cloned()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl GuestSource for Guests {
    fn guest(&self, module: &str) -> Arc<Guest> {
        self.map
            .get(module)
            .cloned()
            .expect("every module named by the schema was linked at startup")
    }
}
