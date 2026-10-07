use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};

use crate::engine::Engine;
use crate::error::WasmError;

/// A compiled module plus the identity of the bytes it came from.
pub struct LoadedModule {
    pub(crate) module: wasmtime::Module,
    /// SHA-256 of the module bytes. Snapshots record it so state evolved by
    /// an older module is not trusted after the module changes.
    pub hash: [u8; 32],
    pub path: PathBuf,
}

impl std::fmt::Debug for LoadedModule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedModule")
            .field("path", &self.path)
            .field("hash", &format_args!("{:02x?}", &self.hash[..4]))
            .finish_non_exhaustive()
    }
}

/// Compiles each module file once per process. Hot reload is out of scope:
/// a changed file is only seen by a new cache.
pub struct ModuleCache {
    engine: Engine,
    modules: Mutex<HashMap<PathBuf, Arc<LoadedModule>>>,
}

impl ModuleCache {
    pub fn new(engine: Engine) -> Self {
        ModuleCache {
            engine,
            modules: Mutex::new(HashMap::new()),
        }
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Resolves `rel` (as written in the schema) against `schema_dir` and
    /// compiles it, or returns the cached compilation.
    pub fn load(&self, schema_dir: &Path, rel: &str) -> Result<Arc<LoadedModule>, WasmError> {
        let rel_path = Path::new(rel);
        if rel_path.is_absolute()
            || rel_path.components().any(|c| {
                matches!(
                    c,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(WasmError::PathEscapes(rel.to_string()));
        }
        let path = schema_dir.join(rel_path);
        if let Some(m) = self
            .modules
            .lock()
            .expect("module cache poisoned")
            .get(&path)
        {
            return Ok(m.clone());
        }
        let bytes = std::fs::read(&path).map_err(|source| WasmError::Io {
            path: path.clone(),
            source,
        })?;
        let loaded = Arc::new(compile(&self.engine, bytes, path.clone())?);
        self.modules
            .lock()
            .expect("module cache poisoned")
            .insert(path, loaded.clone());
        Ok(loaded)
    }

    /// Compiles module bytes that did not come from a file (tests, embedding).
    pub fn load_bytes(&self, name: &str, bytes: Vec<u8>) -> Result<Arc<LoadedModule>, WasmError> {
        compile(&self.engine, bytes, PathBuf::from(name)).map(Arc::new)
    }
}

fn compile(engine: &Engine, bytes: Vec<u8>, path: PathBuf) -> Result<LoadedModule, WasmError> {
    let hash: [u8; 32] = Sha256::digest(&bytes).into();
    let module = wasmtime::Module::new(engine.raw(), &bytes).map_err(|e| WasmError::Compile {
        path: path.clone(),
        reason: e.to_string(),
    })?;
    Ok(LoadedModule { module, hash, path })
}
