//! A schema's files: the root and everything it imports, in the order the
//! contexts are merged (root first, then each import depth-first, each file
//! once). [`Sources::compile`] turns them into one [`Schema`]; [`Sources::bundle`]
//! is the single text the daemon stores, which [`Sources::from_bundle`] turns
//! back into the same files.
//!
//! An `import "rel.fold"` is relative to the importing file; the path follows
//! the wasm-path rules (relative, no `..`, no `:`; S047). Wasm paths inside an
//! imported file are rebased onto the root's directory at compile time, so a
//! schema loaded from disk and the bundle stored from it resolve the same
//! modules.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use crate::ast;
use crate::diag::{Diagnostic, Diagnostics, Section};
use crate::model::Schema;
use crate::span::Span;

/// Reads a file by path; the file system by default, a map in tests and
/// for a stored bundle.
pub trait Loader {
    fn load(&self, path: &Path) -> io::Result<String>;
}

/// The file system.
pub struct FsLoader;

impl Loader for FsLoader {
    fn load(&self, path: &Path) -> io::Result<String> {
        std::fs::read_to_string(path)
    }
}

/// Files held in memory, keyed by the path they are asked for (with `/`
/// separators).
#[derive(Default)]
pub struct MapLoader(pub HashMap<String, String>);

impl Loader for MapLoader {
    fn load(&self, path: &Path) -> io::Result<String> {
        let key = path.to_string_lossy().replace('\\', "/");
        self.0
            .get(&key)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("no such file: {key}")))
    }
}

/// One file of a schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceFile {
    /// Root-relative path with `/` separators; the root is its file name.
    pub path: String,
    pub text: String,
}

impl SourceFile {
    /// The root-relative directory, `""` for a file beside the root.
    pub fn dir(&self) -> &str {
        self.path.rsplit_once('/').map_or("", |(d, _)| d)
    }
}

/// The marker that starts each file's section in a multi-file bundle.
pub const BUNDLE_MARKER: &str = "// ---- file: ";

/// A schema's files, root first.
#[derive(Clone, Debug)]
pub struct Sources {
    files: Vec<SourceFile>,
    /// Parsed files, in `files` order; `None` where parsing failed (the
    /// error is in `pending`).
    asts: Vec<Option<ast::File>>,
    /// Problems found while loading: (file index, diagnostic with a span in
    /// that file).
    pending: Vec<(usize, Diagnostic)>,
    /// The root's directory on disk, for [`Schema::dir`].
    root_dir: Option<PathBuf>,
}

impl Sources {
    /// One file given as text: imports are an error (S046), since there is
    /// no directory to resolve them against.
    pub fn single(text: &str) -> Sources {
        let mut s = Sources {
            files: vec![SourceFile {
                path: "schema.fold".to_string(),
                text: text.to_string(),
            }],
            asts: Vec::new(),
            pending: Vec::new(),
            root_dir: None,
        };
        let ast = s.parse(0);
        if let Some(ast) = &ast {
            for imp in &ast.imports {
                s.pending.push((
                    0,
                    Diagnostic {
                        code: "S046",
                        span: imp.span,
                        message: "a schema compiled from text cannot import; load it from a file"
                            .to_string(),
                    },
                ));
            }
        }
        s.asts.push(ast);
        s
    }

    /// The root at `path` and everything it imports, from the file system.
    pub fn load(path: impl AsRef<Path>) -> Result<Sources, crate::Error> {
        Sources::load_with(path.as_ref(), &FsLoader)
    }

    /// The root at `root` and everything it imports, through `loader`. An
    /// unreadable root is [`crate::Error::Io`]; an unreadable or malformed
    /// import is a diagnostic (S046 / P001) reported by [`Sources::compile`].
    pub fn load_with(root: &Path, loader: &dyn Loader) -> Result<Sources, crate::Error> {
        let root_dir = root
            .parent()
            .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
        let root_name = root
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "schema.fold".to_string());
        let text = loader.load(root).map_err(|source| crate::Error::Io {
            path: root.to_path_buf(),
            source,
        })?;
        let mut s = Sources {
            files: Vec::new(),
            asts: Vec::new(),
            pending: Vec::new(),
            root_dir: Some(root_dir.clone()),
        };
        s.add(root_name, text);
        let mut i = 0;
        // `files` grows while we walk it; each file's imports are visited
        // right after it (depth-first), by inserting them in order.
        while i < s.files.len() {
            let Some(ast) = s.asts[i].clone() else {
                i += 1;
                continue;
            };
            let dir = s.files[i].dir().to_string();
            let mut insert_at = i + 1;
            for imp in &ast.imports {
                let rel = match resolve_import(&dir, &imp.path.value) {
                    Ok(rel) => rel,
                    Err(msg) => {
                        s.pending.push((
                            i,
                            Diagnostic {
                                code: "S047",
                                span: imp.path.span,
                                message: format!("{msg}: {:?}", imp.path.value),
                            },
                        ));
                        continue;
                    }
                };
                if s.files.iter().any(|f| f.path == rel) {
                    continue;
                }
                let disk = if root_dir.as_os_str().is_empty() {
                    PathBuf::from(&rel)
                } else {
                    root_dir.join(&rel)
                };
                match loader.load(&disk) {
                    Ok(text) => {
                        s.insert(insert_at, rel, text);
                        insert_at += 1;
                    }
                    Err(e) => s.pending.push((
                        i,
                        Diagnostic {
                            code: "S046",
                            span: imp.path.span,
                            message: format!("cannot read import {rel:?}: {e}"),
                        },
                    )),
                }
            }
            i += 1;
        }
        Ok(s)
    }

    /// The files of a bundle written by [`Sources::bundle`]: a text without
    /// the marker is one file; otherwise each `// ---- file: path` line
    /// starts a file, the first being the root.
    pub fn from_bundle(text: &str) -> Sources {
        if !text.starts_with(BUNDLE_MARKER) {
            return Sources::single(text);
        }
        let mut map = HashMap::new();
        let mut order = Vec::new();
        let mut current: Option<(String, String)> = None;
        for line in text.split_inclusive('\n') {
            let bare = line.strip_suffix('\n').unwrap_or(line);
            if let Some(path) = bare.strip_prefix(BUNDLE_MARKER) {
                if let Some((p, t)) = current.take() {
                    map.insert(p, t);
                }
                order.push(path.to_string());
                current = Some((path.to_string(), String::new()));
            } else if let Some((_, t)) = &mut current {
                t.push_str(line);
            }
        }
        if let Some((p, t)) = current.take() {
            map.insert(p, t);
        }
        let root = order.first().cloned().unwrap_or_default();
        let loader = MapLoader(map);
        match Sources::load_with(Path::new(&root), &loader) {
            Ok(mut s) => {
                s.root_dir = None;
                s
            }
            // The root section always exists in the map.
            Err(_) => Sources::single(text),
        }
    }

    fn add(&mut self, path: String, text: String) {
        self.files.push(SourceFile { path, text });
        let ast = self.parse(self.files.len() - 1);
        self.asts.push(ast);
    }

    fn insert(&mut self, at: usize, path: String, text: String) {
        self.files.insert(at, SourceFile { path, text });
        self.asts.insert(at, None);
        for (i, _) in &mut self.pending {
            if *i >= at {
                *i += 1;
            }
        }
        let ast = self.parse(at);
        self.asts[at] = ast;
    }

    fn parse(&mut self, i: usize) -> Option<ast::File> {
        match crate::parser::parse(&self.files[i].text) {
            Ok(f) => Some(f),
            Err(e) => {
                self.pending.push((
                    i,
                    Diagnostic {
                        code: "P001",
                        span: e.span,
                        message: e.to_string(),
                    },
                ));
                None
            }
        }
    }

    pub fn files(&self) -> &[SourceFile] {
        &self.files
    }

    pub fn root(&self) -> &SourceFile {
        &self.files[0]
    }

    /// The root's directory on disk, when loaded from one.
    pub fn root_dir(&self) -> Option<&Path> {
        self.root_dir.as_deref()
    }

    /// One text holding every file: the root verbatim when it imports
    /// nothing, else each file after a `// ---- file: path` line.
    pub fn bundle(&self) -> String {
        if self.files.len() == 1 {
            return self.files[0].text.clone();
        }
        let mut out = String::new();
        for f in &self.files {
            out.push_str(BUNDLE_MARKER);
            out.push_str(&f.path);
            out.push('\n');
            out.push_str(&f.text);
            if !f.text.ends_with('\n') {
                out.push('\n');
            }
        }
        out
    }

    /// Where each file's text sits in [`Sources::bundle`].
    fn sections(&self) -> Vec<Section> {
        if self.files.len() == 1 {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(self.files.len());
        let mut at = 0;
        for f in &self.files {
            at += BUNDLE_MARKER.len() + f.path.len() + 1;
            let end = at + f.text.len();
            out.push(Section {
                path: f.path.clone(),
                start: at,
                end,
            });
            at = end + usize::from(!f.text.ends_with('\n'));
        }
        out
    }

    /// Resolve every file into one schema: the root's contexts first, then
    /// each import's in load order, with wasm paths rebased onto the root's
    /// directory. Diagnostics carry the bundle and its sections.
    pub fn compile(&self) -> Result<Schema, Diagnostics> {
        let bundle = self.bundle();
        let sections = self.sections();
        let offset = |i: usize| sections.get(i).map_or(0, |s| s.start);
        if !self.pending.is_empty() {
            let diags = self
                .pending
                .iter()
                .map(|(i, d)| Diagnostic {
                    code: d.code,
                    span: Span::new(d.span.start + offset(*i), d.span.end + offset(*i)),
                    message: d.message.clone(),
                })
                .collect();
            return Err(Diagnostics::new(&bundle, diags).with_sections(sections));
        }
        let mut merged = ast::File {
            docs: Vec::new(),
            imports: Vec::new(),
            contexts: Vec::new(),
        };
        for (i, ast) in self.asts.iter().enumerate() {
            let Some(ast) = ast else { continue };
            let file = ast
                .clone()
                .shift_spans(offset(i))
                .rebase_wasm(self.files[i].dir());
            if i == 0 {
                merged.docs = file.docs;
            }
            merged.contexts.extend(file.contexts);
        }
        let mut schema =
            crate::resolve::resolve(&bundle, &merged).map_err(|d| d.with_sections(sections))?;
        if let Some(dir) = &self.root_dir {
            schema = schema.with_dir(dir.clone());
        }
        Ok(schema)
    }
}

/// An import path resolved against the importing file's root-relative
/// directory.
fn resolve_import(dir: &str, path: &str) -> Result<String, &'static str> {
    if path.is_empty() {
        return Err("import path is empty");
    }
    if path.starts_with('/') || path.starts_with('\\') || path.contains(':') {
        return Err("import path must be relative to the importing file");
    }
    if path.split(['/', '\\']).any(|seg| seg == "..") {
        return Err("import path may not contain `..`");
    }
    let path = path.replace('\\', "/");
    let mut segs: Vec<&str> = dir
        .split('/')
        .filter(|s| !s.is_empty() && *s != ".")
        .collect();
    for seg in path.split('/') {
        if seg.is_empty() || seg == "." {
            continue;
        }
        segs.push(seg);
    }
    Ok(segs.join("/"))
}

#[cfg(test)]
mod tests {
    use super::resolve_import;

    #[test]
    fn import_paths_resolve_against_the_importing_directory() {
        assert_eq!(resolve_import("", "a.fold").unwrap(), "a.fold");
        assert_eq!(resolve_import("sub", "a.fold").unwrap(), "sub/a.fold");
        assert_eq!(
            resolve_import("sub/x", "./b/a.fold").unwrap(),
            "sub/x/b/a.fold"
        );
        assert_eq!(resolve_import("", "b\\a.fold").unwrap(), "b/a.fold");
        assert!(resolve_import("", "").is_err());
        assert!(resolve_import("", "/a.fold").is_err());
        assert!(resolve_import("", "c:a.fold").is_err());
        assert!(resolve_import("sub", "../a.fold").is_err());
    }
}
