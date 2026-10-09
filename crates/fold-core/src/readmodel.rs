//! Read-model rows and projection checkpoints, stored in the index database.
//!
//! Each declared table lives in a redb table named `rm:<projection>:<table>`
//! mapping an encoded key (see [`crate::keyenc`]) to an opaque row. A
//! projection batch commits its rows and its new checkpoint in **one**
//! transaction, so a crash can never leave the checkpoint ahead of or behind
//! the rows.

use std::collections::HashMap;
use std::sync::Arc;

use redb::{ReadTransaction, TableDefinition};

use crate::error::{Error, Result};
use crate::ids::GlobalPosition;
use crate::index::{CHECKPOINTS, read_model_table_name};
use crate::log::Inner;

type RowTable<'a> = TableDefinition<'a, &'static [u8], &'static [u8]>;

/// Handle on the read-model side of a log. Cheap to clone.
#[derive(Clone)]
pub struct ReadModelStore {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for ReadModelStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReadModelStore")
    }
}

impl ReadModelStore {
    pub(crate) fn new(inner: Arc<Inner>) -> Self {
        ReadModelStore { inner }
    }

    /// A consistent view of every table and checkpoint as of now.
    pub fn snapshot(&self) -> Result<ReadModelSnapshot> {
        Ok(ReadModelSnapshot {
            txn: self.inner.index.begin_read()?,
        })
    }

    /// The next position `projection` has to process, `None` if it has never
    /// committed.
    pub fn checkpoint(&self, projection: &str) -> Result<Option<GlobalPosition>> {
        self.snapshot()?.checkpoint(projection)
    }

    /// Applies `puts` and `deletes` and sets `projection`'s checkpoint to
    /// `next_position` in one transaction. `puts` are `(table, key, row)`,
    /// `deletes` are `(table, key)`; a delete of an absent key is a no-op.
    /// Within one commit, later operations on a key win over earlier ones.
    pub fn commit(
        &self,
        projection: &str,
        next_position: GlobalPosition,
        puts: Vec<(String, Vec<u8>, Vec<u8>)>,
        deletes: Vec<(String, Vec<u8>)>,
    ) -> Result<()> {
        let txn = self.inner.index.begin_write()?;
        {
            // Table names must outlive the definitions built from them.
            let mut names: HashMap<&str, String> = HashMap::new();
            for (t, _, _) in &puts {
                names
                    .entry(t.as_str())
                    .or_insert_with(|| read_model_table_name(projection, t));
            }
            for (t, _) in &deletes {
                names
                    .entry(t.as_str())
                    .or_insert_with(|| read_model_table_name(projection, t));
            }
            let mut tables: HashMap<&str, redb::Table<'_, &[u8], &[u8]>> = HashMap::new();
            for (short, full) in &names {
                let def: RowTable<'_> = TableDefinition::new(full);
                tables.insert(short, txn.open_table(def)?);
            }
            for (t, key, row) in &puts {
                tables
                    .get_mut(t.as_str())
                    .expect("table opened above")
                    .insert(key.as_slice(), row.as_slice())?;
            }
            for (t, key) in &deletes {
                tables
                    .get_mut(t.as_str())
                    .expect("table opened above")
                    .remove(key.as_slice())?;
            }
            let mut checkpoints = txn.open_table(CHECKPOINTS)?;
            checkpoints.insert(projection, next_position.0)?;
        }
        txn.commit()?;
        Ok(())
    }
}

impl ReadModelStore {
    /// Drops every row of `projection`'s `tables` and its checkpoint in one
    /// transaction: the state before the projection ever ran. Tables that
    /// were never written are skipped.
    pub fn reset(&self, projection: &str, tables: &[&str]) -> Result<()> {
        let txn = self.inner.index.begin_write()?;
        {
            for t in tables {
                let name = read_model_table_name(projection, t);
                let def: RowTable<'_> = TableDefinition::new(&name);
                match txn.delete_table(def) {
                    Ok(_) => {}
                    Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(e) => return Err(Error::from(e)),
                }
            }
            let mut checkpoints = txn.open_table(CHECKPOINTS)?;
            checkpoints.remove(projection)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Drops `projection`'s `tables` (rows and all), keeping its checkpoint:
    /// for tables the schema no longer declares.
    pub fn drop_tables(&self, projection: &str, tables: &[&str]) -> Result<()> {
        let txn = self.inner.index.begin_write()?;
        for t in tables {
            let name = read_model_table_name(projection, t);
            let def: RowTable<'_> = TableDefinition::new(&name);
            match txn.delete_table(def) {
                Ok(_) => {}
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(e) => return Err(Error::from(e)),
            }
        }
        txn.commit()?;
        Ok(())
    }
}

/// A point-in-time view of the read models.
pub struct ReadModelSnapshot {
    txn: ReadTransaction,
}

impl std::fmt::Debug for ReadModelSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReadModelSnapshot")
    }
}

impl ReadModelSnapshot {
    fn open(
        &self,
        projection: &str,
        table: &str,
    ) -> Result<Option<redb::ReadOnlyTable<&'static [u8], &'static [u8]>>> {
        let name = read_model_table_name(projection, table);
        let def: RowTable<'_> = TableDefinition::new(&name);
        match self.txn.open_table(def) {
            Ok(t) => Ok(Some(t)),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(Error::from(e)),
        }
    }

    /// The row at `key`, if any. A table nobody has written to yet reads as
    /// empty.
    pub fn get(&self, projection: &str, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some(t) = self.open(projection, table)? else {
            return Ok(None);
        };
        Ok(t.get(key)?.map(|g| g.value().to_vec()))
    }

    /// Rows whose key starts with `prefix`, in key order, at most `limit`.
    pub fn scan(
        &self,
        projection: &str,
        table: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let Some(t) = self.open(projection, table)? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        if limit == 0 {
            return Ok(out);
        }
        for item in t.range(prefix..)? {
            let (k, v) = item?;
            let k = k.value();
            if !k.starts_with(prefix) {
                break;
            }
            out.push((k.to_vec(), v.value().to_vec()));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// `projection`'s checkpoint as of this snapshot.
    pub fn checkpoint(&self, projection: &str) -> Result<Option<GlobalPosition>> {
        let t = self.txn.open_table(CHECKPOINTS)?;
        Ok(t.get(projection)?.map(|g| GlobalPosition(g.value())))
    }
}
