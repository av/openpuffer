//! Write-ahead log entries: `openpuffer/{ns}/wal/{seq:08}.bin` (bincode).

use crate::models::Document;
use anyhow::{Context, Result};
use std::collections::HashMap;

/// One committed WAL batch (upserts and deletes in a single object).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WalEntry {
    pub upserts: Vec<WalUpsert>,
    pub deletes: Vec<String>,
}

/// Bincode-friendly upsert row (`attributes` stored as JSON object bytes).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WalUpsert {
    pub id: String,
    pub attributes: Vec<u8>,
}

impl WalEntry {
    pub fn from_write(upserts: Vec<Document>, deletes: Vec<String>) -> Result<Self> {
        let upserts = upserts
            .into_iter()
            .map(|doc| {
                Ok(WalUpsert {
                    id: doc.id,
                    attributes: serde_json::to_vec(&doc.attributes).context("encode attributes")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(WalEntry { upserts, deletes })
    }

    pub fn into_documents(self) -> Result<Vec<Document>> {
        self.upserts
            .into_iter()
            .map(|u| {
                let attributes: HashMap<String, serde_json::Value> =
                    serde_json::from_slice(&u.attributes).context("decode attributes")?;
                Ok(Document {
                    id: u.id,
                    attributes,
                })
            })
            .collect()
    }
}

pub fn wal_key(namespace: &str, seq: u64) -> String {
    format!("{}{namespace}/wal/{seq:08}.bin", crate::models::ROOT_PREFIX)
}

pub fn encode(entry: &WalEntry) -> Result<Vec<u8>> {
    bincode::serialize(entry).context("encode WalEntry")
}

pub fn decode(bytes: &[u8]) -> Result<WalEntry> {
    bincode::deserialize(bytes).context("decode WalEntry")
}

/// Apply a WAL entry to an in-memory document map (last write wins per id).
pub fn apply_entry(docs: &mut HashMap<String, Document>, entry: &WalEntry) -> Result<()> {
    for id in &entry.deletes {
        docs.remove(id);
    }
    for doc in entry.clone().into_documents()? {
        docs.insert(doc.id.clone(), doc);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Document;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn wal_roundtrip_serialize() {
        let entry = WalEntry::from_write(
            vec![Document {
                id: "a".into(),
                attributes: [("text".into(), json!("hello"))].into(),
            }],
            vec!["b".into()],
        )
        .unwrap();
        let bytes = encode(&entry).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded.deletes, entry.deletes);
        assert_eq!(decoded.upserts[0].id, "a");
        let docs = decoded.into_documents().unwrap();
        assert_eq!(docs[0].attributes.get("text").unwrap(), &json!("hello"));
    }

    #[test]
    fn wal_key_format() {
        assert_eq!(
            wal_key("my-ns", 42),
            "openpuffer/my-ns/wal/00000042.bin"
        );
    }

    #[test]
    fn apply_entry_upsert_and_delete() {
        let mut docs = HashMap::new();
        docs.insert(
            "old".into(),
            Document {
                id: "old".into(),
                attributes: Default::default(),
            },
        );
        let entry = WalEntry::from_write(
            vec![Document {
                id: "new".into(),
                attributes: Default::default(),
            }],
            vec!["old".into()],
        )
        .unwrap();
        apply_entry(&mut docs, &entry).unwrap();
        assert!(!docs.contains_key("old"));
        assert!(docs.contains_key("new"));
    }
}