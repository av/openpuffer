//! Write-ahead log entries: `openpuffer/{ns}/wal/{seq:08}.bin` (bincode + optional CRC32).
//!
//! **Wire format (v1):** `[0x01 version][bincode payload][crc32 LE]` where CRC32 is IEEE over
//! the payload only. Legacy segments are raw bincode (no version byte).

use crate::models::Document;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::OnceLock;

/// WAL segment format version: bincode payload + trailing CRC32.
pub const WAL_FORMAT_CRC32: u8 = 1;

const CRC32_TRAILER_LEN: usize = 4;

/// One committed WAL batch (upserts, attribute patches, and deletes in a single object).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WalEntry {
    pub upserts: Vec<WalUpsert>,
    #[serde(default)]
    pub patches: Vec<WalPatch>,
    pub deletes: Vec<String>,
}

/// Bincode-friendly upsert row (`attributes` stored as JSON object bytes).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WalUpsert {
    pub id: String,
    pub attributes: Vec<u8>,
}

/// Partial attribute merge for an existing document (ignored if id does not exist).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WalPatch {
    pub id: String,
    pub attributes: Vec<u8>,
}

impl WalEntry {
    pub fn from_write(
        upserts: Vec<Document>,
        patches: Vec<Document>,
        deletes: Vec<String>,
    ) -> Result<Self> {
        let upserts = upserts
            .into_iter()
            .map(document_to_upsert)
            .collect::<Result<Vec<_>>>()?;
        let patches = patches
            .into_iter()
            .map(document_to_patch)
            .collect::<Result<Vec<_>>>()?;
        Ok(WalEntry {
            upserts,
            patches,
            deletes,
        })
    }

    pub fn into_documents(self) -> Result<Vec<Document>> {
        self.upserts
            .into_iter()
            .map(wal_upsert_to_document)
            .collect()
    }

    pub fn patch_documents(&self) -> Result<Vec<Document>> {
        self.patches
            .iter()
            .map(|p| {
                let attributes: HashMap<String, serde_json::Value> =
                    serde_json::from_slice(&p.attributes).context("decode patch attributes")?;
                Ok(Document {
                    id: p.id.clone(),
                    attributes,
                })
            })
            .collect()
    }
}

fn document_to_upsert(doc: Document) -> Result<WalUpsert> {
    Ok(WalUpsert {
        id: doc.id,
        attributes: serde_json::to_vec(&doc.attributes).context("encode attributes")?,
    })
}

fn document_to_patch(doc: Document) -> Result<WalPatch> {
    Ok(WalPatch {
        id: doc.id,
        attributes: serde_json::to_vec(&doc.attributes).context("encode patch attributes")?,
    })
}

fn wal_upsert_to_document(u: WalUpsert) -> Result<Document> {
    let attributes: HashMap<String, serde_json::Value> =
        serde_json::from_slice(&u.attributes).context("decode attributes")?;
    Ok(Document {
        id: u.id,
        attributes,
    })
}

/// Shallow-merge `patch` attributes into `doc` (turbopuffer patch_rows semantics).
pub fn merge_document_attributes(doc: &mut Document, patch: &Document) {
    for (k, v) in &patch.attributes {
        doc.attributes.insert(k.clone(), v.clone());
    }
}

pub fn wal_key(namespace: &str, seq: u64) -> String {
    format!("{}{namespace}/wal/{seq:08}.bin", crate::models::ROOT_PREFIX)
}

/// Durable doc map at a WAL commit point (`wal/snapshot.bin`), written before indexed segments are deleted.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WalSnapshot {
    /// `index_cursor` / `wal_commit_seq` when the snapshot was taken.
    pub seq: u64,
    pub docs: Vec<Document>,
}

impl WalSnapshot {
    pub fn key(namespace: &str) -> String {
        format!(
            "{}{namespace}/wal/snapshot.bin",
            crate::models::ROOT_PREFIX
        )
    }

    pub fn from_docs(seq: u64, docs: &HashMap<String, Document>) -> Self {
        Self {
            seq,
            docs: docs.values().cloned().collect(),
        }
    }

    pub fn into_docs(self) -> HashMap<String, Document> {
        self.docs
            .into_iter()
            .map(|d| (d.id.clone(), d))
            .collect()
    }
}

pub fn wal_snapshot_key(namespace: &str) -> String {
    WalSnapshot::key(namespace)
}

pub fn encode_snapshot(snapshot: &WalSnapshot) -> Result<Vec<u8>> {
    serde_json::to_vec(snapshot).context("encode WalSnapshot")
}

pub fn decode_snapshot(bytes: &[u8]) -> Result<WalSnapshot> {
    serde_json::from_slice(bytes).context("decode WalSnapshot")
}

/// How to handle a WAL segment that fails CRC or payload decode during replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalCorruptPolicy {
    /// Abort namespace load / replay (default, fail-safe).
    Fail,
    /// Log and skip the corrupt segment; prior segments remain applied.
    Skip,
}

impl WalCorruptPolicy {
    /// `OPENPUFFER_WAL_CORRUPT_POLICY`: `fail` (default) | `skip`.
    pub fn from_env() -> Self {
        Self::from_env_str(
            &std::env::var("OPENPUFFER_WAL_CORRUPT_POLICY").unwrap_or_default(),
        )
    }

    pub fn from_env_str(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "skip" => Self::Skip,
            _ => Self::Fail,
        }
    }

    /// Cached policy for hot replay paths (set via [`init_corrupt_policy`] at serve).
    pub fn current() -> Self {
        *CORRUPT_POLICY.get_or_init(Self::from_env)
    }
}

static CORRUPT_POLICY: OnceLock<WalCorruptPolicy> = OnceLock::new();

/// Install WAL corrupt policy for the process (called from `serve`).
pub fn init_corrupt_policy(policy: WalCorruptPolicy) {
    let _ = CORRUPT_POLICY.set(policy);
}

/// CRC32 / payload mismatch or truncated v1 segment.
#[derive(Debug, Clone)]
pub struct WalCorrupt {
    pub seq: Option<u64>,
    pub reason: String,
}

impl std::fmt::Display for WalCorrupt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.seq {
            Some(seq) => write!(f, "wal segment {seq:08} corrupt: {}", self.reason),
            None => write!(f, "wal segment corrupt: {}", self.reason),
        }
    }
}

impl std::error::Error for WalCorrupt {}

pub fn is_corrupt_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<WalCorrupt>().is_some()
}

pub fn encode(entry: &WalEntry) -> Result<Vec<u8>> {
    let payload = bincode::serialize(entry).context("encode WalEntry payload")?;
    let crc = crc32fast::hash(&payload);
    let mut out = Vec::with_capacity(1 + payload.len() + CRC32_TRAILER_LEN);
    out.push(WAL_FORMAT_CRC32);
    out.extend_from_slice(&payload);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

/// Legacy encoding (no version byte / CRC) — for backward-compat tests only.
#[cfg(test)]
pub fn encode_legacy(entry: &WalEntry) -> Result<Vec<u8>> {
    bincode::serialize(entry).context("encode WalEntry legacy")
}

pub fn decode(bytes: &[u8]) -> Result<WalEntry> {
    decode_segment(bytes, None)
}

/// Decode one WAL object, optionally tagging corrupt errors with `seq`.
pub fn decode_segment(bytes: &[u8], seq: Option<u64>) -> Result<WalEntry> {
    if bytes.is_empty() {
        return Err(corrupt_err(seq, "empty wal segment"));
    }
    if bytes[0] == WAL_FORMAT_CRC32 && bytes.len() >= 1 + CRC32_TRAILER_LEN + 1 {
        match decode_crc32_format(bytes, seq) {
            Ok(entry) => return Ok(entry),
            Err(e) if is_corrupt_error(&e) => {
                // Legacy bincode may also start with `0x01`; only accept if deserialize works.
                if let Ok(entry) = bincode::deserialize(bytes) {
                    return Ok(entry);
                }
                return Err(e);
            }
            Err(e) => return Err(e),
        }
    }
    bincode::deserialize(bytes)
        .map_err(|e| corrupt_err(seq, format!("legacy bincode decode: {e}")))
}

fn decode_crc32_format(bytes: &[u8], seq: Option<u64>) -> Result<WalEntry> {
    if bytes.len() < 1 + CRC32_TRAILER_LEN + 1 {
        return Err(corrupt_err(
            seq,
            format!("truncated v1 wal ({} bytes)", bytes.len()),
        ));
    }
    let payload = &bytes[1..bytes.len() - CRC32_TRAILER_LEN];
    let stored_crc = u32::from_le_bytes(
        bytes[bytes.len() - CRC32_TRAILER_LEN..]
            .try_into()
            .expect("crc trailer length"),
    );
    let computed_crc = crc32fast::hash(payload);
    if stored_crc != computed_crc {
        return Err(corrupt_err(
            seq,
            format!("crc32 mismatch (stored={stored_crc:#010x}, computed={computed_crc:#010x})"),
        ));
    }
    bincode::deserialize(payload)
        .map_err(|e| corrupt_err(seq, format!("bincode payload after crc ok: {e}")))
}

fn corrupt_err(seq: Option<u64>, reason: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(WalCorrupt {
        seq,
        reason: reason.into(),
    })
}

/// Decode and apply corrupt policy (log + fail or skip).
pub fn decode_segment_with_policy(
    bytes: &[u8],
    seq: u64,
    policy: WalCorruptPolicy,
) -> Result<Option<WalEntry>> {
    match decode_segment(bytes, Some(seq)) {
        Ok(entry) => Ok(Some(entry)),
        Err(e) if is_corrupt_error(&e) => {
            tracing::error!(namespace_seq = seq, "{}", e);
            match policy {
                WalCorruptPolicy::Fail => Err(e),
                WalCorruptPolicy::Skip => Ok(None),
            }
        }
        Err(e) => Err(e),
    }
}

/// Build FTS/filter/vector `apply_delta` inputs from WAL batches against a doc map baseline.
///
/// `baseline` should reflect namespace state at `index_cursor` before the batch; each entry
/// is applied in order so patches merge into existing documents.
pub fn collect_index_delta(
    baseline: &mut HashMap<String, Document>,
    entries: &[WalEntry],
) -> Result<(Vec<(String, Document)>, Vec<String>)> {
    let mut upsert_map: HashMap<String, Document> = HashMap::new();
    let mut deletes = Vec::new();
    for entry in entries {
        deletes.extend(entry.deletes.clone());
        let upsert_ids: Vec<String> = entry.upserts.iter().map(|u| u.id.clone()).collect();
        let patch_ids: Vec<String> = entry.patches.iter().map(|p| p.id.clone()).collect();
        apply_entry(baseline, entry)?;
        for id in upsert_ids.into_iter().chain(patch_ids) {
            if let Some(doc) = baseline.get(&id) {
                upsert_map.insert(id, doc.clone());
            }
        }
    }
    Ok((upsert_map.into_iter().collect(), deletes))
}

/// Apply a WAL entry to an in-memory document map.
///
/// Order: deletes → full upserts → attribute patches (patches to missing ids are ignored).
pub fn apply_entry(docs: &mut HashMap<String, Document>, entry: &WalEntry) -> Result<()> {
    for id in &entry.deletes {
        docs.remove(id);
    }
    for doc in entry.clone().into_documents()? {
        docs.insert(doc.id.clone(), doc);
    }
    for patch in entry.patch_documents()? {
        if let Some(existing) = docs.get_mut(&patch.id) {
            merge_document_attributes(existing, &patch);
        }
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
            vec![],
            vec!["b".into()],
        )
        .unwrap();
        let bytes = encode(&entry).unwrap();
        assert_eq!(bytes[0], WAL_FORMAT_CRC32);
        assert!(bytes.len() > 5);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded.deletes, entry.deletes);
        assert_eq!(decoded.upserts[0].id, "a");
        let docs = decoded.into_documents().unwrap();
        assert_eq!(docs[0].attributes.get("text").unwrap(), &json!("hello"));
    }

    #[test]
    fn wal_legacy_roundtrip_without_checksum() {
        let entry = WalEntry::from_write(
            vec![Document {
                id: "legacy".into(),
                attributes: [("text".into(), json!("old"))].into(),
            }],
            vec![],
            vec![],
        )
        .unwrap();
        let bytes = encode_legacy(&entry).unwrap();
        // Legacy path: no CRC trailer (length differs from v1 for same entry).
        let v1 = encode(&entry).unwrap();
        assert_ne!(bytes.len(), v1.len());
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded.upserts[0].id, "legacy");
    }

    #[test]
    fn wal_corrupt_bytes_detected_crc_mismatch() {
        let entry = WalEntry::from_write(
            vec![Document {
                id: "a".into(),
                attributes: Default::default(),
            }],
            vec![],
            vec![],
        )
        .unwrap();
        let mut bytes = encode(&entry).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        let err = decode_segment(&bytes, Some(7)).unwrap_err();
        assert!(is_corrupt_error(&err));
        assert!(format!("{err:#}").contains("crc32 mismatch"));
    }

    #[test]
    fn wal_corrupt_payload_flip_detected() {
        let entry = WalEntry::from_write(
            vec![Document {
                id: "a".into(),
                attributes: Default::default(),
            }],
            vec![],
            vec![],
        )
        .unwrap();
        let mut bytes = encode(&entry).unwrap();
        if bytes.len() > 6 {
            bytes[2] ^= 0x01;
        }
        let err = decode_segment(&bytes, Some(3)).unwrap_err();
        assert!(is_corrupt_error(&err));
    }

    #[test]
    fn wal_corrupt_policy_skip_returns_none() {
        let entry = WalEntry::from_write(
            vec![Document {
                id: "a".into(),
                attributes: Default::default(),
            }],
            vec![],
            vec![],
        )
        .unwrap();
        let mut bytes = encode(&entry).unwrap();
        let tail = bytes.len() - 1;
        bytes[tail] ^= 0xAA;
        let out =
            decode_segment_with_policy(&bytes, 1, WalCorruptPolicy::Skip).expect("skip policy");
        assert!(out.is_none());
    }

    #[test]
    fn wal_key_format() {
        assert_eq!(
            wal_key("my-ns", 42),
            "openpuffer/my-ns/wal/00000042.bin"
        );
    }

    #[test]
    fn wal_snapshot_roundtrip() {
        let mut docs = HashMap::new();
        docs.insert(
            "a".into(),
            Document {
                id: "a".into(),
                attributes: [("text".into(), json!("snap"))].into(),
            },
        );
        let snap = WalSnapshot::from_docs(15, &docs);
        let bytes = encode_snapshot(&snap).unwrap();
        let back = decode_snapshot(&bytes).unwrap();
        assert_eq!(back.seq, 15);
        assert_eq!(back.into_docs().get("a").unwrap().attributes["text"], json!("snap"));
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
            vec![],
            vec!["old".into()],
        )
        .unwrap();
        apply_entry(&mut docs, &entry).unwrap();
        assert!(!docs.contains_key("old"));
        assert!(docs.contains_key("new"));
    }

    #[test]
    fn apply_entry_patch_merges_existing_ignores_missing() {
        let mut docs = HashMap::new();
        docs.insert(
            "a".into(),
            Document {
                id: "a".into(),
                attributes: [
                    ("text".into(), json!("original")),
                    ("tier".into(), json!("pro")),
                ]
                .into(),
            },
        );
        let entry = WalEntry::from_write(
            vec![],
            vec![Document {
                id: "a".into(),
                attributes: [("text".into(), json!("patched"))].into(),
            }],
            vec![],
        )
        .unwrap();
        apply_entry(&mut docs, &entry).unwrap();
        assert_eq!(docs["a"].attributes["text"], json!("patched"));
        assert_eq!(docs["a"].attributes["tier"], json!("pro"));

        let entry2 = WalEntry::from_write(
            vec![],
            vec![Document {
                id: "missing".into(),
                attributes: [("x".into(), json!(1))].into(),
            }],
            vec![],
        )
        .unwrap();
        apply_entry(&mut docs, &entry2).unwrap();
        assert!(!docs.contains_key("missing"));
    }

    #[test]
    fn apply_entry_twice_is_idempotent() {
        let mut docs = HashMap::new();
        let entry = WalEntry::from_write(
            vec![Document {
                id: "a".into(),
                attributes: [("text".into(), json!("v1"))].into(),
            }],
            vec![Document {
                id: "a".into(),
                attributes: [("tier".into(), json!("pro"))].into(),
            }],
            vec!["gone".into()],
        )
        .unwrap();
        apply_entry(&mut docs, &entry).unwrap();
        let snapshot = docs.clone();
        apply_entry(&mut docs, &entry).unwrap();
        assert_eq!(docs.len(), snapshot.len());
        assert_eq!(docs.get("a").map(|d| &d.attributes["text"]), snapshot.get("a").map(|d| &d.attributes["text"]));
        assert!(!docs.contains_key("gone"));
    }

    #[test]
    fn apply_entry_patch_after_upsert_in_same_batch() {
        let mut docs = HashMap::new();
        let entry = WalEntry::from_write(
            vec![Document {
                id: "a".into(),
                attributes: [("text".into(), json!("base"))].into(),
            }],
            vec![Document {
                id: "a".into(),
                attributes: [("tier".into(), json!("pro"))].into(),
            }],
            vec![],
        )
        .unwrap();
        apply_entry(&mut docs, &entry).unwrap();
        assert_eq!(docs["a"].attributes["text"], json!("base"));
        assert_eq!(docs["a"].attributes["tier"], json!("pro"));
    }
}