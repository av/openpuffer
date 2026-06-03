//! Unicode-aware FTS tokenization for BM25 indexing and queries.
//!
//! Tokenization is applied at index-build time (`FtsSegment::build` / `apply_delta`) and
//! again at query time. Existing S3 segments keep their prior term keys until those
//! documents are updated and written into a newer segment generation.

use std::collections::HashSet;
use std::sync::OnceLock;
use unicode_normalization::UnicodeNormalization;

/// Runtime options (env `OPENPUFFER_FTS_STEM`, default off).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FtsTokenizeOptions {
    /// Porter stemming for English tokens (off by default).
    pub stem: bool,
}

impl Default for FtsTokenizeOptions {
    fn default() -> Self {
        Self { stem: false }
    }
}

impl FtsTokenizeOptions {
    /// Read `OPENPUFFER_FTS_STEM` once (`1` / `true` / `yes` enables stemming).
    pub fn from_env() -> Self {
        let stem = std::env::var("OPENPUFFER_FTS_STEM")
            .ok()
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);
        Self { stem }
    }
}

static DEFAULT_OPTS: OnceLock<FtsTokenizeOptions> = OnceLock::new();
static STOPWORDS: OnceLock<HashSet<&'static str>> = OnceLock::new();

/// Minimal English stopword list (high-frequency function words only).
const STOPWORD_LIST: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "been", "being", "but", "by", "can", "did", "do",
    "does", "for", "from", "had", "has", "have", "he", "her", "him", "his", "if", "in", "into",
    "is", "it", "its", "may", "might", "must", "no", "nor", "not", "of", "on", "or", "our",
    "shall", "she", "should", "so", "such", "than", "that", "the", "their", "them", "then",
    "there", "these", "they", "this", "those", "to", "too", "was", "we", "were", "what", "when",
    "where", "which", "while", "who", "whom", "why", "will", "with", "would", "you", "your",
];

fn stopword_set() -> &'static HashSet<&'static str> {
    STOPWORDS.get_or_init(|| STOPWORD_LIST.iter().copied().collect())
}

fn default_opts() -> &'static FtsTokenizeOptions {
    DEFAULT_OPTS.get_or_init(FtsTokenizeOptions::from_env)
}

/// Tokenize with process-default options (env-driven, cached).
pub fn tokenize(text: &str) -> Vec<String> {
    tokenize_with_options(text, default_opts())
}

/// Tokenize with explicit options (used in unit tests).
pub fn tokenize_with_options(text: &str, opts: &FtsTokenizeOptions) -> Vec<String> {
    let normalized: String = text.nfkc().collect();
    let stops = stopword_set();
    let mut tokens = Vec::new();
    let mut cur = String::new();

    for ch in normalized.chars() {
        if is_token_char(ch) {
            cur.push(ch);
        } else if !cur.is_empty() {
            push_token(&mut tokens, &cur, opts, stops);
            cur.clear();
        }
    }
    if !cur.is_empty() {
        push_token(&mut tokens, &cur, opts, stops);
    }
    tokens
}

/// Unicode letter or decimal digit (NFKC-normalized input).
#[inline]
fn is_token_char(ch: char) -> bool {
    ch.is_alphabetic() || ch.is_numeric()
}

fn push_token(
    out: &mut Vec<String>,
    raw: &str,
    opts: &FtsTokenizeOptions,
    stops: &HashSet<&'static str>,
) {
    let mut term = raw.to_lowercase();
    if term.is_empty() || stops.contains(term.as_str()) {
        return;
    }
    if opts.stem {
        term = stem_english(&term);
        if term.is_empty() || stops.contains(term.as_str()) {
            return;
        }
    }
    out.push(term);
}

/// Porter stemmer (English); only used when `opts.stem` is true.
fn stem_english(token: &str) -> String {
    static STEMMER: OnceLock<rust_stemmers::Stemmer> = OnceLock::new();
    let stemmer =
        STEMMER.get_or_init(|| rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English));
    stemmer.stem(token).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_nfkc_and_case_fold() {
        // NFKC: fullwidth + compatibility forms; case fold on query side too.
        let opts = FtsTokenizeOptions::default();
        assert_eq!(
            tokenize_with_options("Ｒｕｓｔ", &opts),
            vec!["rust".to_string()]
        );
        assert_eq!(
            tokenize_with_options("Rust Programming", &opts),
            vec!["rust".to_string(), "programming".to_string()]
        );
    }

    #[test]
    fn query_rust_matches_title_case_document_terms() {
        let opts = FtsTokenizeOptions::default();
        assert_eq!(tokenize_with_options("rust", &opts), vec!["rust"]);
        assert_eq!(
            tokenize_with_options("Rust Programming", &opts),
            vec!["rust", "programming"]
        );
    }

    #[test]
    fn splits_punctuation_and_hyphens() {
        let opts = FtsTokenizeOptions::default();
        assert_eq!(
            tokenize_with_options("Hello, Rust-world!", &opts),
            vec!["hello", "rust", "world"]
        );
    }

    #[test]
    fn stopwords_removed() {
        let opts = FtsTokenizeOptions::default();
        assert_eq!(
            tokenize_with_options("the quick brown fox", &opts),
            vec!["quick", "brown", "fox"]
        );
    }

    #[test]
    fn stemming_optional_reduces_suffixes() {
        let plain = FtsTokenizeOptions { stem: false };
        let stem = FtsTokenizeOptions { stem: true };
        assert_eq!(
            tokenize_with_options("running programs", &plain),
            vec!["running", "programs"]
        );
        let stemmed = tokenize_with_options("running programs", &stem);
        assert!(stemmed.contains(&"run".to_string()));
        assert!(stemmed.contains(&"program".to_string()));
    }
}