//! Startup Integrity Guard
//!
//! Validates the integrity of the source code and critical files at startup.
//! This prevents unauthorized code modification and ensures the node is running verified code.

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Integrity verification errors
#[derive(Debug, Error)]
pub enum IntegrityError {
    #[error("Manifest file not found at {0}")]
    ManifestNotFound(PathBuf),

    #[error("Hash mismatch for file {file}: expected {expected}, got {actual}")]
    HashMismatch {
        file: String,
        expected: String,
        actual: String,
    },

    #[error("IO Error: {0}")]
    IoError(#[from] io::Error),

    #[error("JSON Error: {0}")]
    JsonError(#[from] serde_json::Error),
}

/// Validates source code integrity
pub struct ChecksumValidator {
    manifest_path: PathBuf,
}

impl ChecksumValidator {
    /// Create a new validator with path to manifest.json
    pub fn new(manifest_path: PathBuf) -> Self {
        ChecksumValidator { manifest_path }
    }

    /// Calculate SHA-256 hash of a single file
    pub fn calculate_file_hash(path: &Path) -> io::Result<String> {
        let mut file = File::open(path)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0; 4096];

        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }

        let result = hasher.finalize();
        Ok(hex::encode(result))
    }

    /// Recursively calculate hashes for all .rs files in a directory
    pub fn calculate_directory_hashes(dir: &Path) -> io::Result<HashMap<String, String>> {
        let mut hashes = HashMap::new();

        if dir.is_dir() {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();

                if path.is_dir() {
                    let sub_hashes = Self::calculate_directory_hashes(&path)?;
                    hashes.extend(sub_hashes);
                } else if let Some(ext) = path.extension() {
                    if ext == "rs" {
                        let hash = Self::calculate_file_hash(&path)?;
                        let key = path.to_string_lossy().to_string();
                        hashes.insert(key, hash);
                    }
                }
            }
        }

        Ok(hashes)
    }

    /// Verify current directory against the manifest
    ///
    /// The manifest is expected to be a JSON map: { "path/to/file.rs": "hash_hex" }
    pub fn verify(&self, _source_dir: &Path) -> Result<(), IntegrityError> {
        if !self.manifest_path.exists() {
            return Err(IntegrityError::ManifestNotFound(self.manifest_path.clone()));
        }

        let manifest_content = fs::read_to_string(&self.manifest_path)?;
        let manifest: HashMap<String, String> = serde_json::from_str(&manifest_content)?;

        for (file_path_str, expected_hash) in manifest {
            let path = Path::new(&file_path_str);

            if !path.exists() {
                // If file is missing, treating as mismatch/integrity failure is appropriate
                return Err(IntegrityError::HashMismatch {
                    file: file_path_str,
                    expected: expected_hash,
                    actual: "FILE_MISSING".to_string(),
                });
            }

            let actual_hash = Self::calculate_file_hash(path)?;

            if actual_hash != expected_hash {
                return Err(IntegrityError::HashMismatch {
                    file: file_path_str,
                    expected: expected_hash,
                    actual: actual_hash,
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_calculate_hash() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.rs");
        let mut file = File::create(&file_path).unwrap();
        write!(file, "fn main() {{}}").unwrap();

        let hash = ChecksumValidator::calculate_file_hash(&file_path).unwrap();
        assert_eq!(hash.len(), 64); // SHA-256 hex string length
    }

    #[test]
    fn test_verify_success() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("lib.rs");
        {
            let mut file = File::create(&file_path).unwrap();
            write!(file, "pub fn add(a: i32, b: i32) -> i32 {{ a + b }}").unwrap();
        }

        let hash = ChecksumValidator::calculate_file_hash(&file_path).unwrap();

        let manifest_path = dir.path().join("manifest.json");
        let manifest = json!({
            file_path.to_string_lossy().to_string(): hash
        });

        {
            let mut file = File::create(&manifest_path).unwrap();
            write!(file, "{}", manifest.to_string()).unwrap();
        }

        let validator = ChecksumValidator::new(manifest_path);
        let result = validator.verify(dir.path());
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_mismatch() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("broken.rs");
        {
            let mut file = File::create(&file_path).unwrap();
            write!(file, "original content").unwrap();
        }

        let manifest_path = dir.path().join("manifest.json");
        // Incorrect hash in manifest
        let manifest = json!({
            file_path.to_string_lossy().to_string(): "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        });

        {
            let mut file = File::create(&manifest_path).unwrap();
            write!(file, "{}", manifest.to_string()).unwrap();
        }

        let validator = ChecksumValidator::new(manifest_path);
        let result = validator.verify(dir.path());

        match result {
            Err(IntegrityError::HashMismatch { .. }) => assert!(true),
            _ => assert!(false, "Should have failed with HashMismatch"),
        }
    }
}
