//! FFI (Foreign Function Interface) exports for CGO integration.
//!
//! This module provides C-compatible functions that can be called from Go via CGO.
//! All functions use C strings and raw pointers for cross-language compatibility.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::slice;

use crate::core::utils::{generate_hash, MerkleTree};

/// Error codes for FFI functions
pub const FFI_SUCCESS: i32 = 0;
pub const FFI_ERROR_NULL_POINTER: i32 = -1;
pub const FFI_ERROR_INVALID_UTF8: i32 = -2;
pub const FFI_ERROR_JSON_PARSE: i32 = -3;
pub const FFI_ERROR_BUFFER_TOO_SMALL: i32 = -4;
pub const FFI_ERROR_INTERNAL: i32 = -5;

/// Helper to safely convert C string to Rust string
unsafe fn c_str_to_string(ptr: *const c_char) -> Result<String, i32> {
    if ptr.is_null() {
        return Err(FFI_ERROR_NULL_POINTER);
    }
    CStr::from_ptr(ptr)
        .to_str()
        .map(|s| s.to_string())
        .map_err(|_| FFI_ERROR_INVALID_UTF8)
}

/// Helper to write result string to C buffer
unsafe fn write_result(result: &str, out_buf: *mut c_char, buf_len: usize) -> i32 {
    if out_buf.is_null() {
        return FFI_ERROR_NULL_POINTER;
    }

    let bytes = result.as_bytes();
    if bytes.len() >= buf_len {
        return FFI_ERROR_BUFFER_TOO_SMALL;
    }

    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf as *mut u8, bytes.len());
    *out_buf.add(bytes.len()) = 0; // Null terminator

    FFI_SUCCESS
}

/// Calculate Merkle root from JSON array of events.
///
/// # Arguments
/// * `events_json` - JSON string containing array of events
/// * `result` - Output buffer for the merkle root hash
/// * `result_len` - Size of the output buffer
///
/// # Returns
/// * 0 on success, negative error code on failure
///
/// # Safety
/// - `events_json` must be a valid, null-terminated C string
/// - `result` must point to a buffer of at least `result_len` bytes
/// - `result_len` must be at least 65 bytes to hold SHA256 hex + null terminator
#[no_mangle]
pub unsafe extern "C" fn ffi_calculate_merkle_root(
    events_json: *const c_char,
    result: *mut c_char,
    result_len: usize,
) -> i32 {
    // Parse input
    let json_str = match c_str_to_string(events_json) {
        Ok(s) => s,
        Err(e) => return e,
    };

    // Parse JSON array
    let events: Vec<serde_json::Value> = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(_) => return FFI_ERROR_JSON_PARSE,
    };

    // Generate leaves (hashes of each event)
    let leaves: Vec<String> = events.iter().map(generate_hash).collect();

    // Build Merkle tree
    let tree = MerkleTree::from_leaves(leaves);
    let root = tree.get_root();

    // Write result
    write_result(&root, result, result_len)
}

/// Calculate block hash from JSON block data.
///
/// # Arguments
/// * `block_json` - JSON string containing block data
/// * `result` - Output buffer for the block hash
/// * `result_len` - Size of the output buffer
///
/// # Returns
/// * 0 on success, negative error code on failure
///
/// # Safety
/// - `block_json` must be a valid, null-terminated C string
/// - `result` must point to a buffer of at least `result_len` bytes
/// - `result_len` must be at least 65 bytes to hold SHA256 hex + null terminator
#[no_mangle]
pub unsafe extern "C" fn ffi_calculate_block_hash(
    block_json: *const c_char,
    result: *mut c_char,
    result_len: usize,
) -> i32 {
    // Parse input
    let json_str = match c_str_to_string(block_json) {
        Ok(s) => s,
        Err(e) => return e,
    };

    // Parse JSON
    let block_data: serde_json::Value = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(_) => return FFI_ERROR_JSON_PARSE,
    };

    // Generate hash
    let hash = generate_hash(&block_data);

    // Write result
    write_result(&hash, result, result_len)
}

/// Validate a batch of transactions.
///
/// # Arguments
/// * `transactions_json` - JSON string containing array of transactions
///
/// # Returns
/// * 1 if all valid, 0 if any invalid, negative error code on failure
///
/// # Safety
/// - `transactions_json` must be a valid, null-terminated C string containing valid JSON
#[no_mangle]
pub unsafe extern "C" fn ffi_bulk_validate_transactions(transactions_json: *const c_char) -> i32 {
    // Parse input
    let json_str = match c_str_to_string(transactions_json) {
        Ok(s) => s,
        Err(e) => return e,
    };

    // Parse JSON array
    let transactions: Vec<serde_json::Value> = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(_) => return FFI_ERROR_JSON_PARSE,
    };

    // Validate each transaction has required fields
    for tx in &transactions {
        if !tx.is_object() {
            return 0;
        }
        let obj = tx.as_object().unwrap();

        // Check required fields
        if !obj.contains_key("entity_id") || !obj.contains_key("event") {
            return 0;
        }
    }

    1 // All valid
}

/// Process Arrow IPC batch data.
///
/// # Arguments
/// * `arrow_ipc` - Raw Arrow IPC bytes
/// * `arrow_ipc_len` - Length of input bytes
/// * `result` - Output buffer for processed Arrow IPC
/// * `result_capacity` - Capacity of output buffer
/// * `result_len` - Output: actual length written
///
/// # Returns
/// * 0 on success, negative error code on failure
///
/// # Safety
/// - `arrow_ipc` must point to a valid buffer of at least `arrow_ipc_len` bytes
/// - `result` must point to a buffer of at least `result_capacity` bytes
/// - `result_len` must be a valid pointer to write the output length
/// - None of the pointers may be null
#[no_mangle]
pub unsafe extern "C" fn ffi_process_arrow_batch(
    arrow_ipc: *const u8,
    arrow_ipc_len: usize,
    result: *mut u8,
    result_capacity: usize,
    result_len: *mut usize,
) -> i32 {
    if arrow_ipc.is_null() || result.is_null() || result_len.is_null() {
        return FFI_ERROR_NULL_POINTER;
    }

    // Read input bytes
    let input_bytes = slice::from_raw_parts(arrow_ipc, arrow_ipc_len);

    // For now, just pass through (can add validation/processing later)
    // In production, this would deserialize, validate, and re-serialize

    if input_bytes.len() > result_capacity {
        return FFI_ERROR_BUFFER_TOO_SMALL;
    }

    std::ptr::copy_nonoverlapping(input_bytes.as_ptr(), result, input_bytes.len());
    *result_len = input_bytes.len();

    FFI_SUCCESS
}

/// Get the library version string.
///
/// # Arguments
/// * `result` - Output buffer for version string
/// * `result_len` - Size of output buffer
///
/// # Returns
/// * 0 on success, negative error code on failure
///
/// # Safety
/// - `result` must point to a buffer of at least `result_len` bytes
/// - `result_len` should be at least 32 bytes to safely hold version strings
#[no_mangle]
pub unsafe extern "C" fn ffi_get_version(result: *mut c_char, result_len: usize) -> i32 {
    let version = env!("CARGO_PKG_VERSION");
    write_result(version, result, result_len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_merkle_root() {
        let events_json =
            r#"[{"entity_id": "e1", "event": "test"}, {"entity_id": "e2", "event": "test"}]"#;
        let c_json = CString::new(events_json).unwrap();
        let mut result = vec![0u8; 128];

        unsafe {
            let code = ffi_calculate_merkle_root(
                c_json.as_ptr(),
                result.as_mut_ptr() as *mut c_char,
                result.len(),
            );
            assert_eq!(code, FFI_SUCCESS);

            let result_str = CStr::from_ptr(result.as_ptr() as *const c_char)
                .to_str()
                .unwrap();
            assert!(!result_str.is_empty());
            assert_eq!(result_str.len(), 64); // SHA256 hex
        }
    }

    #[test]
    fn test_block_hash() {
        let block_json = r#"{"index": 1, "previous_hash": "abc", "merkle_root": "def"}"#;
        let c_json = CString::new(block_json).unwrap();
        let mut result = vec![0u8; 128];

        unsafe {
            let code = ffi_calculate_block_hash(
                c_json.as_ptr(),
                result.as_mut_ptr() as *mut c_char,
                result.len(),
            );
            assert_eq!(code, FFI_SUCCESS);
        }
    }

    #[test]
    fn test_validate_transactions() {
        let valid_json = r#"[{"entity_id": "e1", "event": "test"}]"#;
        let c_json = CString::new(valid_json).unwrap();

        unsafe {
            let result = ffi_bulk_validate_transactions(c_json.as_ptr());
            assert_eq!(result, 1);
        }

        let invalid_json = r#"[{"entity_id": "e1"}]"#; // Missing "event"
        let c_json = CString::new(invalid_json).unwrap();

        unsafe {
            let result = ffi_bulk_validate_transactions(c_json.as_ptr());
            assert_eq!(result, 0);
        }
    }
}
