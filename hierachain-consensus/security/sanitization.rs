//! Input Validation and Sanitization
//!
//! Provides utilities for validating and sanitizing inputs to prevent
//! injection attacks, ensure data integrity, and enforce business rules.

use regex::Regex;
use std::fmt::Display;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Validation specific errors
#[derive(Debug, Error, PartialEq)]
pub enum ValidationError {
    #[error("Invalid format: {0}")]
    InvalidFormat(String),
    #[error("Value out of bounds: {0}")]
    OutOfBounds(String),
    #[error("Input too long: length {0}, max {1}")]
    TooLong(usize, usize),
    #[error("Timestamp error: {0}")]
    TimestampError(String),
}

/// Validation and Sanitization Utility
pub struct ValidationSanitizer;

impl ValidationSanitizer {
    /// Validate string against custom regex pattern and length limit
    ///
    /// # Arguments
    /// * `input` - The string to validate
    /// * `pattern` - Regex pattern string
    /// * `max_len` - Maximum allowed length
    pub fn validate_string(
        input: &str,
        pattern: &str,
        max_len: usize,
    ) -> Result<(), ValidationError> {
        if input.len() > max_len {
            return Err(ValidationError::TooLong(input.len(), max_len));
        }

        let re = Regex::new(pattern).map_err(|e| ValidationError::InvalidFormat(e.to_string()))?;

        if !re.is_match(input) {
            return Err(ValidationError::InvalidFormat(format!(
                "Input does not match pattern: {}",
                pattern
            )));
        }

        Ok(())
    }

    /// Sanitize input by removing dangerous characters
    ///
    /// Removes commonly dangerous characters for injection attacks: < > " ' ; %
    pub fn sanitize_input(input: &str) -> String {
        input
            .replace('<', "")
            .replace('>', "")
            .replace('"', "")
            .replace('\'', "")
            .replace(';', "")
            .replace('%', "")
    }

    /// Validate numeric bounds
    pub fn validate_numeric_bounds<T: PartialOrd + Display>(
        val: T,
        min: T,
        max: T,
    ) -> Result<(), ValidationError> {
        if val < min || val > max {
            return Err(ValidationError::OutOfBounds(format!(
                "Value {} is not in range [{}, {}]",
                val, min, max
            )));
        }
        Ok(())
    }

    /// Validate timestamp drift
    ///
    /// Ensures timestamp is not too far in the past or future.
    ///
    /// # Arguments
    /// * `ts` - Timestamp to validate (seconds since epoch)
    /// * `max_drift` - Maximum allowed drift in seconds
    pub fn validate_timestamp(ts: u64, max_drift: u64) -> Result<(), ValidationError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| ValidationError::TimestampError(e.to_string()))?
            .as_secs();

        // Check bounds (now - drift <= ts <= now + drift)
        // Check lower bound (future drift check requires avoiding underflow)
        if ts > now + max_drift {
            return Err(ValidationError::TimestampError(format!(
                "Timestamp too far in future: {} > {}",
                ts,
                now + max_drift
            )));
        }

        // Check upper bound (past drift check)
        if ts < now.saturating_sub(max_drift) {
            return Err(ValidationError::TimestampError(format!(
                "Timestamp too old: {} < {}",
                ts,
                now.saturating_sub(max_drift)
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_string() {
        let pattern = r"^[a-zA-Z0-9_]+$";

        // Valid
        assert!(ValidationSanitizer::validate_string("valid_input_1", pattern, 20).is_ok());

        // Too long
        assert!(matches!(
            ValidationSanitizer::validate_string("this_is_way_too_long_for_limit", pattern, 10),
            Err(ValidationError::TooLong(_, _))
        ));

        // Invalid format
        assert!(matches!(
            ValidationSanitizer::validate_string("invalid-char!", pattern, 20),
            Err(ValidationError::InvalidFormat(_))
        ));
    }

    #[test]
    fn test_sanitize_input() {
        let dangerous = "script<alert('xss')>; DROP TABLE users;";
        let clean = ValidationSanitizer::sanitize_input(dangerous);

        assert!(!clean.contains('<'));
        assert!(!clean.contains('>'));
        assert!(!clean.contains(';'));
        assert!(!clean.contains('\''));

        // Verify basic structure remains
        assert!(clean.contains("script"));
        assert!(clean.contains("alert(xss)"));
    }

    #[test]
    fn test_validate_numeric_bounds() {
        assert!(ValidationSanitizer::validate_numeric_bounds(10, 0, 100).is_ok());
        assert!(matches!(
            ValidationSanitizer::validate_numeric_bounds(150, 0, 100),
            Err(ValidationError::OutOfBounds(_))
        ));
        assert!(matches!(
            ValidationSanitizer::validate_numeric_bounds(-1, 0, 100),
            Err(ValidationError::OutOfBounds(_))
        ));
    }

    #[test]
    fn test_validate_timestamp() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Valid (now)
        assert!(ValidationSanitizer::validate_timestamp(now, 60).is_ok());

        // Future (too far)
        assert!(matches!(
            ValidationSanitizer::validate_timestamp(now + 1000, 60),
            Err(ValidationError::TimestampError(_))
        ));

        // Past (too old)
        assert!(matches!(
            ValidationSanitizer::validate_timestamp(now - 1000, 60),
            Err(ValidationError::TimestampError(_))
        ));
    }
}
