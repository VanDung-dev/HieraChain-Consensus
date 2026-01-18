//! Error Classifier
//!
//! Classifies errors based on patterns and calculates priority using risk matrix.

use crate::error_mitigation::types::{
    ErrorCategory,
    ImpactLevel,
    LikelihoodLevel,
    PriorityLevel
};
use regex::Regex;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

/// Maximum number of unique errors to track to prevent DoS
const MAX_ERROR_HISTORY: usize = 1000;

/// Static patterns to avoid recompilation
static PATTERNS: OnceLock<Vec<ClassificationPattern>> = OnceLock::new();

/// Error information with classification
#[derive(Debug, Clone)]
pub struct ErrorInfo {
    /// Original error message
    pub message: String,
    /// Error category
    pub category: ErrorCategory,
    /// Priority level
    pub priority: PriorityLevel,
    /// Impact level
    pub impact: ImpactLevel,
    /// Likelihood level
    pub likelihood: LikelihoodLevel,
    /// Suggested mitigation strategy
    pub mitigation: String,
}

/// Risk Priority Matrix for calculating priority from impact and likelihood
pub struct RiskPriorityMatrix {
    /// Matrix: [impact][likelihood] -> priority
    matrix: [[PriorityLevel; 5]; 5],
}

impl RiskPriorityMatrix {
    /// Create a new risk matrix with default mappings
    pub fn new() -> Self {
        use PriorityLevel::*;

        // Matrix: rows = impact (Catastrophic->Negligible), cols = likelihood (AlmostCertain->Rare)
        let matrix = [
            // Catastrophic
            [Critical, Critical, Critical, High, High],
            // Major
            [Critical, Critical, High, High, Medium],
            // Moderate
            [Critical, High, High, Medium, Medium],
            // Minor
            [High, High, Medium, Medium, Low],
            // Negligible
            [High, Medium, Medium, Low, Low],
        ];

        Self { matrix }
    }

    /// Get priority for given impact and likelihood
    pub fn get_priority(&self, impact: ImpactLevel, likelihood: LikelihoodLevel) -> PriorityLevel {
        let impact_idx = 5 - impact.value() as usize;
        let likelihood_idx = 5 - likelihood.value() as usize;

        self.matrix
            .get(impact_idx.min(4))
            .and_then(|row| row.get(likelihood_idx.min(4)))
            .copied()
            .unwrap_or(PriorityLevel::Medium)
    }
}

impl Default for RiskPriorityMatrix {
    fn default() -> Self {
        Self::new()
    }
}

/// Error classification patterns
struct ClassificationPattern {
    pattern: Regex,
    category: ErrorCategory,
    impact: ImpactLevel,
    likelihood: LikelihoodLevel,
    mitigation: String,
}

/// Error classifier for categorizing and prioritizing errors
pub struct ErrorClassifier {
    /// Risk matrix for priority calculation
    risk_matrix: RiskPriorityMatrix,
    /// Error history for tracking recurring issues
    error_counts: HashMap<String, u32>,
    /// Callback for critical errors (Lockdown)
    lockdown_callback: Option<Arc<Box<dyn Fn(&str) + Send + Sync>>>,
}

impl ErrorClassifier {
    /// Create a new error classifier with default patterns
    pub fn new() -> Self {
        // Initialize patterns once
        PATTERNS.get_or_init(|| {
            vec![
                // Consensus errors
                ClassificationPattern {
                    pattern: Regex::new(r"(?i)consensus|quorum|vote|agreement").unwrap(),
                    category: ErrorCategory::Consensus,
                    impact: ImpactLevel::Major,
                    likelihood: LikelihoodLevel::Possible,
                    mitigation: "Initiate view change or request re-sync".to_string(),
                },
                ClassificationPattern {
                    pattern: Regex::new(r"(?i)byzantine|malicious|invalid.*signature").unwrap(),
                    category: ErrorCategory::Consensus,
                    impact: ImpactLevel::Catastrophic,
                    likelihood: LikelihoodLevel::Unlikely,
                    mitigation: "Quarantine node and alert administrators".to_string(),
                },
                // Network errors
                ClassificationPattern {
                    pattern: Regex::new(r"(?i)timeout|connection.*refused|network.*unreachable")
                        .unwrap(),
                    category: ErrorCategory::Network,
                    impact: ImpactLevel::Moderate,
                    likelihood: LikelihoodLevel::Likely,
                    mitigation: "Retry with exponential backoff".to_string(),
                },
                ClassificationPattern {
                    pattern: Regex::new(r"(?i)peer.*disconnect|node.*down|unreachable").unwrap(),
                    category: ErrorCategory::Network,
                    impact: ImpactLevel::Moderate,
                    likelihood: LikelihoodLevel::Possible,
                    mitigation: "Attempt reconnection or use alternative peer".to_string(),
                },
                // Cryptographic errors
                ClassificationPattern {
                    pattern: Regex::new(r"(?i)signature.*invalid|verification.*failed|crypto").unwrap(),
                    category: ErrorCategory::Cryptographic,
                    impact: ImpactLevel::Major,
                    likelihood: LikelihoodLevel::Unlikely,
                    mitigation: "Reject message and log incident".to_string(),
                },
                ClassificationPattern {
                    pattern: Regex::new(r"(?i)key.*not.*found|missing.*key|keypair").unwrap(),
                    category: ErrorCategory::Cryptographic,
                    impact: ImpactLevel::Major,
                    likelihood: LikelihoodLevel::Rare,
                    mitigation: "Check key configuration and regenerate if needed".to_string(),
                },
                // Validation errors
                ClassificationPattern {
                    pattern: Regex::new(r"(?i)invalid.*message|malformed|parse.*error").unwrap(),
                    category: ErrorCategory::Validation,
                    impact: ImpactLevel::Minor,
                    likelihood: LikelihoodLevel::Possible,
                    mitigation: "Log and discard invalid message".to_string(),
                },
                // Resource errors
                ClassificationPattern {
                    pattern: Regex::new(r"(?i)out.*of.*memory|disk.*full|resource.*exhausted").unwrap(),
                    category: ErrorCategory::Resource,
                    impact: ImpactLevel::Major,
                    likelihood: LikelihoodLevel::Unlikely,
                    mitigation: "Free resources or scale infrastructure".to_string(),
                },
            ]
        });

        Self {
            risk_matrix: RiskPriorityMatrix::new(),
            error_counts: HashMap::new(),
            lockdown_callback: None,
        }
    }

    /// Set callback for critical errors requiring lockdown
    pub fn set_lockdown_callback<F>(&mut self, callback: F)
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.lockdown_callback = Some(Arc::new(Box::new(callback)));
    }

    /// Sanitize error message to remove sensitive info
    fn sanitize_message(message: &str) -> String {
        // Regex to mask Hex strings (keys, hashes) > 20 chars
        // Using lazy_static equivalent logic here for performance if possible, but for now simple replacement
        let re_hex = Regex::new(r"(0x)?[a-fA-F0-9]{20,}").unwrap();
        let re_ip = Regex::new(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b").unwrap();

        let masked = re_hex.replace_all(message, "[REDACTED_HEX]");
        let masked = re_ip.replace_all(&masked, "[REDACTED_IP]");
        masked.to_string()
    }

    /// Classify an error message
    pub fn classify(&mut self, error_message: &str) -> ErrorInfo {
        // Sanitize first
        let clean_message = Self::sanitize_message(error_message);

        // Get shared patterns
        let patterns = PATTERNS.get().unwrap();

        // Find matching pattern
        let (category, impact, likelihood, mitigation) = patterns
            .iter()
            .find(|p| p.pattern.is_match(&clean_message))
            .map(|p| (p.category, p.impact, p.likelihood, p.mitigation.clone()))
            .unwrap_or((
                ErrorCategory::Unknown,
                ImpactLevel::Moderate,
                LikelihoodLevel::Possible,
                "Investigate and handle manually".to_string(),
            ));

        // Track error occurrence with limit check
        if self.error_counts.len() >= MAX_ERROR_HISTORY
            && !self.error_counts.contains_key(&clean_message)
        {
            if self.error_counts.len() >= MAX_ERROR_HISTORY {
                self.error_counts.clear(); // Drastic but safe against DoS
            }
        }

        let count = self.error_counts.entry(clean_message.clone()).or_insert(0);
        *count += 1;

        // Adjust likelihood based on recurrence
        let adjusted_likelihood = if *count > 5 {
            LikelihoodLevel::AlmostCertain
        } else if *count > 3 {
            LikelihoodLevel::Likely
        } else {
            likelihood
        };

        // Calculate priority
        let priority = self.risk_matrix.get_priority(impact, adjusted_likelihood);

        // Lockdown Trigger Check
        if priority == PriorityLevel::Critical {
            if let Some(callback) = &self.lockdown_callback {
                // Determine reason
                let reason = format!("CRITICAL ERROR: {} [{}]", clean_message, category.as_str());
                callback(&reason);
            }
        }

        ErrorInfo {
            message: clean_message,
            category,
            priority,
            impact,
            likelihood: adjusted_likelihood,
            mitigation,
        }
    }

    /// Get error count for a specific message
    pub fn get_error_count(&self, error_message: &str) -> u32 {
        let clean_message = Self::sanitize_message(error_message);
        self.error_counts.get(&clean_message).copied().unwrap_or(0)
    }

    /// Clear error history
    pub fn clear_history(&mut self) {
        self.error_counts.clear();
    }

    /// Get summary of all tracked errors
    pub fn get_summary(&self) -> HashMap<ErrorCategory, u32> {
        let mut summary: HashMap<ErrorCategory, u32> = HashMap::new();
        let patterns = PATTERNS.get().unwrap();

        for (msg, count) in &self.error_counts {
            let category = patterns
                .iter()
                .find(|p| p.pattern.is_match(msg))
                .map(|p| p.category)
                .unwrap_or(ErrorCategory::Unknown);

            *summary.entry(category).or_insert(0) += count;
        }

        summary
    }
}

impl Default for ErrorClassifier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_risk_matrix() {
        let matrix = RiskPriorityMatrix::new();

        // Catastrophic + Almost Certain = Critical
        assert_eq!(
            matrix.get_priority(ImpactLevel::Catastrophic, LikelihoodLevel::AlmostCertain),
            PriorityLevel::Critical
        );

        // Negligible + Rare = Low
        assert_eq!(
            matrix.get_priority(ImpactLevel::Negligible, LikelihoodLevel::Rare),
            PriorityLevel::Low
        );
    }

    #[test]
    fn test_classify_consensus_error() {
        let mut classifier = ErrorClassifier::new();

        let info = classifier.classify("Consensus quorum not reached");

        assert_eq!(info.category, ErrorCategory::Consensus);
        assert!(info.priority.value() >= PriorityLevel::Medium.value());
    }

    #[test]
    fn test_classify_network_error() {
        let mut classifier = ErrorClassifier::new();

        let info = classifier.classify("Connection timeout");

        assert_eq!(info.category, ErrorCategory::Network);
    }

    #[test]
    fn test_classify_crypto_error() {
        let mut classifier = ErrorClassifier::new();

        let info = classifier.classify("Signature verification failed");

        assert_eq!(info.category, ErrorCategory::Cryptographic);
    }

    #[test]
    fn test_recurrence_increases_likelihood() {
        let mut classifier = ErrorClassifier::new();

        let error = "Network timeout occurred";

        // First occurrence
        let info1 = classifier.classify(error);

        // Multiple occurrences
        for _ in 0..5 {
            classifier.classify(error);
        }

        let info2 = classifier.classify(error);

        // Likelihood should increase
        assert!(info2.likelihood.value() >= info1.likelihood.value());
    }

    #[test]
    fn test_unknown_error() {
        let mut classifier = ErrorClassifier::new();

        let info = classifier.classify("Some random error xyz123");

        assert_eq!(info.category, ErrorCategory::Unknown);
    }
}
