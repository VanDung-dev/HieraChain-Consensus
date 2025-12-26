//! Error Mitigation Types
//!
//! Defines enums and types for error classification and risk assessment.

use serde::{Deserialize, Serialize};

/// Priority levels for error handling
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PriorityLevel {
    /// Critical - immediate action required
    Critical,
    /// High priority - address soon
    High,
    /// Medium priority - normal handling
    Medium,
    /// Low priority - can be deferred
    Low,
}

impl PriorityLevel {
    /// Get the string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            PriorityLevel::Critical => "CRITICAL",
            PriorityLevel::High => "HIGH",
            PriorityLevel::Medium => "MEDIUM",
            PriorityLevel::Low => "LOW",
        }
    }

    /// Parse from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "CRITICAL" => Some(PriorityLevel::Critical),
            "HIGH" => Some(PriorityLevel::High),
            "MEDIUM" => Some(PriorityLevel::Medium),
            "LOW" => Some(PriorityLevel::Low),
            _ => None,
        }
    }

    /// Get numeric value for sorting/comparison
    pub fn value(&self) -> u8 {
        match self {
            PriorityLevel::Critical => 4,
            PriorityLevel::High => 3,
            PriorityLevel::Medium => 2,
            PriorityLevel::Low => 1,
        }
    }
}

impl std::fmt::Display for PriorityLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Default for PriorityLevel {
    fn default() -> Self {
        PriorityLevel::Medium
    }
}

/// Error categories for classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    /// Consensus-related errors
    Consensus,
    /// Network/communication errors
    Network,
    /// Cryptographic errors
    Cryptographic,
    /// Validation errors
    Validation,
    /// Resource/system errors
    Resource,
    /// Configuration errors
    Configuration,
    /// Unknown/unclassified errors
    Unknown,
}

impl ErrorCategory {
    /// Get the string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCategory::Consensus => "consensus",
            ErrorCategory::Network => "network",
            ErrorCategory::Cryptographic => "cryptographic",
            ErrorCategory::Validation => "validation",
            ErrorCategory::Resource => "resource",
            ErrorCategory::Configuration => "configuration",
            ErrorCategory::Unknown => "unknown",
        }
    }

    /// Parse from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "consensus" => Some(ErrorCategory::Consensus),
            "network" => Some(ErrorCategory::Network),
            "cryptographic" => Some(ErrorCategory::Cryptographic),
            "validation" => Some(ErrorCategory::Validation),
            "resource" => Some(ErrorCategory::Resource),
            "configuration" => Some(ErrorCategory::Configuration),
            "unknown" => Some(ErrorCategory::Unknown),
            _ => None,
        }
    }
}

impl std::fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Default for ErrorCategory {
    fn default() -> Self {
        ErrorCategory::Unknown
    }
}

/// Impact level for risk assessment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ImpactLevel {
    /// System-breaking, catastrophic
    Catastrophic,
    /// Significant degradation
    Major,
    /// Noticeable but manageable
    Moderate,
    /// Minimal impact
    Minor,
    /// Virtually no impact
    Negligible,
}

impl ImpactLevel {
    /// Get the string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            ImpactLevel::Catastrophic => "CATASTROPHIC",
            ImpactLevel::Major => "MAJOR",
            ImpactLevel::Moderate => "MODERATE",
            ImpactLevel::Minor => "MINOR",
            ImpactLevel::Negligible => "NEGLIGIBLE",
        }
    }

    /// Parse from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "CATASTROPHIC" => Some(ImpactLevel::Catastrophic),
            "MAJOR" => Some(ImpactLevel::Major),
            "MODERATE" => Some(ImpactLevel::Moderate),
            "MINOR" => Some(ImpactLevel::Minor),
            "NEGLIGIBLE" => Some(ImpactLevel::Negligible),
            _ => None,
        }
    }

    /// Get numeric value for calculations
    pub fn value(&self) -> u8 {
        match self {
            ImpactLevel::Catastrophic => 5,
            ImpactLevel::Major => 4,
            ImpactLevel::Moderate => 3,
            ImpactLevel::Minor => 2,
            ImpactLevel::Negligible => 1,
        }
    }
}

impl std::fmt::Display for ImpactLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Default for ImpactLevel {
    fn default() -> Self {
        ImpactLevel::Moderate
    }
}

/// Likelihood level for risk assessment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LikelihoodLevel {
    /// Almost certain to occur
    AlmostCertain,
    /// Likely to occur
    Likely,
    /// Possible occurrence
    Possible,
    /// Unlikely to occur
    Unlikely,
    /// Rare occurrence
    Rare,
}

impl LikelihoodLevel {
    /// Get the string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            LikelihoodLevel::AlmostCertain => "ALMOST_CERTAIN",
            LikelihoodLevel::Likely => "LIKELY",
            LikelihoodLevel::Possible => "POSSIBLE",
            LikelihoodLevel::Unlikely => "UNLIKELY",
            LikelihoodLevel::Rare => "RARE",
        }
    }

    /// Parse from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().replace(' ', "_").as_str() {
            "ALMOST_CERTAIN" => Some(LikelihoodLevel::AlmostCertain),
            "LIKELY" => Some(LikelihoodLevel::Likely),
            "POSSIBLE" => Some(LikelihoodLevel::Possible),
            "UNLIKELY" => Some(LikelihoodLevel::Unlikely),
            "RARE" => Some(LikelihoodLevel::Rare),
            _ => None,
        }
    }

    /// Get numeric value for calculations
    pub fn value(&self) -> u8 {
        match self {
            LikelihoodLevel::AlmostCertain => 5,
            LikelihoodLevel::Likely => 4,
            LikelihoodLevel::Possible => 3,
            LikelihoodLevel::Unlikely => 2,
            LikelihoodLevel::Rare => 1,
        }
    }
}

impl std::fmt::Display for LikelihoodLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Default for LikelihoodLevel {
    fn default() -> Self {
        LikelihoodLevel::Possible
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_level() {
        assert_eq!(PriorityLevel::Critical.as_str(), "CRITICAL");
        assert_eq!(PriorityLevel::from_str("high"), Some(PriorityLevel::High));
        assert!(PriorityLevel::Critical.value() > PriorityLevel::Low.value());
    }

    #[test]
    fn test_error_category() {
        assert_eq!(ErrorCategory::Consensus.as_str(), "consensus");
        assert_eq!(
            ErrorCategory::from_str("Network"),
            Some(ErrorCategory::Network)
        );
    }

    #[test]
    fn test_impact_level() {
        assert_eq!(ImpactLevel::Catastrophic.value(), 5);
        assert_eq!(ImpactLevel::from_str("MAJOR"), Some(ImpactLevel::Major));
    }

    #[test]
    fn test_likelihood_level() {
        assert_eq!(LikelihoodLevel::AlmostCertain.as_str(), "ALMOST_CERTAIN");
        assert_eq!(
            LikelihoodLevel::from_str("likely"),
            Some(LikelihoodLevel::Likely)
        );
    }

    #[test]
    fn test_serialization() {
        let priority = PriorityLevel::High;
        let json = serde_json::to_string(&priority).unwrap();
        assert_eq!(json, "\"HIGH\"");

        let deserialized: PriorityLevel = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, priority);
    }
}
