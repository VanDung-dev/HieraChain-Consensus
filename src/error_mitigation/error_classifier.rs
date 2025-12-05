/// Error Mitigation Error Classifier Module
///
/// This module provides error classification and risk prioritization. It categorizes
/// errors by priority level, impact, and recovery strategy to enable targeted
/// mitigation approaches.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Error priority levels based on risk assessment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PriorityLevel {
    Critical = 1,
    High = 2,
    Medium = 3,
    Low = 4,
}

impl fmt::Display for PriorityLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PriorityLevel::Critical => write!(f, "CRITICAL"),
            PriorityLevel::High => write!(f, "HIGH"),
            PriorityLevel::Medium => write!(f, "MEDIUM"),
            PriorityLevel::Low => write!(f, "LOW"),
        }
    }
}

/// Error categories for HieraChain framework
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ErrorCategory {
    Consensus,
    Security,
    Performance,
    Storage,
    Network,
    Api,
    Operational,
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCategory::Consensus => write!(f, "consensus"),
            ErrorCategory::Security => write!(f, "security"),
            ErrorCategory::Performance => write!(f, "performance"),
            ErrorCategory::Storage => write!(f, "storage"),
            ErrorCategory::Network => write!(f, "network"),
            ErrorCategory::Api => write!(f, "api"),
            ErrorCategory::Operational => write!(f, "operational"),
        }
    }
}

/// Impact levels for risk assessment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ImpactLevel {
    Catastrophic = 5,
    Major = 4,
    Moderate = 3,
    Minor = 2,
    Negligible = 1,
}

impl fmt::Display for ImpactLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ImpactLevel::Catastrophic => write!(f, "CATASTROPHIC"),
            ImpactLevel::Major => write!(f, "MAJOR"),
            ImpactLevel::Moderate => write!(f, "MODERATE"),
            ImpactLevel::Minor => write!(f, "MINOR"),
            ImpactLevel::Negligible => write!(f, "NEGLIGIBLE"),
        }
    }
}

/// Likelihood levels for risk assessment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LikelihoodLevel {
    VeryHigh = 5,
    High = 4,
    Medium = 3,
    Low = 2,
    VeryLow = 1,
}

impl fmt::Display for LikelihoodLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LikelihoodLevel::VeryHigh => write!(f, "VERY_HIGH"),
            LikelihoodLevel::High => write!(f, "HIGH"),
            LikelihoodLevel::Medium => write!(f, "MEDIUM"),
            LikelihoodLevel::Low => write!(f, "LOW"),
            LikelihoodLevel::VeryLow => write!(f, "VERY_LOW"),
        }
    }
}

/// Information about a classified error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub error_id: String,
    pub error_type: String,
    pub category: ErrorCategory,
    pub priority: PriorityLevel,
    pub impact: ImpactLevel,
    pub likelihood: LikelihoodLevel,
    pub description: String,
    pub mitigation_strategy: String,
    pub timestamp: f64,
    pub metadata: HashMap<String, String>,
}

/// Risk priority matrix for assessing error severity
pub struct RiskPriorityMatrix {
    matrix: HashMap<(ImpactLevel, LikelihoodLevel), PriorityLevel>,
}

impl RiskPriorityMatrix {
    /// Create a new risk priority matrix
    pub fn new() -> Self {
        let mut matrix = HashMap::new();

        // Catastrophic impact
        matrix.insert((ImpactLevel::Catastrophic, LikelihoodLevel::VeryHigh), PriorityLevel::Critical);
        matrix.insert((ImpactLevel::Catastrophic, LikelihoodLevel::High), PriorityLevel::Critical);
        matrix.insert((ImpactLevel::Catastrophic, LikelihoodLevel::Medium), PriorityLevel::Critical);
        matrix.insert((ImpactLevel::Catastrophic, LikelihoodLevel::Low), PriorityLevel::High);
        matrix.insert((ImpactLevel::Catastrophic, LikelihoodLevel::VeryLow), PriorityLevel::High);

        // Major impact
        matrix.insert((ImpactLevel::Major, LikelihoodLevel::VeryHigh), PriorityLevel::Critical);
        matrix.insert((ImpactLevel::Major, LikelihoodLevel::High), PriorityLevel::Critical);
        matrix.insert((ImpactLevel::Major, LikelihoodLevel::Medium), PriorityLevel::High);
        matrix.insert((ImpactLevel::Major, LikelihoodLevel::Low), PriorityLevel::High);
        matrix.insert((ImpactLevel::Major, LikelihoodLevel::VeryLow), PriorityLevel::Medium);

        // Moderate impact
        matrix.insert((ImpactLevel::Moderate, LikelihoodLevel::VeryHigh), PriorityLevel::High);
        matrix.insert((ImpactLevel::Moderate, LikelihoodLevel::High), PriorityLevel::High);
        matrix.insert((ImpactLevel::Moderate, LikelihoodLevel::Medium), PriorityLevel::Medium);
        matrix.insert((ImpactLevel::Moderate, LikelihoodLevel::Low), PriorityLevel::Medium);
        matrix.insert((ImpactLevel::Moderate, LikelihoodLevel::VeryLow), PriorityLevel::Low);

        // Minor impact
        matrix.insert((ImpactLevel::Minor, LikelihoodLevel::VeryHigh), PriorityLevel::Medium);
        matrix.insert((ImpactLevel::Minor, LikelihoodLevel::High), PriorityLevel::Medium);
        matrix.insert((ImpactLevel::Minor, LikelihoodLevel::Medium), PriorityLevel::Low);
        matrix.insert((ImpactLevel::Minor, LikelihoodLevel::Low), PriorityLevel::Low);
        matrix.insert((ImpactLevel::Minor, LikelihoodLevel::VeryLow), PriorityLevel::Low);

        // Negligible impact
        matrix.insert((ImpactLevel::Negligible, LikelihoodLevel::VeryHigh), PriorityLevel::Low);
        matrix.insert((ImpactLevel::Negligible, LikelihoodLevel::High), PriorityLevel::Low);
        matrix.insert((ImpactLevel::Negligible, LikelihoodLevel::Medium), PriorityLevel::Low);
        matrix.insert((ImpactLevel::Negligible, LikelihoodLevel::Low), PriorityLevel::Low);
        matrix.insert((ImpactLevel::Negligible, LikelihoodLevel::VeryLow), PriorityLevel::Low);

        RiskPriorityMatrix { matrix }
    }

    /// Calculate priority level based on impact and likelihood
    pub fn calculate_priority(&self, impact: ImpactLevel, likelihood: LikelihoodLevel) -> PriorityLevel {
        self.matrix
            .get(&(impact, likelihood))
            .copied()
            .unwrap_or(PriorityLevel::Medium)
    }

    /// Get numeric score for priority level
    pub fn get_priority_score(priority: PriorityLevel) -> i32 {
        priority as i32
    }
}

impl Default for RiskPriorityMatrix {
    fn default() -> Self {
        Self::new()
    }
}

/// Main error classifier for HieraChain framework
pub struct ErrorClassifier {
    risk_matrix: RiskPriorityMatrix,
    error_patterns: HashMap<String, String>,
    classification_history: Vec<ErrorInfo>,
    mitigation_strategies: HashMap<String, String>,
}

impl ErrorClassifier {
    /// Create a new error classifier
    pub fn new() -> Self {
        ErrorClassifier {
            risk_matrix: RiskPriorityMatrix::new(),
            error_patterns: Self::load_error_patterns(),
            classification_history: Vec::new(),
            mitigation_strategies: Self::load_mitigation_strategies(),
        }
    }

    /// Classify an error and determine its priority and mitigation strategy
    pub fn classify_error(&mut self, error_data: &HashMap<String, String>) -> ErrorInfo {
        let error_type = error_data.get("error_type").cloned().unwrap_or_else(|| "unknown".to_string());
        let error_message = error_data.get("message").cloned().unwrap_or_default();

        // Determine category
        let category = self.determine_category(&error_type, &error_message);

        // Assess impact and likelihood
        let impact = Self::assess_impact(error_data, category);
        let likelihood = Self::assess_likelihood(category);

        // Calculate priority
        let priority = self.risk_matrix.calculate_priority(impact, likelihood);

        // Determine mitigation strategy
        let mitigation_strategy = self.determine_mitigation_strategy(category, priority);

        // Generate error ID
        let error_id = Self::generate_error_id(&error_type, &error_message);

        let mut metadata = HashMap::new();
        for (k, v) in error_data {
            if k != "error_type" && k != "message" {
                metadata.insert(k.clone(), v.clone());
            }
        }

        let error_info = ErrorInfo {
            error_id,
            error_type,
            category,
            priority,
            impact,
            likelihood,
            description: error_message,
            mitigation_strategy,
            timestamp: Self::current_timestamp(),
            metadata,
        };

        self.classification_history.push(error_info.clone());
        error_info
    }

    /// Get all errors of a specific priority level
    pub fn get_priority_errors(&self, priority: PriorityLevel) -> Vec<ErrorInfo> {
        self.classification_history
            .iter()
            .filter(|e| e.priority == priority)
            .cloned()
            .collect()
    }

    /// Get all errors of a specific category
    pub fn get_category_errors(&self, category: ErrorCategory) -> Vec<ErrorInfo> {
        self.classification_history
            .iter()
            .filter(|e| e.category == category)
            .cloned()
            .collect()
    }

    /// Get summary of error classifications
    pub fn get_classification_summary(&self) -> HashMap<String, serde_json::Value> {
        let mut summary = HashMap::new();

        summary.insert(
            "total_errors".to_string(),
            serde_json::Value::Number(self.classification_history.len().into()),
        );

        let mut categories = HashMap::new();
        for category in [
            ErrorCategory::Consensus,
            ErrorCategory::Security,
            ErrorCategory::Performance,
            ErrorCategory::Storage,
            ErrorCategory::Network,
            ErrorCategory::Api,
            ErrorCategory::Operational,
        ] {
            let count = self.get_category_errors(category).len();
            categories.insert(category.to_string(), serde_json::Value::Number(count.into()));
        }

        summary.insert("categories".to_string(), serde_json::json!(categories));

        let mut priorities = HashMap::new();
        for priority in [PriorityLevel::Critical, PriorityLevel::High, PriorityLevel::Medium, PriorityLevel::Low] {
            let count = self.get_priority_errors(priority).len();
            priorities.insert(priority.to_string(), serde_json::Value::Number(count.into()));
        }

        summary.insert("priorities".to_string(), serde_json::json!(priorities));
        summary
    }

    fn determine_category(&self, error_type: &str, error_message: &str) -> ErrorCategory {
        let combined = format!("{} {}", error_type, error_message).to_lowercase();

        for (pattern, category) in &self.error_patterns {
            if combined.contains(&pattern.to_lowercase()) {
                return match category.as_str() {
                    "consensus" => ErrorCategory::Consensus,
                    "security" => ErrorCategory::Security,
                    "performance" => ErrorCategory::Performance,
                    "storage" => ErrorCategory::Storage,
                    "network" => ErrorCategory::Network,
                    "api" => ErrorCategory::Api,
                    _ => ErrorCategory::Operational,
                };
            }
        }

        // Default categorization based on keywords
        if combined.contains("consensus") || combined.contains("bft") || combined.contains("leader") {
            ErrorCategory::Consensus
        } else if combined.contains("security") || combined.contains("encryption") || combined.contains("key") {
            ErrorCategory::Security
        } else if combined.contains("performance") || combined.contains("resource") || combined.contains("cpu") {
            ErrorCategory::Performance
        } else if combined.contains("storage") || combined.contains("backup") || combined.contains("database") {
            ErrorCategory::Storage
        } else if combined.contains("network") || combined.contains("timeout") {
            ErrorCategory::Network
        } else if combined.contains("api") || combined.contains("endpoint") {
            ErrorCategory::Api
        } else {
            ErrorCategory::Operational
        }
    }

    fn assess_impact(_error_data: &HashMap<String, String>, category: ErrorCategory) -> ImpactLevel {
        match category {
            ErrorCategory::Consensus => ImpactLevel::Major,
            ErrorCategory::Security => ImpactLevel::Major,
            ErrorCategory::Performance => ImpactLevel::Moderate,
            ErrorCategory::Storage => ImpactLevel::Moderate,
            ErrorCategory::Network => ImpactLevel::Moderate,
            ErrorCategory::Api => ImpactLevel::Minor,
            ErrorCategory::Operational => ImpactLevel::Minor,
        }
    }

    fn assess_likelihood(category: ErrorCategory) -> LikelihoodLevel {
        match category {
            ErrorCategory::Consensus => LikelihoodLevel::Medium,
            ErrorCategory::Security => LikelihoodLevel::Low,
            ErrorCategory::Performance => LikelihoodLevel::High,
            ErrorCategory::Storage => LikelihoodLevel::Medium,
            ErrorCategory::Network => LikelihoodLevel::High,
            ErrorCategory::Api => LikelihoodLevel::Low,
            ErrorCategory::Operational => LikelihoodLevel::Medium,
        }
    }

    fn determine_mitigation_strategy(&self, category: ErrorCategory, priority: PriorityLevel) -> String {
        let strategy_key = format!("{}_{}", category.to_string(), priority.to_string().to_lowercase());
        self.mitigation_strategies
            .get(&strategy_key)
            .cloned()
            .unwrap_or_else(|| "monitor_and_log".to_string())
    }

    fn generate_error_id(error_type: &str, message: &str) -> String {
        use sha2::{Sha256, Digest};

        let content = format!("{}{}{}", error_type, message, Self::current_timestamp());
        let mut hasher = Sha256::new();
        hasher.update(content);
        let result = hasher.finalize();

        format!("ERR-{}", hex::encode(&result[..6]).to_uppercase())
    }

    fn current_timestamp() -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0)
    }

    fn load_error_patterns() -> HashMap<String, String> {
        let mut patterns = HashMap::new();
        patterns.insert("insufficient nodes".to_string(), "consensus".to_string());
        patterns.insert("bft consensus".to_string(), "consensus".to_string());
        patterns.insert("leader failure".to_string(), "consensus".to_string());
        patterns.insert("signature verification".to_string(), "security".to_string());
        patterns.insert("encryption".to_string(), "security".to_string());
        patterns.insert("cpu threshold".to_string(), "performance".to_string());
        patterns.insert("backup failed".to_string(), "storage".to_string());
        patterns.insert("network partition".to_string(), "network".to_string());
        patterns
    }

    fn load_mitigation_strategies() -> HashMap<String, String> {
        let mut strategies = HashMap::new();
        strategies.insert("consensus_critical".to_string(), "immediate_leader_election".to_string());
        strategies.insert("consensus_high".to_string(), "view_change".to_string());
        strategies.insert("security_critical".to_string(), "emergency_key_rotation".to_string());
        strategies.insert("performance_high".to_string(), "auto_scaling".to_string());
        strategies.insert("network_high".to_string(), "redundant_path".to_string());
        strategies
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
    fn test_error_classification() {
        let mut classifier = ErrorClassifier::new();
        let mut error_data = HashMap::new();
        error_data.insert("error_type".to_string(), "bft_consensus".to_string());
        error_data.insert("message".to_string(), "insufficient nodes".to_string());

        let error_info = classifier.classify_error(&error_data);
        assert_eq!(error_info.category, ErrorCategory::Consensus);
    }

    #[test]
    fn test_risk_priority_matrix() {
        let matrix = RiskPriorityMatrix::new();
        let priority = matrix.calculate_priority(ImpactLevel::Catastrophic, LikelihoodLevel::VeryHigh);
        assert_eq!(priority, PriorityLevel::Critical);
    }
}
