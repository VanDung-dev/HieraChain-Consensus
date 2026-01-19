//! Client SDK for HieraChain
//!
//! Provides a resilient client for interacting with HieraChain nodes,
//! featuring built-in exponential backoff and retry logic.

use backoff::ExponentialBackoff;
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use thiserror::Error;
use tracing::{error, info, instrument, warn};

/// Client specific errors
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("API error {status}: {message}")]
    Api { status: StatusCode, message: String },
    #[error("Configuration error: {0}")]
    Config(String),
}

/// HieraChain Client Configuration
#[derive(Clone)]
pub struct ClientConfig {
    pub base_url: String,
    pub timeout_seconds: u64,
}

/// Main Client Struct
#[derive(Debug)]
pub struct HieraChainClient {
    base_url: Url,
    http_client: Client,
}

impl HieraChainClient {
    /// Create a new HieraChainClient
    pub fn new(config: ClientConfig) -> Result<Self, ClientError> {
        let base_url = Url::parse(&config.base_url)
            .map_err(|e| ClientError::Config(format!("Invalid URL: {}", e)))?;

        let http_client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .build()?;

        Ok(Self {
            base_url,
            http_client,
        })
    }

    /// Execute a request with exponential backoff retry logic
    ///
    /// Retries on:
    /// - Network errors
    /// - 5xx Server errors
    /// - 429 Too Many Requests
    async fn execute_with_retry<F, Fut, T>(&self, operation: F) -> Result<T, ClientError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, ClientError>>,
    {
        let backoff = ExponentialBackoff {
            max_elapsed_time: Some(Duration::from_secs(60)), // Retry for up to 1 minute
            ..Default::default()
        };

        backoff::future::retry(backoff, || async {
            match operation().await {
                Ok(val) => {
                    info!("Request successful");
                    Ok(val)
                }
                Err(e) => match &e {
                    ClientError::Network(_) => {
                        warn!("Network error, retrying: {}", e);
                        Err(backoff::Error::transient(e))
                    }
                    ClientError::Api { status, .. } => {
                        if status.is_server_error() || *status == StatusCode::TOO_MANY_REQUESTS {
                            warn!("Server error {}, retrying", status);
                            Err(backoff::Error::transient(e))
                        } else {
                            error!("Permanent API error: {}", e);
                            Err(backoff::Error::permanent(e))
                        }
                    }
                    _ => {
                        error!("Permanent client error: {}", e);
                        Err(backoff::Error::permanent(e))
                    }
                },
            }
        })
        .await
    }

    /// Submit an event to the node
    #[instrument(skip(self, event))]
    pub async fn submit_event(&self, event: &Value) -> Result<String, ClientError> {
        info!("Submitting event to HieraChain");
        self.execute_with_retry(|| async {
            let url = self.base_url.join("events/submit").unwrap();

            let response = self
                .http_client
                .post(url.clone())
                .json(event)
                .send()
                .await?;

            let status = response.status();
            if !status.is_success() {
                let message = response.text().await.unwrap_or_default();
                return Err(ClientError::Api { status, message });
            }

            // Assume successful response contains event ID string
            // This needs to match the actual API response structure
            #[derive(Deserialize)]
            struct SubmitResponse {
                event_id: String,
            }

            let resp_body: SubmitResponse = response.json().await?;
            Ok(resp_body.event_id)
        })
        .await
    }

    /// Get block by ID
    #[instrument(skip(self))]
    pub async fn get_block(&self, block_id: u64) -> Result<Value, ClientError> {
        info!("Fetching block {}", block_id);
        self.execute_with_retry(|| async {
            let url = self.base_url.join(&format!("blocks/{}", block_id)).unwrap();

            let response = self.http_client.get(url.clone()).send().await?;

            let status = response.status();
            if !status.is_success() {
                let message = response.text().await.unwrap_or_default();
                return Err(ClientError::Api { status, message });
            }

            let block: Value = response.json().await?;
            Ok(block)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: To properly test this module without a real server, we would need
    // a mock server implementation (like wiremock). For now, we test the
    // configuration and basic structure.

    #[test]
    fn test_client_configuration() {
        let config = ClientConfig {
            base_url: "http://localhost:8080".to_string(),
            timeout_seconds: 10,
        };

        let client = HieraChainClient::new(config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_invalid_url() {
        let config = ClientConfig {
            base_url: "not-a-url".to_string(),
            timeout_seconds: 10,
        };

        let client = HieraChainClient::new(config);
        assert!(matches!(client.unwrap_err(), ClientError::Config(_)));
    }
}
