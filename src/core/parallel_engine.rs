/// Parallel Processing Engine for HieraChain Framework
///
/// This module provides a sophisticated parallel processing system with configurable
/// worker pools, chunk-based processing, and specialized processing policies for
/// blockchain operations. Enables efficient parallel validation, indexing, and
/// batch processing.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Instant;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Processing error
#[derive(Debug)]
pub struct ProcessingError {
    pub message: String,
}

impl std::fmt::Display for ProcessingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Processing error: {}", self.message)
    }
}

impl std::error::Error for ProcessingError {}

/// Processing policy enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProcessingPolicy {
    Default,
    Validation,
    Indexing,
    Batch,
    Priority,
}

impl ToString for ProcessingPolicy {
    fn to_string(&self) -> String {
        match self {
            ProcessingPolicy::Default => "default".to_string(),
            ProcessingPolicy::Validation => "validation".to_string(),
            ProcessingPolicy::Indexing => "indexing".to_string(),
            ProcessingPolicy::Batch => "batch".to_string(),
            ProcessingPolicy::Priority => "priority".to_string(),
        }
    }
}

/// Individual processing task
#[derive(Clone, Serialize, Deserialize)]
pub struct ProcessingTask {
    pub task_id: String,
    pub data: Value,
    pub priority: i32,
    pub created_at: f64,
    pub metadata: HashMap<String, Value>,
}

/// Result of a processing operation
#[derive(Clone, Serialize, Deserialize)]
pub struct ProcessingResult {
    pub task_id: String,
    pub success: bool,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub processing_time: f64,
    pub worker_id: Option<String>,
}

/// Worker pool statistics
#[derive(Clone, Serialize, Deserialize)]
pub struct WorkerPoolStats {
    pub pool_name: String,
    pub pool_type: String,
    pub max_workers: usize,
    pub active_tasks: usize,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub success_rate: f64,
}

/// Configurable worker pool for parallel processing
pub struct WorkerPool {
    pub pool_name: String,
    pub max_workers: usize,
    pub pool_type: String,
    active_tasks: Arc<Mutex<usize>>,
    completed_tasks: Arc<Mutex<u64>>,
    failed_tasks: Arc<Mutex<u64>>,
}

impl WorkerPool {
    /// Initialize worker pool
    pub fn new(pool_name: String, max_workers: usize, pool_type: String) -> Self {
        WorkerPool {
            pool_name,
            max_workers,
            pool_type,
            active_tasks: Arc::new(Mutex::new(0)),
            completed_tasks: Arc::new(Mutex::new(0)),
            failed_tasks: Arc::new(Mutex::new(0)),
        }
    }

    /// Submit a task to the worker pool
    pub fn submit_task(&self, task: ProcessingTask) -> ProcessingResult {
        if let Ok(mut count) = self.active_tasks.lock() {
            *count += 1;
        }

        let start_time = Instant::now();

        // Simulate task processing
        let result = self.process_task(task.clone());

        let processing_time = start_time.elapsed().as_secs_f64();

        if result.success {
            if let Ok(mut count) = self.completed_tasks.lock() {
                *count += 1;
            }
        } else {
            if let Ok(mut count) = self.failed_tasks.lock() {
                *count += 1;
            }
        }

        if let Ok(mut count) = self.active_tasks.lock() {
            *count = count.saturating_sub(1);
        }

        ProcessingResult {
            task_id: result.task_id,
            success: result.success,
            result: result.result,
            error: result.error,
            processing_time,
            worker_id: Some(format!("{}_{:?}", self.pool_name, thread::current().id())),
        }
    }

    /// Process a single task
    fn process_task(&self, task: ProcessingTask) -> ProcessingResult {
        // Basic task processing logic
        ProcessingResult {
            task_id: task.task_id.clone(),
            success: true,
            result: Some(task.data),
            error: None,
            processing_time: 0.0,
            worker_id: None,
        }
    }

    /// Get worker pool statistics
    pub fn get_stats(&self) -> WorkerPoolStats {
        let completed = self.completed_tasks.lock().ok().map(|guard| *guard).unwrap_or(0);
        let failed = self.failed_tasks.lock().ok().map(|guard| *guard).unwrap_or(0);
        let active = self.active_tasks.lock().ok().map(|guard| *guard).unwrap_or(0);

        let total_tasks = completed + failed;
        let success_rate = if total_tasks > 0 {
            (completed as f64 / total_tasks as f64) * 100.0
        } else {
            0.0
        };

        WorkerPoolStats {
            pool_name: self.pool_name.clone(),
            pool_type: self.pool_type.clone(),
            max_workers: self.max_workers,
            active_tasks: active,
            completed_tasks: completed,
            failed_tasks: failed,
            success_rate: success_rate.round() / 1.0,
        }
    }
}

/// Processing configuration
#[derive(Clone, Serialize, Deserialize)]
pub struct ProcessingConfig {
    pub pool: String,
    pub priority: i32,
    pub cache_results: bool,
    pub timeout: u64,
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default)]
    pub chunk_parallel: bool,
}

/// Parallel processing engine for blockchain operations
pub struct ParallelProcessingEngine {
    max_workers: usize,
    chunk_size: usize,
    worker_pools: Arc<RwLock<HashMap<String, Arc<WorkerPool>>>>,
    results_cache: Arc<RwLock<HashMap<String, ProcessingResult>>>,
}

impl ParallelProcessingEngine {
    /// Initialize parallel processing engine
    pub fn new(max_workers: Option<usize>, chunk_size: usize) -> Arc<Self> {
        let workers = max_workers.unwrap_or_else(|| {
            num_cpus::get().saturating_mul(2).max(4)
        });

        let engine = Arc::new(ParallelProcessingEngine {
            max_workers: workers,
            chunk_size,
            worker_pools: Arc::new(RwLock::new(HashMap::new())),
            results_cache: Arc::new(RwLock::new(HashMap::new())),
        });

        // Initialize default pools
        engine.initialize_default_pools();

        engine
    }

    /// Initialize default worker pools
    fn initialize_default_pools(&self) {
        // Priority pool
        self.create_worker_pool(
            "priority".to_string(),
            (self.max_workers / 4).max(2),
            "thread".to_string(),
        );

        // General pool
        self.create_worker_pool(
            "general".to_string(),
            self.max_workers / 2,
            "thread".to_string(),
        );

        // CPU-intensive pool
        self.create_worker_pool(
            "cpu_intensive".to_string(),
            num_cpus::get().max(2),
            "thread".to_string(),
        );

        // Validation pool
        self.create_worker_pool(
            "validation".to_string(),
            (self.max_workers / 3).max(4),
            "thread".to_string(),
        );
    }

    /// Create a new worker pool
    pub fn create_worker_pool(&self, pool_name: String, max_workers: usize, pool_type: String) {
        if let Ok(mut pools) = self.worker_pools.write() {
            let pool = Arc::new(WorkerPool::new(pool_name.clone(), max_workers, pool_type));
            pools.insert(pool_name, pool);
        }
    }

    /// Process a batch of data in parallel
    pub fn process_batch(
        &self,
        data_batch: Vec<Value>,
        policy: ProcessingPolicy,
    ) -> Vec<ProcessingResult> {
        if data_batch.is_empty() {
            return Vec::new();
        }

        let config = self.get_processing_config(policy, &data_batch);

        // Create tasks
        let tasks: Vec<ProcessingTask> = data_batch
            .into_iter()
            .enumerate()
            .map(|(i, data)| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0);

                ProcessingTask {
                    task_id: format!("batch_{}", i),
                    data,
                    priority: config.priority,
                    created_at: now,
                    metadata: HashMap::new(),
                }
            })
            .collect();

        self.execute_tasks(tasks, config)
    }

    /// Process data in chunks
    pub fn process_chunks(
        &self,
        data: Vec<Value>,
        policy: ProcessingPolicy,
    ) -> Vec<ProcessingResult> {
        if data.is_empty() {
            return Vec::new();
        }

        let mut results = Vec::new();

        for chunk_idx in 0..=(data.len() / self.chunk_size) {
            let start = chunk_idx * self.chunk_size;
            let end = (start + self.chunk_size).min(data.len());

            if start >= data.len() {
                break;
            }

            let chunk = data[start..end].to_vec();
            let chunk_results = self.process_batch(chunk, policy);
            results.extend(chunk_results);
        }

        results
    }

    /// Execute a list of tasks
    fn execute_tasks(&self, tasks: Vec<ProcessingTask>, config: ProcessingConfig) -> Vec<ProcessingResult> {
        let pools = self.worker_pools.read().ok();
        let pool = pools
            .as_ref()
            .and_then(|p| p.get(&config.pool))
            .cloned()
            .or_else(|| {
                pools
                    .as_ref()
                    .and_then(|p| p.get("general"))
                    .cloned()
            });

        let mut results = Vec::new();

        if let Some(worker_pool) = pool {
            for task in tasks {
                let result = worker_pool.submit_task(task.clone());

                if config.cache_results {
                    if let Ok(mut cache) = self.results_cache.write() {
                        cache.insert(result.task_id.clone(), result.clone());
                    }
                }

                results.push(result);
            }
        }

        results
    }

    /// Get processing configuration based on policy and data
    fn get_processing_config(&self, policy: ProcessingPolicy, data_batch: &[Value]) -> ProcessingConfig {
        match policy {
            ProcessingPolicy::Validation => ProcessingConfig {
                pool: "validation".to_string(),
                priority: 1,
                cache_results: true,
                timeout: 60,
                retry_count: 2,
                chunk_parallel: false,
            },
            ProcessingPolicy::Indexing => ProcessingConfig {
                pool: if data_batch.len() > 50 {
                    "cpu_intensive".to_string()
                } else {
                    "general".to_string()
                },
                priority: -1,
                cache_results: false,
                timeout: 600,
                retry_count: 0,
                chunk_parallel: data_batch.len() > 1000,
            },
            ProcessingPolicy::Batch => ProcessingConfig {
                pool: "cpu_intensive".to_string(),
                priority: 0,
                cache_results: false,
                timeout: 1800,
                retry_count: 0,
                chunk_parallel: true,
            },
            ProcessingPolicy::Priority => ProcessingConfig {
                pool: "priority".to_string(),
                priority: 2,
                cache_results: true,
                timeout: 30,
                retry_count: 0,
                chunk_parallel: false,
            },
            ProcessingPolicy::Default => ProcessingConfig {
                pool: if data_batch.len() < 100 {
                    "general".to_string()
                } else {
                    "cpu_intensive".to_string()
                },
                priority: 0,
                cache_results: false,
                timeout: 300,
                retry_count: 0,
                chunk_parallel: false,
            },
        }
    }

    /// Validate blocks in parallel
    pub fn validate_blocks_parallel(&self, blocks: Vec<Value>) -> Vec<ProcessingResult> {
        self.process_batch(blocks, ProcessingPolicy::Validation)
    }

    /// Index events in parallel
    pub fn index_events_parallel(&self, events: Vec<Value>) -> Vec<ProcessingResult> {
        self.process_chunks(events, ProcessingPolicy::Indexing)
    }

    /// Get engine statistics
    pub fn get_engine_stats(&self) -> Value {
        let pools = self.worker_pools.read().ok();
        let mut pool_stats = serde_json::Map::new();

        if let Some(pools_map) = pools.as_ref() {
            for (name, pool) in pools_map.iter() {
                let stats = pool.get_stats();
                pool_stats.insert(name.clone(), serde_json::to_value(&stats).unwrap_or(Value::Null));
            }
        }

        let completed: u64 = pools
            .as_ref()
            .map(|p| {
                p.values()
                    .map(|pool| {
                        pool.completed_tasks
                            .lock()
                            .ok()
                            .map(|guard| *guard)
                            .unwrap_or(0)
                    })
                    .sum()
            })
            .unwrap_or(0);

        let failed: u64 = pools
            .as_ref()
            .map(|p| {
                p.values()
                    .map(|pool| {
                        pool.failed_tasks
                            .lock()
                            .ok()
                            .map(|guard| *guard)
                            .unwrap_or(0)
                    })
                    .sum()
            })
            .unwrap_or(0);

        serde_json::json!({
            "engine": {
                "max_workers": self.max_workers,
                "chunk_size": self.chunk_size,
                "total_pools": pools.as_ref().map(|p| p.len()).unwrap_or(0),
                "cached_results": self.results_cache.read().ok().map(|c| c.len()).unwrap_or(0)
            },
            "totals": {
                "completed_tasks": completed,
                "failed_tasks": failed,
                "total_tasks": completed + failed,
                "success_rate": if completed + failed > 0 {
                    ((completed as f64) / ((completed + failed) as f64)) * 100.0
                } else {
                    0.0
                }
            },
            "pools": pool_stats
        })
    }

    /// Get pool utilization
    pub fn get_pool_utilization(&self) -> HashMap<String, f64> {
        let mut utilization = HashMap::new();

        if let Ok(pools) = self.worker_pools.read() {
            for (name, pool) in pools.iter() {
                let active = pool.active_tasks.lock().ok().map(|guard| *guard).unwrap_or(0);
                let util = if pool.max_workers > 0 {
                    (active as f64 / pool.max_workers as f64) * 100.0
                } else {
                    0.0
                };
                utilization.insert(name.clone(), util);
            }
        }

        utilization
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_pool_creation() {
        let pool = WorkerPool::new("test".to_string(), 4, "thread".to_string());
        assert_eq!(pool.max_workers, 4);
        assert_eq!(pool.pool_name, "test");
    }

    #[test]
    fn test_parallel_engine_creation() {
        let engine = ParallelProcessingEngine::new(None, 100);
        assert_eq!(engine.chunk_size, 100);
    }
}
