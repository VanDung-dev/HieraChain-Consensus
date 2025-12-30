//! Arrow Schemas for HieraChain Core Data Structures.
//!
//! This module defines the Apache Arrow schemas used for:
//! - Events: Domain-specific actions
//! - Blocks: Groups of events (Header + Event List)
//! - Transactions: Standardized transport format

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Return the Arrow schema for an Event.
/// Matches Python's EVENT_SCHEMA.
pub fn get_event_schema() -> Schema {
    Schema::new(vec![
        Field::new("entity_id", DataType::Utf8, true),
        Field::new("event", DataType::Utf8, true),
        Field::new("timestamp", DataType::Float64, true),
        // Map<String, String> in Arrow is List<Struct<key, value>>
        Field::new(
            "details",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false, // Map entries are not nullable
                )),
                false, // Map is not sorted
            ),
            true,
        ),
        Field::new("data", DataType::Binary, true),
    ])
}

/// Return the Arrow schema for a Block Header.
/// Matches Python's BLOCK_HEADER_SCHEMA.
pub fn get_block_header_schema() -> Schema {
    Schema::new(vec![
        Field::new("index", DataType::Int64, true),
        Field::new("timestamp", DataType::Float64, true),
        Field::new("previous_hash", DataType::Utf8, true),
        Field::new("nonce", DataType::Int64, true),
        Field::new("merkle_root", DataType::Utf8, true),
        Field::new("hash", DataType::Utf8, true),
    ])
}

/// Return the Arrow schema for a full Block (header + events).
/// Match Python's `get_block_schema`.
pub fn get_block_schema() -> Schema {
    let event_struct_fields = vec![
        Field::new("entity_id", DataType::Utf8, true),
        Field::new("event", DataType::Utf8, true),
        Field::new("timestamp", DataType::Float64, true),
        Field::new(
            "details",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        ),
        Field::new("data", DataType::Binary, true),
    ];

    Schema::new(vec![
        Field::new("index", DataType::Int64, true),
        Field::new("timestamp", DataType::Float64, true),
        Field::new("previous_hash", DataType::Utf8, true),
        Field::new("nonce", DataType::Int64, true),
        Field::new("merkle_root", DataType::Utf8, true),
        Field::new("hash", DataType::Utf8, true),
        Field::new(
            "events",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(event_struct_fields.into()),
                true,
            ))),
            true,
        ),
    ])
}

/// Return the Arrow schema for a Transaction.
/// Defines consistency across Rust, Go, and Python.
pub fn get_transaction_schema() -> Schema {
    Schema::new(vec![
        Field::new("tx_id", DataType::Utf8, false), // Mandatory
        Field::new("entity_id", DataType::Utf8, false), // Mandatory
        Field::new("event_type", DataType::Utf8, false), // Mandatory
        Field::new("arrow_payload", DataType::Binary, true),
        Field::new("signature", DataType::Utf8, true),
        Field::new("timestamp", DataType::Float64, false), // Mandatory
        Field::new(
            "details",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        ),
    ])
}
