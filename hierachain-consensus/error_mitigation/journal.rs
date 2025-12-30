use crate::consensus::types::{ArrowEventData, EventPayload};
use arrow::array::ArrayRef;
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::ipc::reader::read_record_batch;
use arrow::ipc::writer::StreamWriter;
use arrow::ipc::{root_as_message, MetadataVersion};
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub struct TransactionJournal {
    file_path: PathBuf,
    writer: Mutex<Option<File>>,
}

impl TransactionJournal {
    pub fn new(storage_dir: &str, log_name: &str) -> io::Result<Self> {
        let mut path = PathBuf::from(storage_dir);
        std::fs::create_dir_all(&path)?;
        path.push(log_name);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(&path)?;

        Ok(TransactionJournal {
            file_path: path,
            writer: Mutex::new(Some(file)),
        })
    }

    pub fn log_event(&self, event_data: &Value) -> io::Result<()> {
        let mut guard = self.writer.lock().unwrap();
        if let Some(file) = guard.as_mut() {
            let json_str = serde_json::to_string(event_data)?;
            writeln!(file, "{}", json_str)?;
            file.flush()?; // Ensure durability
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "Journal closed"))
        }
    }

    pub fn log_arrow_event(&self, batch: &RecordBatch) -> io::Result<()> {
        let mut guard = self.writer.lock().unwrap();
        if let Some(file) = guard.as_mut() {

            // Calculate schema size using a temporary buffer/writer
            let schema_size = {
                let mut dummy = Vec::new();
                let _ = StreamWriter::try_new(&mut dummy, &batch.schema())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                dummy.len()
            };

            let mut buffer = Vec::new();
            {
                // Real writer
                let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

                // Write the batch
                writer
                    .write(batch)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

                // Drop writer
            }

            // Verify buffer has more than just schema
            if buffer.len() <= schema_size {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Failed to serialize Arrow batch",
                ));
            }

            let batch_message = &buffer[schema_size..];

            // Write Length Prefix (4 bytes, little endian)
            let len = batch_message.len() as u32;
            file.write_all(&len.to_le_bytes())?;

            // Write Arrow Data
            file.write_all(batch_message)?;

            file.flush()?;
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "Journal closed"))
        }
    }

    // Schema definition helper (matches Python's EVENT_SCHEMA)
    fn get_event_schema() -> Schema {
        Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, true),
            Field::new("event", DataType::Utf8, true),
            Field::new("timestamp", DataType::Float64, true),
            Field::new(
                "details",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Arc::new(Field::new("key", DataType::Utf8, false)),
                            Arc::new(Field::new("value", DataType::Utf8, true)),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            ),
            Field::new("data", DataType::Binary, true),
        ])
    }

    pub fn replay(&self) -> io::Result<Vec<EventPayload>> {
        if !self.file_path.exists() {
            return Ok(Vec::new());
        }

        let mut file = File::open(&self.file_path)?;
        let mut events = Vec::new();
        let schema = Arc::new(Self::get_event_schema());
        let projection = None; // Read all columns

        loop {
            // 1. Read Length (4 bytes)
            let mut len_buf = [0u8; 4];
            if let Err(e) = file.read_exact(&mut len_buf) {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    break; // EOF
                }
                return Err(e);
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            // 2. Read Message Body
            let mut body_buf = vec![0u8; len];
            file.read_exact(&mut body_buf)?;

            // 3. Decode RecordBatch
            let dictionaries: HashMap<i64, ArrayRef> = HashMap::new();

            // Parse FlatBuffers Message
            let message = root_as_message(&body_buf[..]).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Invalid IPC: {:?}", e))
            })?;

            // Extract header as RecordBatch (safer than matching enum)
            let ipc_batch = message.header_as_record_batch().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "IPC message is not a RecordBatch",
                )
            })?;

            let buffer = Buffer::from(&body_buf[..]);

            // read_record_batch(buffer, ipc_batch, schema, dicts, proj, version)
            // Using ipc_batch (by value)
            match read_record_batch(
                &buffer,
                ipc_batch,
                schema.clone(),
                &dictionaries,
                projection,
                &MetadataVersion::V5,
            ) {
                Ok(batch) => {
                    events.push(EventPayload::Arrow(ArrowEventData {
                        batch: Arc::new(batch),
                        schema_digest: "digest_placeholder".to_string(),
                    }));
                }
                Err(e) => {
                    eprintln!("Failed to decode replay batch: {}", e);
                    continue;
                }
            }
        }

        Ok(events)
    }

    pub fn close(&self) {
        let mut guard = self.writer.lock().unwrap();
        *guard = None;
    }
}
