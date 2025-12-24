use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Mutex;

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

    pub fn replay(&self) -> io::Result<Vec<Value>> {
        let file = File::open(&self.file_path)?;
        let reader = BufReader::new(file);
        let mut events = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(event) = serde_json::from_str(&line) {
                events.push(event);
            }
        }

        Ok(events)
    }

    pub fn close(&self) {
        let mut guard = self.writer.lock().unwrap();
        *guard = None;
    }
}
