use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader};
use tokio::sync::broadcast;

#[derive(Clone, Debug, Default)]
struct Stat {
    min: f64,
    max: f64,
    sum: f64,
    count: i64,
}

impl Stat {
    pub fn new(value: f64) -> Self {
        Stat {
            min: value,
            max: value,
            sum: value,
            count: 1,
        }
    }
}

#[derive(Debug, Default, Clone)]
struct ThreadData {
    stat: HashMap<String, Stat>,
}

struct StatResult {
    min: f64,
    max: f64,
    mean: f64,
}

impl fmt::Display for StatResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:.1}/{:.1}/{:.1}", self.min, self.mean, self.max)
    }
}

async fn read_file_in_chunks(
    file_path: &str,
    chunk_size: usize,
    num_threads: usize,
) -> anyhow::Result<()> {
    // Open the file and create a buffered reader
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    // Create a channel for sending chunks to worker threads
    let (tx, mut _rx) = broadcast::channel(16);

    // Spawn worker threads
    let mut handles = vec![];
    for i in 0..num_threads {
        let mut rx2 = tx.subscribe();
        let handle: tokio::task::JoinHandle<Result<_, anyhow::Error>> = tokio::spawn(async move {
            // Worker thread loop
            let mut data = ThreadData::default();
            while let Ok(chunk) = rx2.recv().await {
                process_chunk(&mut data, chunk, i)?;
            }
            //println!("Thread {} finished", i);
            Ok(data)
        });
        handles.push(handle);
    }

    // Read file and send chunks to workers
    let mut chunk = Vec::new();
    let mut line_count = 0;

    for line in reader.lines() {
        let line = line?;
        chunk.push(line);
        line_count += 1;

        // When chunk size is reached, send to workers
        if chunk.len() >= chunk_size {
            tx.send(chunk.clone())?;
            chunk.clear();
        }
    }

    // Send any remaining lines
    if !chunk.is_empty() {
        tx.send(chunk)?;
    }

    // Drop sender to signal workers to finish
    drop(tx);

    // Wait for all worker threads to complete
    let mut thread_datas = vec![];
    let mut keys = HashSet::new();
    for handle in handles {
        let r = handle.await.unwrap().unwrap();
        keys.extend(r.stat.keys().cloned());
        thread_datas.push(r);
    }

    let mut result = BTreeMap::new();
    for key in keys {
        let mut stat = Stat::default();
        for thread_data in &thread_datas {
            if let Some(s) = thread_data.stat.get(&key) {
                if s.min < stat.min {
                    stat.min = s.min;
                }
                if s.max > stat.max {
                    stat.max = s.max;
                }
                stat.sum += s.sum;
                stat.count += s.count;
            };
        }

        let r = StatResult {
            min: stat.min,
            max: stat.max,
            mean: stat.sum / stat.count as f64,
        };
        result.insert(key, r);
    }

    let formatted: Vec<String> = result.iter().map(|(k, v)| format!("{}={}", k, v)).collect();

    println!("Processed {} lines total", line_count);
    println!("{{{}}}", formatted.join(", "));
    Ok(())
}

// Function to process each chunk of lines
fn process_chunk(
    data: &mut ThreadData,
    chunk: Vec<String>,
    _thread_id: usize,
) -> anyhow::Result<()> {
    /*
    println!(
        "Thread {} processing chunk of {} lines",
        thread_id,
        chunk.len()
    );
    */
    for line in chunk {
        let parts: Vec<&str> = line.split(';').collect();
        if parts.len() != 2 {
            continue;
        }
        let city = parts[0].to_string();
        let value = parts[1].trim().parse::<f64>()?;
        data.stat
            .entry(city)
            .and_modify(|stat| {
                if value < stat.min {
                    stat.min = value;
                }
                if value > stat.max {
                    stat.max = value;
                }
                stat.sum += value;
                stat.count += 1;
            })
            .or_insert(Stat::new(value));
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let file_path = "data/weather_stations.csv"; // Replace with your file path
                                                 //let file_path = "data/test.csv"; // Replace with your file path
    let chunk_size = 1000;
    let num_threads = 20; // Adjust number of worker threads as needed

    match read_file_in_chunks(file_path, chunk_size, num_threads).await {
        Ok(_) => println!("File processed successfully"),
        Err(e) => eprintln!("Error reading file: {}", e),
    }
}
