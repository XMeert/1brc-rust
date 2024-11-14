use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::io::prelude::*;
use std::collections::HashMap;
use std::time::Instant;
use std::thread;
use std::sync::mpsc;
use std::io::{Seek, SeekFrom};

type StMap = HashMap<String, (String, f32, f32, f32, i32)>;

const LINES: usize = 1000000000; // number of lines to read, does not work with values under 10000000
const THREADS: usize = 16; // number of threads to spawn, this number cannot be changed

fn main() -> io::Result<()> {
    let now = Instant::now();
    let (tx, rx) = mpsc::channel();

    let take: usize = LINES / THREADS;
    let mut skips = 0;

    // Spawn threads for processing data
    for _ in 0..THREADS {
        let tx1 = tx.clone();
        thread::spawn(move|| {
            let stations = process(skips, take);
            tx1.send(stations).unwrap();
        });
        skips += 1;
    }

    // Collect each resulting hash map
    let mut iter = rx.into_iter();
    let group = vec![(iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap()),
    (iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap()),
    (iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap()),
    (iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap())];

    let (send, rcv) = mpsc::channel();

    // Spawn threads for the combining of hash maps
    for sub in group {
        let send1 = send.clone();
        thread::spawn(move|| {
            let stations = merge_maps(sub.0, sub.1, sub.2, sub.3);
            send1.send(stations).unwrap();
        });
    }

    // Combine resulting hash maps into one
    let mut result = rcv.into_iter();
    let stations = merge_maps(result.next().unwrap(), result.next().unwrap(), result.next().unwrap(), result.next().unwrap());

    // Create a file and write the results to it
    let mut file = File::create("src/output.csv").expect("Could not create file");

    for (_key, station) in &stations {
        let data = format!("{},{},{},{:.1}", station.0, station.1, station.2, station.3/station.4 as f32);
        if let Err(e) = writeln!(file, "{}", data) {
            eprintln!("Couldn't write to file: {}", e);
        }
    }

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
    Ok(())
}

fn process(skip: usize , take: usize) -> StMap {
    let path = Path::new("/home/xander/Documents/rust_workshop/2/OICT-Rust-Workshop/5_concurrency/challenge/1brc/data/1b.txt");
    let mut file = File::open(path).expect("failed to open file");

    let metadata = file.metadata().expect("Unable to read metadata");
    let file_size = metadata.len();

    // The reader starts reading further in the file the more threads are spawned
    file.seek(SeekFrom::Start((file_size / 16) * skip as u64)).expect("Failed to mover cursor");
    let reader = io::BufReader::new(file);    

    let mut skip_val: usize =  1;
    let mut take_val: usize =  take;

    // If it is the first thread, make sure that it starts at 0 and reads one extra line
    if skip == 0 {
        skip_val = 0;
        take_val += 1;
    }
    
    // The initial skip in nessicary to make sure that the reader does not start in the middel of a line
    // The .take() ensures that it stops reading once it has read its 1/16th of the file
    let iterator = reader.lines().skip(skip_val).take(take_val);

    let mut stations: StMap = HashMap::new();

    for line_result in iterator {
        let line = line_result.expect("Could not open line");

        let mut name = "".to_string();
        let mut number = 0.0;

        // This is slightly faster that using .split()
        let separator_index = line.find(';');
        if let Some(index) = separator_index {
            name = (&line[..index]).to_string();
            number = (&line[index+1..]).parse::<f32>().unwrap();
        }

        match stations.get_mut(&name) {
            // If the station is already in the hash map, update all values accordingly
            Some(station) => { 
                station.1 = station.1.min(number);
                station.2 = station.2.max(number);
                station.3 += number;
                station.4 += 1;
            },
            // If the station is not yet in the hash map, add it
            None => { 
                stations.insert(name.clone(), (name, number, number, number, 1));
            },
        }
    }
    return stations;
}

fn merge_maps(mut map1: StMap, mut map2: StMap, mut map3: StMap, mut map4: StMap) -> StMap {
    for (key, st1) in &mut map1 {
        // get the values from every other hash map
        let st2 = map2.get_mut(key.as_str()).unwrap();
        let st3 = map3.get_mut(key.as_str()).unwrap();
        let st4 = map4.get_mut(key.as_str()).unwrap();
        
        // Update values in the first map accordingly
        st1.1 = st1.1.max(st2.1).max(st3.1).max(st4.1);
        st1.2 = st1.2.min(st2.2).min(st3.2).min(st4.2);
        st1.3 = st1.3 + st2.3 + st3.3 + st4.3;
        st1.4 = st1.4 + st2.4 + st3.4 + st4.4;
    }
    return map1;
}