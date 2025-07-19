use crate::data;
use ahash::HashMap;
use polars::prelude::{col, lit, IntoLazy, LazyFrame};
use std::env;
use std::time::Instant;

/// Compare the performance of push down selection predicate when configure
/// Polars into single-thread.
/// Polars is configured to use 1 threads.
//  Elapsed: 147.95ms
//  Not push down: {414712: "Downey Sr., Robert", 414711: "Downey Jr., Robert"}
//  Elapsed: 172.04ms
//  Push down: {414711: "Downey Jr., Robert", 414712: "Downey Sr., Robert"}
//  Elapsed: 281.54ms
//  Push down2: {414711: "Downey Jr., Robert", 414712: "Downey Sr., Robert"}
pub fn test_read() -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        env::set_var("POLARS_MAX_THREADS", "1");
    }
    let thread_count = &polars_core::POOL.current_num_threads();
    println!("Polars is configured to use {} threads.", thread_count);

    let db = data::ImdbData::new();

    let n = &db.n;
    let now = Instant::now();
    let n_m: HashMap<i32, &str> = n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?)
        .filter_map(|(id, name)| {
            if let (Some(id), Some(name)) = (id, name) {
                if name.contains("Downey") && name.contains("Robert") {
                    Some((id, name))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();
    println!("Elapsed: {:.2?}", now.elapsed());
    println!("Not push down: {:?}", n_m);

    let now2 = Instant::now();
    let filtered_df = n
        .clone()
        .lazy()
        .filter(
            col("name")
                .str()
                .contains(lit("Downey"), true)
                .and(col("name").str().contains(lit("Robert"), true)),
        )
        .collect()?;

    let n_m: HashMap<i32, &str> = filtered_df
        .column("id")?
        .i32()?
        .into_iter()
        .zip(filtered_df.column("name")?.str()?)
        .filter_map(|(id, name)| {
            if let (Some(id), Some(name)) = (id, name) {
                Some((id, name))
            } else {
                None
            }
        })
        .collect();
    println!("Elapsed: {:.2?}", now2.elapsed());
    println!("Push down: {:?}", n_m);

    let predicate = col("name")
        .str()
        .contains_literal(lit("Downey"))
        .and(col("name").str().contains_literal(lit("Robert")));

    let now3 = Instant::now();
    let filtered_df = LazyFrame::scan_parquet(
        "imdb/name.parquet", // File path
        Default::default(),  // Scan options
    )?
    .filter(predicate)
    .collect()?;

    let n_m: HashMap<i32, &str> = filtered_df
        .column("id")?
        .i32()?
        .into_iter()
        .zip(filtered_df.column("name")?.str()?)
        .filter_map(|(id, name)| {
            if let (Some(id), Some(name)) = (id, name) {
                Some((id, name))
            } else {
                None
            }
        })
        .collect();
    println!("Elapsed: {:.2?}", now3.elapsed());
    println!("Push down2: {:?}", n_m);

    Ok(())
}
