use crate::data;
use ahash::HashMap;
use polars::prelude::{IntoLazy, LazyFrame, ScanArgsParquet, col, lit};
use std::env;
use std::time::Instant;

/// Compare the performance of push down selection predicate when configure
/// Polars into single-thread.
pub fn test_read() -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        env::set_var("POLARS_MAX_THREADS", "1");
    }
    let thread_count = &polars_core::POOL.current_num_threads();
    println!("Polars is configured to use {} threads.", thread_count);

    test1()?;
    test2()?;
    test3()?;

    Ok(())
}

// Test 1: Vanilla filter in-memory
pub fn test1() -> Result<(), Box<dyn std::error::Error>> {
    let db = data::ImdbData::new();
    let n = &db.n;

    let now = Instant::now();
    let n_m: HashMap<i32, &str> = n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?)
        .filter_map(|(id, name)| {
            let (id, name) = (id?, name?);
            if name.contains("Downey") && name.contains("Robert") {
                Some((id, name))
            } else {
                None
            }
        })
        .collect();
    println!("Elapsed: {:.2?}", now.elapsed());
    println!("Not push down: {:?}", n_m);
    Ok(())
}

// Test 2: Using Polars API to filter in-memory
pub fn test2() -> Result<(), Box<dyn std::error::Error>> {
    let db = data::ImdbData::new();
    let thread_count = &polars_core::POOL.current_num_threads();
    println!("Polars is configured to use {} threads.", thread_count);

    let now2 = Instant::now();
    let filtered_df =
        db.n.lazy()
            .filter(
                col("name")
                    .str()
                    .contains(lit("Downey"), true)
                    .and(col("name").str().contains(lit("Robert"), true)),
            )
            .select([col("id"), col("name")]) // Only select needed columns
            .collect()?;

    let n_m: HashMap<i32, &str> = filtered_df
        .column("id")?
        .i32()?
        .into_iter()
        .zip(filtered_df.column("name")?.str()?)
        .filter_map(|(id, name)| Some((id?, name?)))
        .collect();
    println!("Elapsed: {:.2?}", now2.elapsed());
    println!("Push down: {:?}", n_m);
    Ok(())
}

// Test 3: filter data during read parquet
pub fn test3() -> Result<(), Box<dyn std::error::Error>> {
    let thread_count = &polars_core::POOL.current_num_threads();
    println!("Polars is configured to use {} threads.", thread_count);
    let now3 = Instant::now();
    let filtered_df = LazyFrame::scan_parquet("imdb/name.parquet", ScanArgsParquet::default())?
        .filter(
            col("name")
                .str()
                .contains(lit("Downey"), true)
                .and(col("name").str().contains(lit("Robert"), true)),
        )
        .select([col("id"), col("name")]) // Only read needed columns from disk
        .collect()?;

    let n_m: HashMap<i32, &str> = filtered_df
        .column("id")?
        .i32()?
        .into_iter()
        .zip(filtered_df.column("name")?.str()?)
        .filter_map(|(id, name)| Some((id?, name?)))
        .collect();
    println!("Elapsed: {:.2?}", now3.elapsed());
    println!("Push down2: {:?}", n_m);
    Ok(())
}
