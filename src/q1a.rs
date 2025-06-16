use polars::prelude::*;
use std::time::Instant;

pub fn q1a() {
    let ct = LazyFrame::scan_parquet("imdb/company_type.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let it = LazyFrame::scan_parquet("imdb/info_type.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let mc = LazyFrame::scan_parquet("imdb/movie_companies.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let mi_idx = LazyFrame::scan_parquet("imdb/movie_info_idx.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let t = LazyFrame::scan_parquet("imdb/title.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();

    let start = Instant::now();

    let ct = ct
        .lazy()
        .filter(col("kind").eq(lit("production companies")))
        .collect()
        .unwrap();
    let it = it
        .lazy()
        .filter(col("info").eq(lit("top 250 rank")))
        .collect()
        .unwrap();
    let mc = mc
        .lazy()
        .filter(
            col("note")
                .str()
                .contains(lit("(as Metro-Goldwyn-Mayer Pictures)"), false)
                .not(),
        )
        .filter(
            col("note")
                .str()
                .contains(lit("(co-production)"), false)
                .or(col("note").str().contains(lit("(presents)"), false)),
        )
        .collect()
        .unwrap();

    let res = ct
        .lazy()
        .join_builder()
        .with(mc.lazy())
        .left_on([col("id")])
        .right_on([col("company_type_id")])
        .suffix("_mc")
        .finish()
        .join_builder()
        .with(t.lazy())
        .left_on([col("movie_id")])
        .right_on([col("id")])
        .suffix("_t")
        .finish()
        .join_builder()
        .with(mi_idx.lazy())
        .left_on([col("movie_id")])
        .right_on([col("movie_id")])
        .suffix("_mi_idx")
        .finish()
        .join_builder()
        .with(it.lazy())
        .left_on([col("info_type_id")])
        .right_on([col("id")])
        .suffix("_it")
        .finish()
        .select([
            col("note").min().alias("production_note"),
            col("title").min().alias("movie_title"),
            col("production_year").min().alias("movie_year"),
        ])
        .collect()
        .unwrap();

    println!("{:?}", res);

    let duration = start.elapsed();
    dbg!(duration);
}

// 1a.sql
// SELECT MIN(mc.note) AS production_note,
//        MIN(t.title) AS movie_title,
//        MIN(t.production_year) AS movie_year
// FROM company_type AS ct,
//      info_type AS it,
//      movie_companies AS mc,
//      movie_info_idx AS mi_idx,
//      title AS t
// WHERE ct.kind = 'production companies'
//   AND it.info = 'top 250 rank'
//   AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
//   AND (mc.note LIKE '%(co-production)%'
//        OR mc.note LIKE '%(presents)%')
//   AND ct.id = mc.company_type_id
//   AND t.id = mc.movie_id
//   AND t.id = mi_idx.movie_id
//   AND mc.movie_id = mi_idx.movie_id
//   AND it.id = mi_idx.info_type_id; 