use polars::prelude::*;
use std::{collections::HashSet, time::Instant, vec};

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

    let it_s: HashSet<i32> = it.column("id").unwrap().i32().unwrap().into_iter().flatten().collect();
    let ct_s: HashSet<i32> = ct.column("id").unwrap().i32().unwrap().into_iter().flatten().collect();

    dbg!(&it_s);
    dbg!(&ct_s);

    let mut mi_idx_s: HashSet<i32> = HashSet::new();

    // iterate over the movie_id and info_type_id columns of mi_idx
    // if info_type_id is in it_s, add movie_id to mi_idx_s

    for (x, y) in mi_idx.column("movie_id").unwrap().i32().unwrap().into_iter()
        .zip(mi_idx.column("info_type_id").unwrap().i32().unwrap().into_iter()) {
        if let (Some(movie_id), Some(info_type_id)) = (x, y) {
            if it_s.contains(&info_type_id) {
                mi_idx_s.insert(movie_id);
            }
        }
    }

    let mut mc_s: HashSet<i32> = HashSet::new();

    // iterate over the company_type_id and movie_id columns of mc
    // if company_type_id is in ct_s, add movie_id to mc_s

    for (x, y) in mc.column("company_type_id").unwrap().i32().unwrap().into_iter()
        .zip(mc.column("movie_id").unwrap().i32().unwrap().into_iter()) {
        if let (Some(company_type_id), Some(movie_id)) = (x, y) {
            if ct_s.contains(&company_type_id) {
                mc_s.insert(movie_id);
            }
        }
    }

    let mut res = None;

    // iterate over the id, title, and production year columns of t
    // if id is in both mi_idx_s and mc_s, replace res with the minimum of the old res and the new title and production year

    for ((x, y), z) in t.column("id").unwrap().i32().unwrap().into_iter()
        .zip(t.column("title").unwrap().str().unwrap().into_iter())
        .zip(t.column("production_year").unwrap().i32().unwrap().into_iter()) {
        if let (Some(id), Some(title), Some(production_year)) = (x, y, z) {
            if mi_idx_s.contains(&id) && mc_s.contains(&id) {
                let new_res = (title.to_string(), production_year);
                if let Some((old_title, old_year)) = res.as_ref() {
                    let new_year = std::cmp::min(*old_year, production_year);
                    let title_string = title.to_string();
                    let new_title: &String  = std::cmp::min(old_title, &title_string);
                    res = Some((new_title.to_string(), new_year));
                } else {
                    res = Some(new_res);
                }
            }
        }
    }

    dbg!(res);

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