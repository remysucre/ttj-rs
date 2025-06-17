use polars::prelude::*;
use std::{collections::HashSet, time::Instant, vec};

pub fn q1a() -> Result<(), PolarsError> {
    let ct = LazyFrame::scan_parquet("imdb/company_type.parquet", Default::default())?
        .collect()?;
    let it = LazyFrame::scan_parquet("imdb/info_type.parquet", Default::default())?
        .collect()?;
    let mc = LazyFrame::scan_parquet("imdb/movie_companies.parquet", Default::default())?
        .collect()?;
    let mi_idx = LazyFrame::scan_parquet("imdb/movie_info_idx.parquet", Default::default())?
        .collect()?;
    let t = LazyFrame::scan_parquet("imdb/title.parquet", Default::default())?
        .collect()?;

    let start = Instant::now();

    let ct = ct
        .lazy()
        .filter(col("kind").eq(lit("production companies")))
        .collect()?;
    let it = it
        .lazy()
        .filter(col("info").eq(lit("top 250 rank")))
        .collect()?;
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
        .collect()?;

    let it_s: HashSet<i32> = it.column("id")?.i32()?.into_iter().flatten().collect();
    let ct_s: HashSet<i32> = ct.column("id")?.i32()?.into_iter().flatten().collect();

    dbg!(&it_s);
    dbg!(&ct_s);

    let mut mi_idx_s: HashSet<i32> = HashSet::new();

    for (x, y) in mi_idx.column("movie_id")?.i32()?.into_iter()
        .zip(mi_idx.column("info_type_id")?.i32()?.into_iter()) {
        if let (Some(movie_id), Some(info_type_id)) = (x, y) {
            if it_s.contains(&info_type_id) {
                mi_idx_s.insert(movie_id);
            }
        }
    }

    let mut mc_s: HashSet<i32> = HashSet::new();

    for (x, y) in mc.column("company_type_id")?.i32()?.into_iter()
        .zip(mc.column("movie_id")?.i32()?.into_iter()) {
        if let (Some(company_type_id), Some(movie_id)) = (x, y) {
            if ct_s.contains(&company_type_id) {
                mc_s.insert(movie_id);
            }
        }
    }

    let mut res = None;

    for ((x, y), z) in t.column("id")?.i32()?.into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .zip(t.column("production_year")?.i32()?.into_iter()) {
        if let (Some(id), Some(title), Some(production_year)) = (x, y, z) {
            if mi_idx_s.contains(&id) && mc_s.contains(&id) {
                if let Some((old_title, old_year)) = res.as_mut() {
                    if production_year < *old_year {
                        *old_year = production_year;
                    } 
                    if title < *old_title {
                        *old_title = title;
                    }
                } else {
                    res = Some((title, production_year));
                }
            }
        }
    }

    dbg!(res);

    let duration = start.elapsed();
    dbg!(duration);

    Ok(())
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