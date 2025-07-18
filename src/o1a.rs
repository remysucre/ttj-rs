use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;
use crate::data::ImdbData;

pub fn q1a(db: &ImdbData) -> Result<(), PolarsError> {
    let ct = &db.ct;
    let it = &db.it;
    let mc = &db.mc;
    let mi_idx = &db.mi_idx;
    let t = &db.t;

    let start = Instant::now();

    let ct_s: HashSet<i32> = ct
        .column("kind")?
        .str()?
        .into_iter()
        .zip(ct.column("id")?.i32()?.into_iter())
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if kind == "production companies" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let it_s: HashSet<i32> = it
        .column("info")?
        .str()?
        .into_iter()
        .zip(it.column("id")?.i32()?.into_iter())
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "top 250 rank" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mc_s: HashSet<i32> = mc
        .column("note")?
        .str()?
        .into_iter()
        .zip(mc.column("company_type_id")?.i32()?.into_iter())
        .zip(mc.column("movie_id")?.i32()?.into_iter())
        .filter_map(|((note, company_type_id), movie_id)| {
            if let (Some(note), Some(company_type_id), Some(movie_id)) = (note, company_type_id, movie_id) {
                if !note.contains("(as Metro-Goldwyn-Mayer Pictures)")
                    && (note.contains("(co-production)") || note.contains("(presents)"))
                    && ct_s.contains(&company_type_id)
                {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut mi_idx_s: HashSet<i32> = HashSet::default();

    for (x, y) in mi_idx
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi_idx.column("info_type_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(info_type_id)) = (x, y) {
            if it_s.contains(&info_type_id) {
                mi_idx_s.insert(movie_id);
            }
        }
    }

    let mut res = None;

    for ((x, y), z) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .zip(t.column("production_year")?.i32()?.into_iter())
    {
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

    // println!("{:}", res);

    let duration = start.elapsed().as_secs_f32();
    println!("{:}", duration);

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
