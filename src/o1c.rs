use crate::data::ImdbData;
// use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

// imdb.q1c_movie_companies(movie_id,company_id,company_type_id)
// |imdb.q1c_title(movie_id,kind_id)
// |imdb.q1c_company_type(company_type_id)
// |imdb_int.movie_info_idx(movie_id,info_type_id)
// ||imdb.q1c_info_type(info_type_id)
pub fn q1c(db: &ImdbData) -> Result<Option<(&str, &str, i32)>, PolarsError> {
    let ct = &db.ct;
    let it = &db.it;
    let mc = &db.mc;
    let mi_idx = &db.mi_idx;
    let t = &db.t;

    let start = Instant::now();

    let it_s: HashSet<i32> = it
        .column("info")?
        .str()?
        .into_iter()
        .zip(it.column("id")?.i32()?)
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

    let mi_idx_s: HashSet<i32> = mi_idx
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi_idx.column("info_type_id")?.i32()?)
        .filter_map(|(movie_id, info_type_id)| {
            if let (Some(movie_id), Some(info_type_id)) = (movie_id, info_type_id) {
                if it_s.contains(&info_type_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let ct_s: HashSet<i32> = ct
        .column("kind")?
        .str()?
        .into_iter()
        .zip(ct.column("id")?.i32()?)
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

    let mc_m: HashMap<i32, Vec<&str>> = mc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("note")?.str()?)
        .zip(mc.column("company_type_id")?.i32()?)
        .filter_map(|((movie_id, note), company_type_id)| {
            if let (Some(movie_id), Some(note), Some(company_type_id)) =
                (movie_id, note, company_type_id)
            {
                if ct_s.contains(&company_type_id)
                    && mi_idx_s.contains(&movie_id)
                    && !note.contains("(as Metro-Goldwyn-Mayer Pictures)")
                    && note.contains("(co-production)")
                {
                    Some((movie_id, note))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (movie_id, note)| {
            acc.entry(movie_id).or_default().push(note);
            acc
        });

    let mut res: Option<(&str, &str, i32)> = None;

    for ((id, title), production_year) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?)
        .zip(t.column("production_year")?.i32()?.into_iter())
    {
        if let (Some(id), Some(title), Some(production_year)) = (id, title, production_year) {
            if production_year > 2010
                && let Some(notes) = mc_m.get(&id)
            {
                for note in notes {
                    if let Some((old_note, old_title, old_production_year)) = res.as_mut() {
                        if title < *old_title {
                            *old_title = title;
                        }
                        if production_year < *old_production_year {
                            *old_production_year = production_year;
                        }
                        if note < old_note {
                            *old_note = note;
                        }
                    } else {
                        res = Some((note, title, production_year));
                    }
                }
            }
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("{duration:}");

    Ok(res)
}

// 1c.sql
// SELECT MIN(mc.note) AS production_note,
// MIN(t.title) AS movie_title,
// MIN(t.production_year) AS movie_year
// FROM company_type AS ct,
// info_type AS it,
// movie_companies AS mc,
// movie_info_idx AS mi_idx,
// title AS t
// WHERE ct.kind = 'production companies'
// AND it.info = 'top 250 rank'
// AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
// AND (mc.note LIKE '%(co-production)%')
// AND t.production_year >2010
// AND ct.id = mc.company_type_id
// AND t.id = mc.movie_id
// AND t.id = mi_idx.movie_id
// AND mc.movie_id = mi_idx.movie_id
// AND it.id = mi_idx.info_type_id;
#[cfg(test)]
mod test_1c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q1c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        assert_eq!(q1c(&db)?, Some(("(co-production)", "Intouchables", 2011)));
        Ok(())
    }
}
