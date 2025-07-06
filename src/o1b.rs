use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

// imdb.q1b_movie_companies(movie_id,company_id,company_type_id)
// |imdb.q1b_title(movie_id,kind_id)
// ||imdb_int.movie_info_idx(movie_id,info_type_id)
// |||imdb.q1b_info_type(info_type_id)
// |imdb.q1b_company_type(company_type_id)
pub fn q1b(db: &ImdbData) -> Result<Option<(&str, &str, i32)>, PolarsError> {
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
        .zip(it.column("id")?.i32()?.into_iter())
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "bottom 10 rank" {
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
        .zip(mi_idx.column("info_type_id")?.i32()?.into_iter())
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

    let t_m: HashMap<i32, Vec<(&str, i32)>> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .zip(t.column("production_year")?.i32()?.into_iter())
        .filter_map(|((id, title), production_year)| {
            if let (Some(id), Some(title), Some(production_year)) = (id, title, production_year) {
                if mi_idx_s.contains(&id) && production_year >= 2005 && production_year <= 2010 {
                    Some((id, title, production_year))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(
            HashMap::default(),
            |mut acc, (id, title, production_year)| {
                acc.entry(id).or_default().push((title, production_year));
                acc
            },
        );

    let mut res: Option<(&str, &str, i32)> = None;

    for ((movie_id, company_type_id), note) in mc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_type_id")?.i32()?.into_iter())
        .zip(mc.column("note")?.str()?.into_iter())
    {
        if let (Some(note), Some(company_type_id), Some(movie_id)) =
            (note, company_type_id, movie_id)
        {
            if !note.contains("(as Metro-Goldwyn-Mayer Pictures)")
                && ct_s.contains(&company_type_id)
            {
                if let Some(tuples) = t_m.get(&movie_id) {
                    for (title, production_year) in tuples {
                        if let Some((old_note, old_title, old_production_year)) = res.as_mut() {
                            if *title < *old_title {
                                *old_title = title;
                            }
                            if *production_year < *old_production_year {
                                *old_production_year = *production_year;
                            }
                            if note < *old_note {
                                *old_note = note;
                            }
                        } else {
                            res = Some((note, title, *production_year));
                        }
                    }
                }
            }
        }
    }

    // let mc_m: HashMap<i32, Vec<&str>> = mc
    //     .column("note")?
    //     .str()?
    //     .into_iter()
    //     .zip(mc.column("company_type_id")?.i32()?.into_iter())
    //     .zip(mc.column("movie_id")?.i32()?.into_iter())
    //     .filter_map(|((note, company_type_id), movie_id)| {
    //         if let (Some(note), Some(company_type_id), Some(movie_id)) =
    //             (note, company_type_id, movie_id)
    //         {
    //             if !note.contains("(as Metro-Goldwyn-Mayer Pictures)")
    //                 && ct_s.contains(&company_type_id)
    //             {
    //                 Some((movie_id, note))
    //             } else {
    //                 None
    //             }
    //         } else {
    //             None
    //         }
    //     })
    //     .fold(HashMap::default(), |mut acc, (id, note)| {
    //         acc.entry(id).or_default().push(note);
    //         acc
    //     });

    let duration = start.elapsed();
    dbg!(duration);

    Ok(res)
}

// -- JOB 1b.sql
// SELECT MIN(mc.note) AS production_note,
// MIN(t.title) AS movie_title,
// MIN(t.production_year) AS movie_year
// FROM company_type AS ct,
// info_type AS it,
// movie_companies AS mc,
// movie_info_idx AS mi_idx,
// title AS t
// WHERE ct.kind = 'production companies'
// AND it.info = 'bottom 10 rank'
// AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
// AND t.production_year BETWEEN 2005 AND 2010
// AND ct.id = mc.company_type_id
// AND t.id = mc.movie_id
// AND t.id = mi_idx.movie_id
// AND mc.movie_id = mi_idx.movie_id
// AND it.id = mi_idx.info_type_id;
#[cfg(test)]
mod test_1b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q1b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q1b(&db)?;

        // The expected tuple should match the SQL SELECT order:
        // (production_note, movie_title, movie_year)
        let expected = Some((
            "(Set Decoration Rentals) (uncredited)",
            "Disaster Movie",
            2008,
        ));

        assert_eq!(res, expected);
        Ok(())
    }
}
