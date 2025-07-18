use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q18b(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let it = &db.it;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let n = &db.n;
    let t = &db.t;
    let ci = &db.ci;

    let start = Instant::now();

    let it1_s: HashSet<i32> = it
        .column("id")?
        .i32()?
        .into_iter()
        .zip(it.column("info")?.str()?.into_iter())
        .filter_map(|(id, info)| {
            if let (Some(id), Some(info)) = (id, info) {
                if info == "genres" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let mut mi_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (((id_opt, info_opt), it1_id_opt), note) in mi
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi.column("info")?.str()?.into_iter())
        .zip(mi.column("info_type_id")?.i32()?.into_iter())
        .zip(mi.column("note")?.str()?.into_iter())
    {
        if let (Some(id), Some(info), Some(it1_id), None) = (id_opt, info_opt, it1_id_opt, note) {
            if matches!(info, "Horror" | "Thriller") && it1_s.contains(&it1_id) {
                mi_m.entry(id).or_default().push(info);
            }
        }
    }

    let it2_s: HashSet<i32> = it
        .column("id")?
        .i32()?
        .into_iter()
        .zip(it.column("info")?.str()?.into_iter())
        .filter_map(|(id, info)| {
            if let (Some(id), Some(info)) = (id, info) {
                if info == "rating" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let mut mi_idx_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id_opt, info_opt), it2_id_opt) in mi_idx
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi_idx.column("info")?.str()?.into_iter())
        .zip(mi_idx.column("info_type_id")?.i32()?.into_iter())
    {
        if let (Some(id), Some(info), Some(it2_id)) = (id_opt, info_opt, it2_id_opt) {
            if info > "8.0" && it2_s.contains(&it2_id) {
                mi_idx_m.entry(id).or_default().push(info);
            }
        }
    }

    let mut t_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((movie_id, title), production_year) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .zip(t.column("production_year")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(title), Some(production_year)) =
            (movie_id, title, production_year)
        {
            if (2008..=2014).contains(&production_year)
                && mi_m.contains_key(&movie_id)
                && mi_idx_m.contains_key(&movie_id)
            {
                t_m.entry(movie_id).or_default().push(title);
            }
        }
    }

    let mut n_s: HashSet<i32> = HashSet::default();

    for (person_id, gender) in n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("gender")?.str()?.into_iter())
    {
        if let (Some(person_id), Some(gender)) = (person_id, gender) {
            if gender == "f" {
                n_s.insert(person_id);
            }
        }
    }

    let mut res: Option<(&str, &str, &str)> = None;

    for ((person_id, movie_id), note) in ci
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(db.ci.column("movie_id")?.i32()?.into_iter())
        .zip(db.ci.column("note")?.str()?.into_iter())
    {
        if let (Some(person_id), Some(movie_id), Some(note)) = (person_id, movie_id, note) {
            if matches!(
                note,
                "(writer)" | "(head writer)" | "(written by)" | "(story)" | "(story editor)"
            ) && n_s.contains(&person_id)
            {
                if let (Some(ts), Some(info), Some(xinfo)) = (
                    t_m.get(&movie_id),
                    mi_m.get(&movie_id),
                    mi_idx_m.get(&movie_id),
                ) {
                    for title in ts {
                        for i in info {
                            for x in xinfo {
                                if let Some((old_info, old_xinfo, old_title)) = res.as_mut() {
                                    if title < old_title {
                                        *old_title = title;
                                    }
                                    if i < old_info {
                                        *old_info = i;
                                    }
                                    if x < old_xinfo {
                                        *old_xinfo = x;
                                    }
                                } else {
                                    res = Some((i, x, title));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    dbg!(start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 18b
// SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(t.title) AS movie_title
// FROM cast_info AS ci,
// info_type AS it1,
// info_type AS it2,
// movie_info AS mi,
// movie_info_idx AS mi_idx,
// name AS n,
// title AS t
// WHERE ci.note in ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)')
// AND it1.info = 'genres'
// AND it2.info = 'rating'
// AND mi.info in ('Horror', 'Thriller')
// and mi.note is NULL
// AND mi_idx.info > '8.0'
// AND n.gender is not null
// and n.gender = 'f'
// AND t.production_year between 2008 and 2014
// AND t.id = mi.movie_id
// AND t.id = mi_idx.movie_id
// AND t.id = ci.movie_id
// AND ci.movie_id = mi.movie_id
// AND ci.movie_id = mi_idx.movie_id
// AND mi.movie_id = mi_idx.movie_id
// AND n.id = ci.person_id
// AND it1.id = mi.info_type_id
// AND it2.id = mi_idx.info_type_id;
#[cfg(test)]
mod test_18b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q18b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q18b(&db)?;
        assert_eq!(res, Some(("Horror", "8.1", "Agorable")));
        Ok(())
    }
}
