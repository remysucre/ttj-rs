use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q18c(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let it = &db.it;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let n = &db.n;
    let t = &db.t;

    let start = Instant::now();

    let it1_s: HashSet<i32> = it
        .column("id")?
        .i32()?
        .into_iter()
        .zip(it.column("info")?.str()?)
        .filter_map(|(id, info)| {
            if let (Some(id), Some(info)) = (id, info) {
                if info == "genres" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let mut mi_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id_opt, info_opt), it1_id_opt) in mi
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi.column("info")?.str()?.into_iter())
        .zip(mi.column("info_type_id")?.i32()?.into_iter())
    {
        if let (Some(id), Some(info), Some(it1_id)) = (id_opt, info_opt, it1_id_opt) {
            if matches!(
                info,
                "Horror" | "Action" | "Sci-Fi" | "Thriller" | "Crime" | "War"
            ) && it1_s.contains(&it1_id)
            {
                mi_m.entry(id).or_default().push(info);
            }
        }
    }

    let it2_s: HashSet<i32> = it
        .column("id")?
        .i32()?
        .into_iter()
        .zip(it.column("info")?.str()?)
        .filter_map(|(id, info)| {
            if let (Some(id), Some(info)) = (id, info) {
                if info == "votes" { Some(id) } else { None }
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
            if it2_s.contains(&it2_id) {
                mi_idx_m.entry(id).or_default().push(info);
            }
        }
    }

    let mut t_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (movie_id, title) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
    {
        if let (Some(movie_id), Some(title)) = (movie_id, title) {
            if mi_m.contains_key(&movie_id) && mi_idx_m.contains_key(&movie_id) {
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
            if gender == "m" {
                n_s.insert(person_id);
            }
        }
    }

    let mut res: Option<(&str, &str, &str)> = None;

    for ((person_id, movie_id), note) in db
        .ci
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
            ) {
                if let Some(ts) = t_m.get(&movie_id) {
                    if n_s.contains(&person_id) {
                        if let (Some(info), Some(xinfo)) =
                            (mi_m.get(&movie_id), mi_idx_m.get(&movie_id))
                        {
                            for title in ts {
                                for i in info {
                                    for x in xinfo {
                                        if let Some((old_info, old_xinfo, old_title)) = res.as_mut()
                                        {
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
        }
    }

    println!("18c,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 18c
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
// AND it2.info = 'votes'
// AND mi.info in ('Horror', 'Action', 'Sci-Fi', 'Thriller', 'Crime', 'War')
// AND n.gender = 'm'
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
mod test_18c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q18c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q18c(&db)?;
        assert_eq!(res, Some(("Action", "10", "#PostModem")));
        Ok(())
    }
}
