use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;
use crate::data::ImdbData;

pub fn q25c(db: &ImdbData) -> Result<Option<(&str, &str, &str, &str)>, PolarsError> {

    let ci = &db.ci;
    let it = &db.it;
    let k = &db.k;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let mk = &db.mk;
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

    let k_s: HashSet<i32> = k
        .column("id")?
        .i32()?
        .into_iter()
        .zip(k.column("keyword")?.str()?)
        .filter_map(|(id, keyword)| {
            if let (Some(id), Some(keyword)) = (id, keyword) {
                if matches!(
                    keyword,
                    "murder"
                        | "violence"
                        | "blood"
                        | "gore"
                        | "death"
                        | "female-nudity"
                        | "hospital"
                ) {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut mk_s: HashSet<i32> = HashSet::default();

    for (movie_id, keyword_id) in mk
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("keyword_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(keyword_id)) = (movie_id, keyword_id) {
            if k_s.contains(&keyword_id) {
                mk_s.insert(movie_id);
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
            if mk_s.contains(&movie_id)
                && mi_m.contains_key(&movie_id)
                && mi_idx_m.contains_key(&movie_id)
            {
                t_m.entry(movie_id).or_default().push(title);
            }
        }
    }

    let mut n_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((person_id, name), gender) in n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?.into_iter())
        .zip(n.column("gender")?.str()?.into_iter())
    {
        if let (Some(person_id), Some(name), Some(gender)) = (person_id, name, gender) {
            if gender == "m" {
                n_m.entry(person_id).or_default().push(name);
            }
        }
    }

    let mut res: Option<(&str, &str, &str, &str)> = None;

    for ((person_id, movie_id), note) in ci
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("movie_id")?.i32()?.into_iter())
        .zip(ci.column("note")?.str()?.into_iter())
    {
        if let (Some(person_id), Some(movie_id), Some(note)) = (person_id, movie_id, note) {
            if matches!(
                note,
                "(writer)" | "(head writer)" | "(written by)" | "(story)" | "(story editor)"
            ) {
                if let Some(ts) = t_m.get(&movie_id) {
                    if let Some(names) = n_m.get(&person_id) {
                        if let Some(info) = mi_m.get(&movie_id) {
                            if let Some(xinfo) = mi_idx_m.get(&movie_id) {
                                for name in names {
                                    for title in ts {
                                        for i in info {
                                            for x in xinfo {
                                                if let Some((old_name, old_title, old_info, old_xinfo)) =
                                                    res.as_mut()
                                                {
                                                    if name < old_name {
                                                        *old_name = name;
                                                    }
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
                                                    res = Some((name, title, i, x));
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
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("{duration:}");

    Ok(res)
}

// -- JOB Query 25c
// SELECT MIN(mi.info)     AS movie_budget,
// MIN(mi_idx.info) AS movie_votes,
// MIN(n.name)      AS male_writer,
// MIN(t.title)     AS violent_movie_title
// FROM cast_info AS ci,
// info_type AS it1,
// info_type AS it2,
// keyword AS k,
// movie_info AS mi,
// movie_info_idx AS mi_idx,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE ci.note in ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)')
// AND it1.info = 'genres'
// AND it2.info = 'votes'
// AND k.keyword in ('murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital')
// AND mi.info in ('Horror', 'Action', 'Sci-Fi', 'Thriller', 'Crime', 'War')
// AND n.gender = 'm'
// AND t.id = mi.movie_id
// AND t.id = mi_idx.movie_id
// AND t.id = ci.movie_id
// AND t.id = mk.movie_id
// AND ci.movie_id = mi.movie_id
// AND ci.movie_id = mi_idx.movie_id
// AND ci.movie_id = mk.movie_id
// AND mi.movie_id = mi_idx.movie_id
// AND mi.movie_id = mk.movie_id
// AND mi_idx.movie_id = mk.movie_id
// AND n.id = ci.person_id
// AND it1.id = mi.info_type_id
// AND it2.id = mi_idx.info_type_id
// AND k.id = mk.keyword_id;

#[cfg(test)]
mod test_q25c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q25c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q25c(&db)?;

        let expected = Some(("Aakeson, Kim Fupz", "$", "Action", "10"));

        assert_eq!(res, expected);
        Ok(())
    }
}
