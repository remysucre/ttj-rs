use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q18a(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let it = &db.it;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let n = &db.n;
    let t = &db.t;
    let ci = &db.ci;

    let tim = Finder::new("Tim");

    let start = Instant::now();

    let mut it1_id: &i32 = &0;
    let mut it2_id: &i32 = &0;

    for (id, info) in it.id.iter().zip(it.info.iter()) {
        if info == "budget" {
            it1_id = id;
        } else if info == "votes" {
            it2_id = id;
        }
    }

    let mi_m: HashMap<&i32, Vec<&str>> = mi
        .movie_id
        .iter()
        .zip(mi.info.iter())
        .zip(mi.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            (it1_id == info_type_id).then_some((movie_id, info))
        })
        .fold(HashMap::default(), |mut acc, (movie_id, info)| {
            acc.entry(movie_id).or_default().push(info);
            acc
        });

    let mi_idx_m: ahash::HashMap<&i32, Vec<&str>> = mi_idx
        .movie_id
        .iter()
        .zip(mi_idx.info.iter())
        .zip(mi_idx.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            (it2_id == info_type_id).then_some((movie_id, info))
        })
        .fold(ahash::HashMap::default(), |mut acc, (idx, info)| {
            acc.entry(idx).or_insert_with(Vec::new).push(info);
            acc
        });

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .filter_map(|(movie_id, title)| {
                if mi_m.contains_key(&movie_id) && mi_idx_m.contains_key(&movie_id) {
                    Some((movie_id, title.as_str()))
                } else {
                    None
                }
            })
            .collect();

    let n_s: HashSet<&i32> =
        n.id.iter()
            .zip(n.name.iter())
            .zip(n.gender.iter())
            .filter_map(|((id, name), gender)| {
                if let Some(gender) = gender
                    && gender == "m"
                    && tim.find(name.as_bytes()).is_some()
                {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();

    let target_note: ahash::HashSet<&str> =
        ["(producer)", "(executive producer)"].into_iter().collect();

    let mut res: Option<(&str, &str, &str)> = None;

    for ((person_id, movie_id), note) in ci
        .person_id
        .iter()
        .zip(ci.movie_id.iter())
        .zip(ci.note.iter())
    {
        if let Some(note) = note
            && target_note.contains(note.as_str())
            && n_s.contains(&person_id)
            && let Some(title) = t_m.get(&movie_id)
            && let Some(info) = mi_m.get(&movie_id)
            && let Some(xinfo) = mi_idx_m.get(&movie_id)
        {
            res = match res {
                Some((old_info, old_xinfo, old_title)) => Some((
                    info.iter().min().unwrap().min(&old_info),
                    xinfo.iter().min().unwrap().min(&old_xinfo),
                    title.min(&old_title),
                )),
                None => Some((
                    info.iter().min().unwrap(),
                    xinfo.iter().min().unwrap(),
                    title,
                )),
            };
        }
    }

    println!("18a,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 18a
// SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(t.title) AS movie_title
// FROM cast_info AS ci,
// info_type AS it1,
// info_type AS it2,
// movie_info AS mi,
// movie_info_idx AS mi_idx,
// name AS n,
// title AS t
// WHERE ci.note in ('(producer)', '(executive producer)')
// AND it1.info = 'budget'
// AND it2.info = 'votes'
// AND n.gender = 'm'
// and n.name like '%Tim%'
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
mod test_q18a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q18a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q18a(&data)?;
        assert_eq!(res, Some(("$1,000", "10", "40 Days and 40 Nights")));
        Ok(())
    }
}
