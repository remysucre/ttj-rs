use crate::data::Data;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q25c(db: &Data) -> Result<Option<(&str, &str, &str, &str)>, PolarsError> {
    let ci = &db.ci;
    let it = &db.it;
    let k = &db.k;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let mk = &db.mk;
    let n = &db.n;
    let t = &db.t;

    let start = Instant::now();

    let mut it_id: i32 = 0;
    let mut it2_id: i32 = 0;
    for (info, id) in it.info.iter().zip(it.id.iter()) {
        if info == &"genres" {
            it_id = *id;
        } else if info == "votes" {
            it2_id = *id;
        }
    }

    let target_info: HashSet<&str> = ["Horror", "Action", "Sci-Fi", "Thriller", "Crime", "War"]
        .into_iter()
        .collect();

    let mi_m: HashMap<i32, Vec<&str>> = mi
        .movie_id
        .iter()
        .zip(mi.info.iter())
        .zip(mi.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            (it_id == *info_type_id && target_info.contains(info.as_str()))
                .then_some((*movie_id, info.as_str()))
        })
        .fold(HashMap::default(), |mut acc, (movie_id, info)| {
            acc.entry(movie_id).or_default().push(info);
            acc
        });

    let mi_idx_m: HashMap<i32, Vec<&str>> = mi_idx
        .movie_id
        .iter()
        .zip(mi_idx.info.iter())
        .zip(mi_idx.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            (it2_id == *info_type_id && mi_m.contains_key(movie_id))
                .then_some((*movie_id, info.as_str()))
        })
        .fold(HashMap::default(), |mut acc, (movie_id, info)| {
            acc.entry(movie_id).or_default().push(info);
            acc
        });

    let target_keywords: HashSet<&str> = [
        "murder",
        "violence",
        "blood",
        "gore",
        "death",
        "female-nudity",
        "hospital",
    ]
    .into_iter()
    .collect();

    let k_s: HashSet<i32> =
        k.id.iter()
            .zip(k.keyword.iter())
            .filter_map(|(id, keyword)| target_keywords.contains(keyword.as_str()).then_some(*id))
            .collect();

    let mk_s: HashSet<i32> = mk
        .movie_id
        .iter()
        .zip(mk.keyword_id.iter())
        .filter_map(|(movie_id, keyword_id)| {
            (mi_idx_m.contains_key(movie_id) && k_s.contains(keyword_id)).then_some(*movie_id)
        })
        .collect();

    let t_m: HashMap<&i32, Vec<&str>> =
        t.id.iter()
            .zip(t.title.iter())
            .filter_map(|(id, title)| {
                (mk_s.contains(&id) && mi_m.contains_key(&id) && mi_idx_m.contains_key(&id))
                    .then_some((id, title))
            })
            .fold(HashMap::default(), |mut acc, (id, title)| {
                acc.entry(id).or_default().push(title);
                acc
            });

    let n_m: HashMap<i32, Vec<&str>> =
        n.id.iter()
            .zip(n.gender.iter())
            .zip(n.name.iter())
            .filter_map(|((id, gender), name)| {
                gender
                    .as_ref()
                    .filter(|gender| gender == &"m")
                    .map(|_| (*id, name))
            })
            .fold(HashMap::default(), |mut acc, (id, name)| {
                acc.entry(id).or_default().push(name);
                acc
            });

    let target_note: HashSet<&str> = [
        "(writer)",
        "(head writer)",
        "(written by)",
        "(story)",
        "(story editor)",
    ]
    .into_iter()
    .collect();

    let mut res: Option<(&str, &str, &str, &str)> = None;

    for ((person_id, movie_id), note) in ci
        .person_id
        .iter()
        .zip(ci.movie_id.iter())
        .zip(ci.note.iter())
    {
        if let Some(note) = note
            && target_note.contains(note.as_str())
            && let Some(mi_info) = mi_m.get(&movie_id)
            && mk_s.contains(&movie_id)
            && let Some(mi_idx_info) = mi_idx_m.get(&movie_id)
            && let Some(names) = n_m.get(&person_id)
            && let Some(titles) = t_m.get(&movie_id)
        {
            res = match res {
                Some((old_mi_info, old_mi_idx_info, old_names, old_titles)) => Some((
                    mi_info.iter().min().unwrap().min(&old_mi_info),
                    mi_idx_info.iter().min().unwrap().min(&old_mi_idx_info),
                    names.iter().min().unwrap().min(&old_names),
                    titles.iter().min().unwrap().min(&old_titles),
                )),
                None => Some((
                    mi_info.iter().min().unwrap(),
                    mi_idx_info.iter().min().unwrap(),
                    names.iter().min().unwrap(),
                    titles.iter().min().unwrap(),
                )),
            };
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("25c,{duration:}");

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
        let data = Data::new(&db);
        let res = q25c(&data)?;
        assert_eq!(res, Some(("Action", "10", "Aakeson, Kim Fupz", "$")));
        Ok(())
    }
}
