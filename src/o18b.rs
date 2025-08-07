use crate::data::Data;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q18b(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let it = &db.it;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let n = &db.n;
    let t = &db.t;
    let ci = &db.ci;

    let start = Instant::now();

    let mut it1_id: &i32 = &0;
    let mut it2_id: &i32 = &0;

    for (id, info) in it.id.iter().zip(it.info.iter()) {
        if info == "genres" {
            it1_id = id;
        } else if info == "rating" {
            it2_id = id;
        }
    }

    let target_info: ahash::HashSet<&str> = ["Horror", "Thriller"].into_iter().collect();

    let mi_m: HashMap<&i32, Vec<&str>> = mi
        .movie_id
        .iter()
        .zip(mi.info.iter())
        .zip(mi.info_type_id.iter())
        .zip(mi.note.iter())
        .filter_map(|(((movie_id, info), info_type_id), note)| {
            (it1_id == info_type_id && note.is_none() && target_info.contains(info.as_str()))
                .then_some((movie_id, info))
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
            (it2_id == info_type_id && info.as_str() > "8.0").then_some((movie_id, info))
        })
        .fold(ahash::HashMap::default(), |mut acc, (idx, info)| {
            acc.entry(idx).or_insert_with(Vec::new).push(info);
            acc
        });

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|((movie_id, title), production_year)| {
                if let Some(production_year) = production_year
                    && (2008..=2014).contains(production_year)
                    && mi_m.contains_key(&movie_id)
                    && mi_idx_m.contains_key(&movie_id)
                {
                    Some((movie_id, title.as_str()))
                } else {
                    None
                }
            })
            .collect();

    let n_s: HashSet<&i32> =
        n.id.iter()
            .zip(n.gender.iter())
            .filter_map(|(id, gender)| {
                if let Some(gender) = gender
                    && gender == "f"
                {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();

    let target_note: ahash::HashSet<&str> = [
        "(writer)",
        "(head writer)",
        "(written by)",
        "(story)",
        "(story editor)",
    ]
    .into_iter()
    .collect();

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

    println!("18b,{:}", start.elapsed().as_secs_f32());

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
mod test_q18b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q18b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q18b(&data)?;
        assert_eq!(res, Some(("Horror", "8.1", "Agorable")));
        Ok(())
    }
}
