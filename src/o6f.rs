use crate::data::Data;
use ahash::{HashMap, HashMapExt};
use polars::prelude::*;
use std::time::Instant;

pub fn q6f(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let ci = &db.ci;
    let k = &db.k;
    let mk = &db.mk;
    let n = &db.n;
    let t = &db.t;

    let n_m: HashMap<i32, Vec<&str>> =
        n.id.iter()
            .zip(n.name.iter())
            .map(|(id, name)| (*id, name.as_str()))
            .fold(HashMap::new(), |mut acc, (id, name)| {
                acc.entry(id).or_insert_with(Vec::new).push(name);
                acc
            });

    let start = Instant::now();

    let target_keywords: ahash::HashSet<&str> = [
        "superhero",
        "sequel",
        "second-part",
        "marvel-comics",
        "based-on-comic",
        "tv-special",
        "fight",
        "violence",
    ]
    .into_iter()
    .collect();

    let k_m: HashMap<i32, &str> =
        k.id.iter()
            .zip(k.keyword.iter())
            .filter_map(|(id, keyword)| {
                target_keywords
                    .contains(keyword.as_str())
                    .then_some((*id, keyword.as_str()))
            })
            .collect();

    let mk_m: HashMap<i32, Vec<i32>> = mk
        .movie_id
        .iter()
        .zip(mk.keyword_id.iter())
        .filter_map(|(movie_id, keyword_id)| {
            (k_m.contains_key(&keyword_id)).then_some((*movie_id, *keyword_id))
        })
        .fold(HashMap::new(), |mut acc, (movie_id, keyword_id)| {
            acc.entry(movie_id)
                .or_insert_with(Vec::new)
                .push(keyword_id);
            acc
        });

    let t_m: HashMap<i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|((id, title), production_year)| {
                production_year
                    .filter(|&year| year > 2000 && mk_m.contains_key(&id))
                    .map(|_| (*id, title.as_str()))
            })
            .collect();

    let mut res: Option<(&str, &str, &str)> = None;

    for (pid, mid) in ci.person_id.iter().zip(ci.movie_id.iter()) {
        if let Some(title) = t_m.get(&mid)
            && let Some(kids) = mk_m.get(&mid)
            && let Some(names) = n_m.get(&pid)
        {
            for kid in kids {
                if let Some(keyword) = k_m.get(kid) {
                    res = match res {
                        Some((old_name, old_keyword, old_title)) => Some((
                            names.iter().min().unwrap().min(&old_name),
                            keyword.min(&old_keyword),
                            title.min(&old_title),
                        )),
                        None => Some((names.iter().min().unwrap(), keyword, title)),
                    };
                }
            }
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("6f,{duration:}");

    Ok(res)
}

// -- JOB Query 6f
// SELECT MIN(k.keyword) AS movie_keyword, MIN(n.name) AS actor_name, MIN(t.title) AS hero_movie
// FROM cast_info AS ci,
// keyword AS k,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE k.keyword in
// ('superhero', 'sequel', 'second-part', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence')
// AND t.production_year > 2000
// AND k.id = mk.keyword_id
// AND t.id = mk.movie_id
// AND t.id = ci.movie_id
// AND ci.movie_id = mk.movie_id
// AND n.id = ci.person_id;

#[cfg(test)]
mod test_q6f {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q6f() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q6f(&data)?;
        assert_eq!(
            res,
            Some((
                "\"Steff\", Stefanie Oxmann Mcgaha",
                "based-on-comic",
                "& Teller 2",
            ))
        );
        Ok(())
    }
}
