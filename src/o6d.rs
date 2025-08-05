use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q6d(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let ci = &db.ci;
    let k = &db.k;
    let mk = &db.mk;
    let n = &db.n;
    let t = &db.t;

    let downey = Finder::new("Downey");
    let robert = Finder::new("Robert");

    let start = Instant::now();

    let n_m: HashMap<i32, &str> =
        n.id.iter()
            .zip(n.name.iter())
            .filter_map(|(id, name)| {
                (downey.find(name.as_bytes()).is_some() && robert.find(name.as_bytes()).is_some())
                    .then_some((*id, name.as_str()))
            })
            .collect();

    let target_keywords: HashSet<&str> = [
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

    let mk_m: HashMap<i32, Vec<&str>> = mk
        .movie_id
        .iter()
        .zip(mk.keyword_id.iter())
        .filter_map(|(movie_id, keyword_id)| {
            if let Some(keyword) = k_m.get(keyword_id) {
                Some((*movie_id, *keyword))
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (movie_id, keyword)| {
            acc.entry(movie_id).or_default().push(keyword);
            acc
        });

    let t_m: HashMap<i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|((id, title), production_year)| {
                if let Some(production_year) = production_year
                    && production_year > &2000
                    && mk_m.contains_key(&id)
                {
                    Some((*id, title.as_str()))
                } else {
                    None
                }
            })
            .collect();

    let mut res: Option<(&str, &str, &str)> = None;

    for (pid, mid) in ci.person_id.iter().zip(ci.movie_id.iter()) {
        if let Some(title) = t_m.get(&mid)
            && let Some(name) = n_m.get(&pid)
            && let Some(keywords) = mk_m.get(&mid)
        {
            res = match res {
                Some((old_keyword, old_name, old_title)) => Some((
                    keywords.iter().min().unwrap().min(&old_keyword),
                    name.min(&old_name),
                    title.min(&old_title),
                )),
                None => Some((keywords.iter().min().unwrap(), name, title)),
            };
        }
    }

    println!("6d,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 6d
// SELECT MIN(k.keyword) AS movie_keyword, MIN(n.name) AS actor_name, MIN(t.title) AS hero_movie
// FROM cast_info AS ci,
// keyword AS k,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE k.keyword in
// ('superhero', 'sequel', 'second-part', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence')
// AND n.name LIKE '%Downey%Robert%'
// AND t.production_year > 2000
// AND k.id = mk.keyword_id
// AND t.id = mk.movie_id
// AND t.id = ci.movie_id
// AND ci.movie_id = mk.movie_id
// AND n.id = ci.person_id;
#[cfg(test)]
mod test_q6d {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q6d() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q6d(&data)?;
        assert_eq!(
            res,
            Some((
                "based-on-comic",
                "Downey Jr., Robert",
                "2008 MTV Movie Awards"
            ))
        );
        Ok(())
    }
}
