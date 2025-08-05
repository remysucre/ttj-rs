use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem;
use polars::prelude::*;
use std::time::Instant;

pub fn q6c(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let ci = &db.ci;
    let k = &db.k;
    let mk = &db.mk;
    let n = &db.n;
    let t = &db.t;

    let downey = memmem::Finder::new(b"Downey");
    let robert = memmem::Finder::new(b"Robert");

    let start = Instant::now();

    let n_m: HashMap<i32, &str> = n
        .id
        .iter()
        .zip(n.name.iter())
        .filter_map(|(id, name)| {
            if downey.find(name.as_bytes()).is_some() && robert.find(name.as_bytes()).is_some() {
                Some((*id, name.as_str()))
            } else {
                None
            }
        })
        .collect();

    // 6c has WHERE k.keyword = 'marvel-cinematic-universe'
    // then it's guaranteed that MIN(k.keyword) in SELECT is 'marvel-cinematic-universe'.
    // Therefore, we only need the id that corresponds to 'marvel-cinematic-universe'.
    let k_id = k
        .keyword
        .iter()
        .zip(k.id.iter())
        .find(|(keyword, _)| *keyword == "marvel-cinematic-universe")
        .map(|(_, id)| id)
        .unwrap();

    let t_m: HashMap<i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|((id, title), production_year)| {
                production_year
                    .filter(|&year| year > 2014)
                    .map(|_| (*id, title.as_str()))
            })
            .collect();

    // Because k.id = mk.keyword_id and k.id is only one,
    // we just need a hashset.
    let mk_s: HashSet<i32> = mk
        .movie_id
        .iter()
        .zip(mk.keyword_id.iter())
        .filter_map(|(movie_id, keyword_id)| {
            (k_id == keyword_id && t_m.contains_key(&movie_id)).then_some(*movie_id)
        })
        .collect();

    let mut res: Option<(&str, &str)> = None;

    for (pid, mid) in ci.person_id.iter().zip(ci.movie_id.iter()) {
        if mk_s.contains(&mid)
            && let Some(name) = n_m.get(&pid)
            && let Some(title) = t_m.get(&mid)
        {
            res = match res {
                Some((old_name, old_title)) => Some((name.min(&old_name), title.min(&old_title))),
                None => Some((name, title)),
            };
        }
    }

    println!("6c,{:}", start.elapsed().as_secs_f32());

    Ok(match res {
        Some((name, title)) => Some(("marvel-cinematic-universe", name, title)),
        _ => None,
    })
}

// -- JOB Query 6c
// SELECT MIN(k.keyword) AS movie_keyword, MIN(n.name) AS actor_name, MIN(t.title) AS marvel_movie
// FROM cast_info AS ci,
// keyword AS k,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE k.keyword = 'marvel-cinematic-universe'
// AND n.name LIKE '%Downey%Robert%'
// AND t.production_year > 2014
// AND k.id = mk.keyword_id
// AND t.id = mk.movie_id
// AND t.id = ci.movie_id
// AND ci.movie_id = mk.movie_id
// AND n.id = ci.person_id;
#[cfg(test)]
mod test_6c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q6c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q6c(&data)?;
        assert_eq!(
            res,
            Some((
                "marvel-cinematic-universe",
                "Downey Jr., Robert",
                "The Avengers 2"
            ))
        );
        Ok(())
    }
}
