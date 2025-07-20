use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q6c(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let ci = &db.ci;
    let k = &db.k;
    let mk = &db.mk;
    let n = &db.n;
    let t = &db.t;

    let start = Instant::now();

    let n_m: HashMap<i32, &str> = n
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(n.column("name")?.str()?.into_no_null_iter())
        .filter_map(|(id, name)| {
            if name.contains("Downey") && name.contains("Robert") {
                Some((id, name))
            } else {
                None
            }
        })
        .collect();

    // 6c has WHERE k.keyword = 'marvel-cinematic-universe'
    // then it's guaranteed that MIN(k.keyword) in SELECT is 'marvel-cinematic-universe'.
    // Therefore, we only need the id that corresponds to 'marvel-cinematic-universe'.
    let k_id = k
        .column("keyword")?
        .str()?
        .into_no_null_iter()
        .zip(k.column("id")?.i32()?.into_no_null_iter())
        .find(|(keyword, _)| *keyword == "marvel-cinematic-universe")
        .map(|(_, id)| id)
        .unwrap();

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(t.column("title")?.str()?.into_no_null_iter())
        .zip(t.column("production_year")?.i32()?)
        .filter_map(|((id, title), production_year)| {
            production_year
                .filter(|&year| year > 2014)
                .map(|_| (id, title))
        })
        .collect();

    // Because k.id = mk.keyword_id and k.id is only one,
    // we just need a hashset.
    let mk_s: HashSet<i32> = mk
        .column("movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mk.column("keyword_id")?.i32()?.into_no_null_iter())
        .filter_map(|(movie_id, keyword_id)| {
            if k_id == keyword_id && t_m.contains_key(&movie_id) {
                Some(movie_id)
            } else {
                None
            }
        })
        .collect();

    let mut res: Option<(&str, &str)> = None;

    for (pid, mid) in ci
        .column("person_id")?
        .i32()?
        .into_no_null_iter()
        .zip(ci.column("movie_id")?.i32()?.into_no_null_iter())
    {
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
        let res = q6c(&db)?;
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
