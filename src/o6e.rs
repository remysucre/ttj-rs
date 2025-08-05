use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q6e(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
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

    let k_id = k
        .keyword
        .iter()
        .zip(k.id.iter())
        .find(|(keyword, _)| *keyword == "marvel-cinematic-universe")
        .map(|(_, id)| id)
        .unwrap();

    let mk_s: HashSet<i32> = mk
        .movie_id
        .iter()
        .zip(mk.keyword_id.iter())
        .filter_map(|(movie_id, keyword_id)| (k_id == keyword_id).then_some(*movie_id))
        .collect();

    let t_m: HashMap<i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|((id, title), production_year)| {
                production_year
                    .filter(|&year| year > 2000 && mk_s.contains(&id))
                    .map(|_| (*id, title.as_str()))
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

    println!("6e,{:}", start.elapsed().as_secs_f32());

    Ok(res.map(|(name, title)| ("marvel-cinematic-universe", name, title)))
}

// -- JOB Query 6e
// SELECT MIN(k.keyword) AS movie_keyword,
// MIN(n.name) AS actor_name,
// MIN(t.title) AS marvel_movie
// FROM cast_info AS ci,
// keyword AS k,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE k.keyword = 'marvel-cinematic-universe'
// AND n.name LIKE '%Downey%Robert%'
// AND t.production_year > 2000
// AND k.id = mk.keyword_id
// AND t.id = mk.movie_id
// AND t.id = ci.movie_id
// AND ci.movie_id = mk.movie_id
// AND n.id = ci.person_id;
#[cfg(test)]
mod test_6e {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q6e() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q6e(&data)?;
        assert_eq!(
            res,
            Some((
                "marvel-cinematic-universe",
                "Downey Jr., Robert",
                "Iron Man 3"
            ))
        );
        Ok(())
    }
}
