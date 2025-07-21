use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q6d(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
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
            (name.contains("Downey") && name.contains("Robert")).then_some((id, name))
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

    let k_m: HashMap<i32, &str> = k
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(k.column("keyword")?.str()?.into_no_null_iter())
        .filter_map(|(id, keyword)| target_keywords.contains(keyword).then_some((id, keyword)))
        .collect();

    // Optimization: Pre-compute minimum keywords per movie: Instead of storing all keywords per movie
    // and finding the minimum repeatedly, we compute the minimum keyword once during the initial pass through mk.
    let mk_m: HashMap<i32, &str> = mk
        .column("movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mk.column("keyword_id")?.i32()?.into_no_null_iter())
        .filter_map(|(movie_id, keyword_id)| {
            k_m.get(&keyword_id).map(|&keyword| (movie_id, keyword))
        })
        .fold(HashMap::default(), |mut acc, (movie_id, keyword)| {
            acc.entry(movie_id)
                .and_modify(|existing| {
                    if keyword < *existing {
                        *existing = keyword;
                    }
                })
                .or_insert(keyword);
            acc
        });

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(t.column("title")?.str()?.into_no_null_iter())
        .zip(t.column("production_year")?.i32()?)
        .filter_map(|((id, title), production_year)| {
            if let Some(production_year) = production_year
                && production_year > 2000
                && mk_m.contains_key(&id)
            {
                Some((id, title))
            } else {
                None
            }
        })
        .collect();

    let mut res: Option<(&str, &str, &str)> = None;

    for (pid, mid) in ci
        .column("person_id")?
        .i32()?
        .into_no_null_iter()
        .zip(ci.column("movie_id")?.i32()?.into_no_null_iter())
    {
        if let Some(title) = t_m.get(&mid)
            && let Some(name) = n_m.get(&pid)
            && let Some(keyword) = mk_m.get(&mid)
        {
            res = match res {
                Some((old_keyword, old_name, old_title)) => Some((
                    keyword.min(&old_keyword),
                    name.min(&old_name),
                    title.min(&old_title),
                )),
                None => Some((keyword, name, title)),
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
mod test_6d {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q6d() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q6d(&db)?;
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
