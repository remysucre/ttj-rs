use crate::data::ImdbData;
use ahash::HashMap;
use polars::prelude::*;
use std::time::Instant;

pub fn q6a(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let ci = &db.ci;
    let k = &db.k;
    let mk = &db.mk;
    let n = &db.n;
    let t = &db.t;

    let start = Instant::now();

    let n_m: HashMap<i32, &str> = n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?)
        .filter_map(|(id, name)| {
            if let (Some(id), Some(name)) = (id, name) {
                if name.contains("Downey") && name.contains("Robert") {
                    Some((id, name))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (id, name)| {
            acc.insert(id, name);
            acc
        });

    let mut k_m: HashMap<i32, &str> = HashMap::default();

    for (id, keyword) in k
        .column("id")?
        .i32()?
        .into_iter()
        .zip(k.column("keyword")?.str()?.into_iter())
    {
        if let (Some(id), Some(keyword)) = (id, keyword) {
            if keyword == "marvel-cinematic-universe" {
                k_m.insert(id, keyword);
            }
        }
    }

    let mut mk_m: HashMap<i32, Vec<i32>> = HashMap::default();

    for (movie_id, keyword_id) in mk
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("keyword_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(keyword_id)) = (movie_id, keyword_id) {
            if k_m.contains_key(&keyword_id) {
                mk_m.entry(movie_id).or_default().push(keyword_id);
            }
        }
    }

    let mut t_m: HashMap<i32, &str> = HashMap::default();

    for ((id, title), production_year) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .zip(t.column("production_year")?.i32()?.into_iter())
    {
        if let (Some(id), Some(title), Some(production_year)) = (id, title, production_year) {
            if mk_m.contains_key(&id) && production_year > 2010 {
                t_m.insert(id, title);
            }
        }
    }

    let mut res: Option<(&str, &str, &str)> = None;

    for (pid, mid) in ci
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("movie_id")?.i32()?.into_iter())
    {
        if let (Some(pid), Some(mid)) = (pid, mid) {
            if let Some(title) = t_m.get(&mid)
                && let Some(kids) = mk_m.get(&mid)
                && let Some(name) = n_m.get(&pid)
            {
                for kid in kids {
                    if let Some(keyword) = k_m.get(kid) {
                        if let Some((old_keyword, old_name, old_title)) = res.as_mut() {
                            if name < old_name {
                                *old_name = name;
                            }
                            if keyword < old_keyword {
                                *old_keyword = keyword;
                            }
                            if title < old_title {
                                *old_title = title;
                            }
                        } else {
                            res = Some((keyword, name, title));
                        }
                    }
                }
            }
        }
    }

    println!("6a,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 6a
// SELECT MIN(k.keyword) AS movie_keyword, MIN(n.name) AS actor_name, MIN(t.title) AS marvel_movie
// FROM cast_info AS ci,
// keyword AS k,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE k.keyword = 'marvel-cinematic-universe'
// AND n.name LIKE '%Downey%Robert%'
// AND t.production_year > 2010
// AND k.id = mk.keyword_id
// AND t.id = mk.movie_id
// AND t.id = ci.movie_id
// AND ci.movie_id = mk.movie_id
// AND n.id = ci.person_id;
#[cfg(test)]
mod test_6a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q6a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q6a(&db)?;
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
