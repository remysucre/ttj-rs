use crate::data::ImdbData;
use ahash::HashMap;
use polars::prelude::*;
use std::time::Instant;

pub fn q6b(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
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
        .collect();

    let k_m: HashMap<i32, &str> = k
        .column("id")?
        .i32()?
        .into_iter()
        .zip(k.column("keyword")?.str()?)
        .filter_map(|(id, keyword)| {
            if let (Some(id), Some(keyword)) = (id, keyword) {
                if matches!(
                    keyword,
                    "marvel-cinematic-universe"
                        | "superhero"
                        | "sequel"
                        | "second-part"
                        | "marvel-comics"
                        | "based-on-comic"
                        | "tv-special"
                        | "fight"
                        | "violence"
                ) {
                    Some((id, keyword))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?)
        .zip(t.column("production_year")?.i32()?)
        .filter_map(|((id, title), production_year)| {
            if let (Some(id), Some(title), Some(production_year)) = (id, title, production_year) {
                if production_year > 2014 {
                    Some((id, title))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mk_m: HashMap<i32, Vec<&str>> = mk
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("keyword_id")?.i32()?)
        .filter_map(|(movie_id, keyword_id)| {
            if let (Some(movie_id), Some(keyword_id)) = (movie_id, keyword_id) {
                if let Some(keyword) = k_m.get(&keyword_id)
                    && t_m.contains_key(&movie_id)
                {
                    Some((movie_id, keyword))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (movie_id, keyword)| {
            acc.entry(movie_id).or_default().push(keyword);
            acc
        });

    let mut res: Option<(&str, &str, &str)> = None;

    for (pid, mid) in ci
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("movie_id")?.i32()?.into_iter())
    {
        if let (Some(pid), Some(mid)) = (pid, mid) {
            if let Some(name) = n_m.get(&pid)
                && let Some(title) = t_m.get(&mid)
                && let Some(keywords) = mk_m.get(&mid)
            {
                for keyword in keywords {
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

    println!("6b,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 6b
// SELECT MIN(k.keyword) AS movie_keyword, MIN(n.name) AS actor_name, MIN(t.title) AS hero_movie
// FROM cast_info AS ci,
// keyword AS k,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE k.keyword in
// ('superhero', 'sequel', 'second-part', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence')
// AND n.name LIKE '%Downey%Robert%'
// AND t.production_year > 2014
// AND k.id = mk.keyword_id
// AND t.id = mk.movie_id
// AND t.id = ci.movie_id
// AND ci.movie_id = mk.movie_id
// AND n.id = ci.person_id;
#[cfg(test)]
mod test_6b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q6b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q6b(&db)?;
        assert_eq!(
            res,
            Some(("based-on-comic", "Downey Jr., Robert", "The Avengers 2"))
        );
        Ok(())
    }
}
