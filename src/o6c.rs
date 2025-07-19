use crate::data::ImdbData;
use polars::prelude::*;
use rustc_hash::{FxBuildHasher, FxHashMap as HashMap};
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
        .fold(
            HashMap::with_capacity_and_hasher(2, FxBuildHasher),
            |mut acc, (id, name)| {
                acc.insert(id, name);
                acc
            },
        );

    // let k_m: HashMap<i32, &str> = k
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .zip(k.column("keyword")?.str()?)
    //     .filter_map(|(id, keyword)| {
    //         if let (Some(id), Some(keyword)) = (id, keyword) {
    //             if keyword == "marvel-cinematic-universe" {
    //                 Some((id, keyword))
    //             } else {
    //                 None
    //             }
    //         } else {
    //             None
    //         }
    //     })
    //     .fold(
    //         HashMap::with_capacity_and_hasher(k.height(), FxBuildHasher),
    //         |mut acc, (id, keyword)| {
    //             acc.insert(id, keyword);
    //             acc
    //         },
    //     );

    // Exploit the fact that |k_m| = 1
    let k_m = k
        .column("id")?
        .i32()?
        .into_iter()
        .zip(k.column("keyword")?.str()?)
        .find_map(|(id, keyword)| {
            if let (Some(id), Some(keyword)) = (id, keyword) {
                if keyword == "marvel-cinematic-universe" {
                    Some((id, keyword))
                } else {
                    None
                }
            } else {
                None
            }
        });

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .zip(t.column("production_year")?.i32()?.into_iter())
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
        .fold(
            HashMap::with_capacity_and_hasher(438, FxBuildHasher),
            |mut acc, (id, title)| {
                acc.insert(id, title);
                acc
            },
        );

    let mk_m: HashMap<i32, Vec<i32>> = mk
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("keyword_id")?.i32()?)
        .filter_map(|(movie_id, keyword_id)| {
            if let (Some(movie_id), Some(keyword_id)) = (movie_id, keyword_id) {
                if let Some((id, _)) = k_m
                    && id == keyword_id
                    && t_m.contains_key(&movie_id)
                {
                    Some((movie_id, keyword_id))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(
            HashMap::with_capacity_and_hasher(mk.height(), FxBuildHasher),
            |mut acc, (movie_id, keyword_id)| {
                acc.entry(movie_id).or_default().push(keyword_id);
                acc
            },
        );

    let mut res: Option<(&str, &str, &str)> = None;

    for (pid, mid) in ci
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("movie_id")?.i32()?)
    {
        if let (Some(pid), Some(mid)) = (pid, mid) {
            if let Some(name) = n_m.get(&pid)
                && let Some((id, keyword)) = k_m
                && let Some(title) = t_m.get(&mid)
                && let Some(kids) = mk_m.get(&mid)
            {
                for kid in kids {
                    if id == *kid {
                        if let Some((old_keyword, old_name, old_title)) = res.as_mut() {
                            if name < old_name {
                                *old_name = name;
                            }
                            if keyword < *old_keyword {
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

    println!("6c,{:}", start.elapsed().as_secs_f32());

    Ok(res)
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
    fn test_q6b() -> Result<(), PolarsError> {
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
