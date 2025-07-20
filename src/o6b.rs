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

    let ci_m: HashMap<i32, Vec<i32>> = ci
        .column("movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(ci.column("person_id")?.i32()?.into_no_null_iter())
        .filter_map(|(movie_id, person_id)| {
            if n_m.contains_key(&person_id) {
                Some((movie_id, person_id))
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (movie_id, person_id)| {
            acc.entry(movie_id).or_default().push(person_id);
            acc
        });

    let target_keywords: ahash::HashSet<&str> = [
        "marvel-cinematic-universe",
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
        .filter_map(|(id, keyword)| {
            if target_keywords.contains(keyword) {
                Some((id, keyword))
            } else {
                None
            }
        })
        .collect();

    let mk_m: HashMap<i32, Vec<&str>> = mk
        .column("movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mk.column("keyword_id")?.i32()?.into_no_null_iter())
        .filter_map(|(movie_id, keyword_id)| {
            if ci_m.contains_key(&movie_id) {
                k_m.get(&keyword_id).map(|&keyword| (movie_id, keyword))
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (movie_id, keyword)| {
            acc.entry(movie_id).or_default().push(keyword);
            acc
        });

    let mut min_keyword: Option<&str> = None;
    let mut min_name: Option<&str> = None;
    let mut min_title: Option<&str> = None;

    for ((movie_id, title), production_year) in t
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(t.column("title")?.str()?.into_no_null_iter())
        .zip(t.column("production_year")?.i32()?)
    {
        if let Some(production_year) = production_year
            && production_year > 2014
            && let Some(keywords) = mk_m.get(&movie_id)
            && let Some(person_ids) = ci_m.get(&movie_id)
        {
            min_title = Some(match min_title {
                None => title,
                Some(current) => {
                    if title < current {
                        title
                    } else {
                        current
                    }
                }
            });

            for &person_id in person_ids {
                if let Some(&name) = n_m.get(&person_id) {
                    min_name = Some(match min_name {
                        None => name,
                        Some(current) => {
                            if name < current {
                                name
                            } else {
                                current
                            }
                        }
                    });

                    for &keyword in keywords {
                        min_keyword = Some(match min_keyword {
                            None => keyword,
                            Some(current) => {
                                if keyword < current {
                                    keyword
                                } else {
                                    current
                                }
                            }
                        });
                    }
                }
            }
        }
    }

    println!("6b,{:}", start.elapsed().as_secs_f32());

    match (min_keyword, min_name, min_title) {
        (Some(keyword), Some(name), Some(title)) => Ok(Some((keyword, name, title))),
        _ => Ok(None),
    }
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
