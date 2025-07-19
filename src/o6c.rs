use crate::data::ImdbData;
use ahash::HashMap;
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

    let k_m: HashMap<i32, &str> = k
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(k.column("keyword")?.str()?.into_no_null_iter())
        .filter_map(|(id, keyword)| {
            if keyword == "marvel-cinematic-universe" {
                Some((id, keyword))
            } else {
                None
            }
        })
        .collect();

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(t.column("title")?.str()?.into_no_null_iter())
        .zip(t.column("production_year")?.i32()?)
        .filter_map(|((id, title), production_year)| {
            if let Some(production_year) = production_year {
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

    let mk_m: HashMap<i32, Vec<i32>> = mk
        .column("movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mk.column("keyword_id")?.i32()?.into_no_null_iter())
        .filter_map(|(movie_id, keyword_id)| {
            if k_m.contains_key(&keyword_id) && t_m.contains_key(&movie_id) {
                Some((movie_id, keyword_id))
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (movie_id, keyword_id)| {
            acc.entry(movie_id).or_default().push(keyword_id);
            acc
        });

    let mut res: Option<(&str, &str, &str)> = None;
    let valid_pairs: Vec<(i32, i32)> = ci
        .column("person_id")?
        .i32()?
        .into_no_null_iter()
        .zip(ci.column("movie_id")?.i32()?.into_no_null_iter())
        .collect();

    for (pid, mid) in valid_pairs {
        if let Some(name) = n_m.get(&pid)
            && let Some(title) = t_m.get(&mid)
            && let Some(kids) = mk_m.get(&mid)
        {
            let best_keyword = kids.iter().filter_map(|kid| k_m.get(kid)).min();
            if let Some(keyword) = best_keyword {
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
    }

    // let mut res: Option<(&str, &str, &str)> = None;
    //
    // for (pid, mid) in ci
    //     .column("person_id")?
    //     .i32()?
    //     .into_iter()
    //     .zip(ci.column("movie_id")?.i32()?)
    // {
    //     if let (Some(pid), Some(mid)) = (pid, mid) {
    //         if let Some(name) = n_m.get(&pid)
    //             && let Some(title) = t_m.get(&mid)
    //             && let Some(kids) = mk_m.get(&mid)
    //         {
    //             for kid in kids {
    //                 if let Some(keyword) = k_m.get(&kid) {
    //                     if let Some((old_keyword, old_name, old_title)) = res.as_mut() {
    //                         if name < old_name {
    //                             *old_name = name;
    //                         }
    //                         if *keyword < *old_keyword {
    //                             *old_keyword = keyword;
    //                         }
    //                         if title < old_title {
    //                             *old_title = title;
    //                         }
    //                     } else {
    //                         res = Some((keyword, name, title));
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }

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
