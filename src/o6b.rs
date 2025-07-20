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

    let re = regex::Regex::new(r"Downey.*Robert")?;

    let start = Instant::now();

    let n_m: HashMap<i32, &str> = n
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(n.column("name")?.str()?.into_no_null_iter())
        .filter_map(|(id, name)| {
            if re.is_match(name) {
                Some((id, name))
            } else {
                None
            }
        })
        .collect();

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
        .filter_map(|(id, keyword)| target_keywords.contains(keyword).then_some((id, keyword)))
        .collect();

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

    let mk_m: HashMap<i32, Vec<&str>> = mk
        .column("movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mk.column("keyword_id")?.i32()?.into_no_null_iter())
        .filter_map(|(movie_id, keyword_id)| {
            if t_m.contains_key(&movie_id) {
                k_m.get(&keyword_id).map(|&keyword| (movie_id, keyword))
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
        .into_no_null_iter()
        .zip(ci.column("movie_id")?.i32()?.into_no_null_iter())
    {
        if let Some(name) = n_m.get(&pid)
            && let Some(title) = t_m.get(&mid)
            && let Some(keywords) = mk_m.get(&mid)
        {
            for &keyword in keywords {
                match res {
                    Some((min_keyword, min_name, min_title)) => {
                        let new_keyword = if keyword < min_keyword {
                            keyword
                        } else {
                            min_keyword
                        };
                        let new_name = if *name < min_name { *name } else { min_name };
                        let new_title = if *title < min_title {
                            *title
                        } else {
                            min_title
                        };
                        res = Some((new_keyword, new_name, new_title));
                    }
                    None => {
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
