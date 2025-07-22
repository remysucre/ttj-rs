use crate::data::Data;
use ahash::HashMap;
use polars::prelude::*;
use std::time::Instant;

use memchr::memmem;

#[inline]
fn matches(haystack: &str, finder: &memchr::memmem::Finder) -> bool {
    finder.find(haystack.as_bytes()).is_some()
}

pub fn q6b(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let ci = &db.ci;
    let k = &db.k;
    let mk = &db.mk;
    let n = &db.n;
    let t = &db.t;

    let downey = memmem::Finder::new(b"Downey");
    let robert = memmem::Finder::new(b"Robert");

    let start = Instant::now();

    let n_m: HashMap<i32, &str> =
        n.id.iter()
            .zip(n.name.iter())
            .filter_map(|(id, name)| {
                if matches(name, &downey) && matches(name, &robert) {
                    Some((*id, name.as_str()))
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

    let k_m: HashMap<i32, &str> =
        k.id.iter()
            .zip(k.keyword.iter())
            .filter_map(|(id, keyword)| {
                target_keywords
                    .contains(keyword.as_str())
                    .then_some((*id, keyword.as_str()))
            })
            .collect();

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

    let mk_m: HashMap<i32, Vec<&str>> = mk
        .movie_id
        .iter()
        .zip(mk.keyword_id.iter())
        .filter_map(|(movie_id, keyword_id)| {
            if t_m.contains_key(&movie_id) {
                k_m.get(&keyword_id).map(|&keyword| (movie_id, keyword))
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (movie_id, keyword)| {
            acc.entry(*movie_id).or_default().push(keyword);
            acc
        });

    let mut res: Option<(&str, &str, &str)> = None;

    for (pid, mid) in ci.person_id.iter().zip(ci.movie_id.iter()) {
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
        let data = Data::new(&db);
        let res = q6b(&data)?;
        assert_eq!(
            res,
            Some(("based-on-comic", "Downey Jr., Robert", "The Avengers 2"))
        );
        Ok(())
    }
}
