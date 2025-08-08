use crate::data::Data;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q32b(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let k = &db.k;
    let lt = &db.lt;
    let mk = &db.mk;
    let ml = &db.ml;
    let t = &db.t;

    let t1_m: HashMap<&i32, Vec<&str>> =
        t.id.iter()
            .zip(t.title.iter())
            .fold(HashMap::default(), |mut acc, (id, title)| {
                acc.entry(id).or_default().push(title);
                acc
            });

    let t2_m: HashMap<&i32, Vec<&str>> =
        t.id.iter()
            .zip(t.title.iter())
            .fold(HashMap::default(), |mut acc, (id, title)| {
                acc.entry(id).or_default().push(title);
                acc
            });

    let lt_m: HashMap<&i32, Vec<&str>> =
        lt.id
            .iter()
            .zip(lt.link.iter())
            .fold(HashMap::default(), |mut acc, (id, link)| {
                acc.entry(id).or_default().push(link);
                acc
            });

    let elapsed = Instant::now();

    let k_id = k
        .keyword
        .iter()
        .zip(k.id.iter())
        .find(|(keyword, _)| *keyword == "character-name-in-title")
        .map(|(_, id)| id)
        .unwrap();

    let mk_s: HashSet<&i32> = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| {
            (k_id == keyword_id && t1_m.contains_key(&movie_id)).then_some(movie_id)
        })
        .collect();

    let mut res: Option<(&str, &str, &str)> = None;

    for ((movie_id, linked_movie_id), link_type_id) in ml
        .movie_id
        .iter()
        .zip(ml.linked_movie_id.iter())
        .zip(ml.link_type_id.iter())
    {
        if mk_s.contains(&movie_id) {
            if let (Some(links), Some(titles1), Some(titles2)) = (
                lt_m.get(&link_type_id),
                t1_m.get(&movie_id),
                t2_m.get(&linked_movie_id),
            ) {
                res = match res {
                    Some((old_link, old_title1, old_title2)) => Some((
                        links.iter().min().unwrap().min(&old_link),
                        titles1.iter().min().unwrap().min(&old_title1),
                        titles2.iter().min().unwrap().min(&old_title2),
                    )),
                    None => Some((
                        links.iter().min().unwrap(),
                        titles1.iter().min().unwrap(),
                        titles2.iter().min().unwrap(),
                    )),
                };
            }
        }
    }

    println!("32b,{:}", elapsed.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 32b
// SELECT MIN(lt.link) AS link_type, MIN(t1.title) AS first_movie, MIN(t2.title) AS second_movie
// FROM keyword AS k,
// link_type AS lt,
// movie_keyword AS mk,
// movie_link AS ml,
// title AS t1,
// title AS t2
// WHERE k.keyword = 'character-name-in-title'
// AND mk.keyword_id = k.id
// AND t1.id = mk.movie_id
// AND ml.movie_id = t1.id
// AND ml.linked_movie_id = t2.id
// AND lt.id = ml.link_type_id
// AND mk.movie_id = t1.id;
#[cfg(test)]
mod test_q32b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q32b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q32b(&data)?;
        assert_eq!(
            res,
            Some((
                "alternate language version of",
                "12 oz. Mouse",
                "'Angel': Season 2 Overview"
            ))
        );
        Ok(())
    }
}
