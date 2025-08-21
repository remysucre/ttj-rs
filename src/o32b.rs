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

    let elapsed = Instant::now();

    let lt_m: HashMap<&i32, &str> = lt
        .id
        .iter()
        .zip(lt.link.iter())
        .map(|(id, link)| (id, link.as_str()))
        .collect();

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
        .filter_map(|(keyword_id, movie_id)| (k_id == keyword_id).then_some(movie_id))
        .collect();

    let t1_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .filter_map(|(id, title)| mk_s.contains(id).then_some((id, title.as_str())))
            .collect();

    let mut res: Option<(&str, &str, &str)> = None;

    for ((movie_id, linked_movie_id), link_type_id) in ml
        .movie_id
        .iter()
        .zip(ml.linked_movie_id.iter())
        .zip(ml.link_type_id.iter())
    {
        if mk_s.contains(&movie_id)
            && let Some(link) = lt_m.get(&link_type_id)
            && let Some(title1) = t1_m.get(&movie_id)
            && let Some(title2) = t1_m.get(&linked_movie_id)
        {
            res = match res {
                Some((old_link, old_title1, old_title2)) => Some((
                    link.min(&old_link),
                    title1.min(&old_title1),
                    title2.min(&old_title2),
                )),
                None => Some((link, title1, title2)),
            };
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
