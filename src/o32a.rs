use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

// imdb_int.movie_link(movie_id,linked_movie_id,link_type_id)
// |imdb_int.link_type(link_type_id)
// |imdb_int.title(movie_id,kind_id)
// |imdb.q32a_title2(linked_movie_id,kind_id1)
// |imdb_int.movie_keyword(movie_id,keyword_id)
// ||imdb.q32a_keyword(keyword_id)
pub fn q32a(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let k = &db.k;
    let lt = &db.lt;
    let mk = &db.mk;
    let ml = &db.ml;
    let t1 = &db.t;
    let t2 = &db.t;

    let t1_m: HashMap<i32, Vec<&str>> = t1
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t1.column("title")?.str()?.into_iter())
        .fold(HashMap::default(), |mut acc, (id, title)| {
            if let (Some(id), Some(title)) = (id, title) {
                acc.entry(id).or_default().push(title);
            }
            acc
        });

    let t2_m: HashMap<i32, Vec<&str>> = t2
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t2.column("title")?.str()?.into_iter())
        .fold(HashMap::default(), |mut acc, (id, title)| {
            if let (Some(id), Some(title)) = (id, title) {
                acc.entry(id).or_default().push(title);
            }
            acc
        });

    let lt_m: HashMap<i32, Vec<&str>> = lt
        .column("id")?
        .i32()?
        .into_iter()
        .zip(lt.column("link")?.str()?.into_iter())
        .fold(HashMap::default(), |mut acc, (id, link)| {
            if let (Some(id), Some(link)) = (id, link) {
                acc.entry(id).or_default().push(link);
            }
            acc
        });

    let elapsed = Instant::now();

    let k_s: HashSet<i32> = k
        .column("keyword")?
        .str()?
        .into_iter()
        .zip(k.column("id")?.i32()?.into_iter())
        .filter_map(|(keyword, id)| {
            if let (Some(keyword), Some(id)) = (keyword, id) {
                if keyword == "10,000-mile-club" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mk_s = mk
        .column("keyword_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("movie_id")?.i32()?.into_iter())
        .filter_map(|(keyword_id, movie_id)| {
            if let (Some(keyword_id), Some(movie_id)) = (keyword_id, movie_id) {
                if k_s.contains(&keyword_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let mut res: Option<(&str, &str, &str)> = None;

    for ((movie_id, linked_movie_id), link_type_id) in ml
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ml.column("linked_movie_id")?.i32()?.into_iter())
        .zip(ml.column("link_type_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(linked_movie_id), Some(link_type_id)) =
            (movie_id, linked_movie_id, link_type_id)
        {
            if mk_s.contains(&movie_id) {
                if let (Some(link_types), Some(titles1), Some(titles2)) = (
                    lt_m.get(&link_type_id),
                    t1_m.get(&movie_id),
                    t2_m.get(&linked_movie_id),
                ) {
                    for link_type in link_types {
                        for title1 in titles1 {
                            for title2 in titles2 {
                                if let Some((old_link_type, old_title1, old_title2)) = res.as_mut()
                                {
                                    if link_type < old_link_type {
                                        *old_link_type = *link_type;
                                    }
                                    if title1 < old_title1 {
                                        *old_title1 = *title1;
                                    }
                                    if title2 < old_title2 {
                                        *old_title2 = *title2;
                                    }
                                } else {
                                    res = Some((link_type, title1, title2));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    println!("{:}", elapsed.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 32a
// SELECT MIN(lt.link) AS link_type,
// MIN(t1.title) AS first_movie,
// MIN(t2.title) AS second_movie
// FROM keyword AS k,
// link_type AS lt,
// movie_keyword AS mk,
// movie_link AS ml,
// title AS t1,
// title AS t2
// WHERE k.keyword ='10,000-mile-club'
// AND mk.keyword_id = k.id
// AND t1.id = mk.movie_id
// AND ml.movie_id = t1.id
// AND ml.linked_movie_id = t2.id
// AND lt.id = ml.link_type_id
// AND mk.movie_id = t1.id;
#[cfg(test)]
mod test_32a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q32a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q32a(&db)?;
        assert_eq!(res, None);
        Ok(())
    }
}
