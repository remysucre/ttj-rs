use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

// imdb.q4a_movie_info_idx(movie_id,info_type_id)
// |imdb.q4a_title(movie_id,kind_id)
// |imdb.q4a_info_type(info_type_id)
// |imdb_int.movie_keyword(movie_id,keyword_id)
// ||imdb.q4a_keyword(keyword_id)
pub fn q4a(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    let it = &db.it;
    let k = &db.k;
    let mi_idx = &db.mi_idx;
    let mk = &db.mk;
    let t = &db.t;

    let start = Instant::now();

    let it_s: HashSet<i32> = it
        .column("info")?
        .str()?
        .into_iter()
        .zip(it.column("id")?.i32()?.into_iter())
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "rating" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let k_s: HashSet<i32> = k
        .column("keyword")?
        .str()?
        .into_iter()
        .zip(k.column("id")?.i32()?.into_iter())
        .filter_map(|(keyword, id)| {
            if let (Some(keyword), Some(id)) = (keyword, id) {
                if keyword.contains("sequel") {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mk_s: HashSet<i32> = mk
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("keyword_id")?.i32()?.into_iter())
        .filter_map(|(movie_id, keyword_id)| {
            if let (Some(movie_id), Some(keyword_id)) = (movie_id, keyword_id) {
                if k_s.contains(&keyword_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let t_m: HashMap<i32, Vec<&str>> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .zip(t.column("production_year")?.i32()?.into_iter())
        .filter_map(|((id, title), production_year)| {
            if let (Some(id), Some(title), Some(production_year)) = (id, title, production_year) {
                if production_year > 2005 && mk_s.contains(&id) {
                    Some((id, title))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (id, title)| {
            acc.entry(id).or_default().push(title);
            acc
        });

    let mut res: Option<(&str, &str)> = None;

    for ((movie_id, info), info_type_id) in mi_idx
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi_idx.column("info")?.str()?.into_iter())
        .zip(mi_idx.column("info_type_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(info), Some(info_type_id)) = (movie_id, info, info_type_id) {
            if it_s.contains(&info_type_id) && info > "5.0" {
                if let Some(titles) = t_m.get(&movie_id) {
                    for title in titles {
                        if let Some((old_info, old_title)) = res.as_mut() {
                            if info < *old_info {
                                *old_info = info;
                            }
                            if title < old_title {
                                *old_title = title;
                            }
                        } else {
                            res = Some((info, title));
                        }
                    }
                }
            }
        }
    }
    let duration = start.elapsed().as_secs_f32();
    println!("{:}", duration);

    Ok(res)
}

// JOB 4a
// SELECT MIN(mi_idx.info) AS rating,
// MIN(t.title) AS movie_title
// FROM info_type AS it,
// keyword AS k,
// movie_info_idx AS mi_idx,
// movie_keyword AS mk,
// title AS t
// WHERE it.info ='rating'
// AND k.keyword LIKE '%sequel%'
// AND mi_idx.info > '5.0'
// AND t.production_year > 2005
// AND t.id = mi_idx.movie_id
// AND t.id = mk.movie_id
// AND mk.movie_id = mi_idx.movie_id
// AND k.id = mk.keyword_id
// AND it.id = mi_idx.info_type_id;
#[cfg(test)]
mod test_4a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q4a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q4a(&db)?;
        assert_eq!(res, Some(("5.1", "& Teller 2")));
        Ok(())
    }
}
