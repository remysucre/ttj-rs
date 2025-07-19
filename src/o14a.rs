use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

// imdb.q14a_movie_info_idx2(movie_id,info_type_id2)
// |imdb.q14a_info_type2(info_type_id2)
// |imdb.q14a_title(movie_id,kind_id)
// ||imdb.q14a_kind_type(kind_id)
// |imdb.q14a_movie_info(movie_id,info_type_id)
// ||imdb.q14a_info_type1(info_type_id)
// |imdb_int.movie_keyword(movie_id,keyword_id)
// ||imdb.q14a_keyword(keyword_id)
pub fn q14a(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    let it1 = &db.it;
    let it2 = &db.it;
    let k = &db.k;
    let kt = &db.kt;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let t = &db.t;
    let mk = &db.mk;

    let start = Instant::now();

    let it1_s: HashSet<i32> = it1
        .column("info")?
        .str()?
        .into_iter()
        .zip(it1.column("id")?.i32()?)
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "countries" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let it2_s: HashSet<i32> = it2
        .column("info")?
        .str()?
        .into_iter()
        .zip(it2.column("id")?.i32()?)
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
        .zip(k.column("id")?.i32()?)
        .filter_map(|(keyword, id)| {
            if let (Some(keyword), Some(id)) = (keyword, id) {
                if matches!(keyword, "murder" | "murder-in-title" | "blood" | "violence") {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let kt_s: HashSet<i32> = kt
        .column("kind")?
        .str()?
        .into_iter()
        .zip(kt.column("id")?.i32()?)
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if kind == "movie" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let mi_s: HashSet<i32> = mi
        .column("info")?
        .str()?
        .into_iter()
        .zip(mi.column("movie_id")?.i32()?)
        .zip(mi.column("info_type_id")?.i32()?)
        .filter_map(|((info, movie_id), info_type_id)| {
            if let (Some(info), Some(movie_id), Some(info_type_id)) = (info, movie_id, info_type_id)
            {
                if matches!(
                    info,
                    "Sweden"
                        | "Norway"
                        | "Germany"
                        | "Denmark"
                        | "Swedish"
                        | "Denish"
                        | "Norwegian"
                        | "German"
                        | "USA"
                        | "American"
                ) && it1_s.contains(&(info_type_id))
                {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    // TODO: this step can be optimized using prebuilting index and then time the
    // filter step.
    let mk_s: HashSet<i32> = mk
        .column("keyword_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("movie_id")?.i32()?)
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
        .collect();

    let t_m: HashMap<i32, Vec<&str>> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?)
        .zip(t.column("production_year")?.i32()?)
        .zip(t.column("kind_id")?.i32()?)
        .filter_map(|(((id, title), production_year), kind_id)| {
            if let (Some(id), Some(title), Some(production_year), Some(kind_id)) =
                (id, title, production_year, kind_id)
            {
                if production_year > 2010 && kt_s.contains(&kind_id) {
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

    for ((info_type_id, movie_id), info) in mi_idx
        .column("info_type_id")?
        .i32()?
        .into_iter()
        .zip(mi_idx.column("movie_id")?.i32()?.into_iter())
        .zip(mi_idx.column("info")?.str()?.into_iter())
    {
        if let (Some(info_type_id), Some(movie_id), Some(info)) = (info_type_id, movie_id, info) {
            if it2_s.contains(&info_type_id) && info < "8.5" {
                if let Some(titles) = t_m.get(&movie_id) {
                    if mi_s.contains(&movie_id) && mk_s.contains(&movie_id) {
                        for title in titles {
                            if let Some((old_title, old_info)) = res.as_mut() {
                                if title < old_title {
                                    *old_title = title;
                                }
                                if info < *old_info {
                                    *old_info = info;
                                }
                            } else {
                                res = Some((title, info));
                            }
                        }
                    }
                }
            }
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("14a,{duration:}");

    Ok(res)
}

// -- JOB Query 14a
// SELECT MIN(mi_idx.info) AS rating,
//        MIN(t.title) AS northern_dark_movie
// FROM info_type AS it1,
//      info_type AS it2,
//      keyword AS k,
//      kind_type AS kt,
//      movie_info AS mi,
//      movie_info_idx AS mi_idx,
//      movie_keyword AS mk,
//      title AS t
// WHERE it1.info = 'countries'
//   AND it2.info = 'rating'
//   AND k.keyword IN ('murder',
//                     'murder-in-title',
//                     'blood',
//                     'violence')
//   AND kt.kind = 'movie'
//   AND mi.info IN ('Sweden',
//                   'Norway',
//                   'Germany',
//                   'Denmark',
//                   'Swedish',
//                   'Denish',
//                   'Norwegian',
//                   'German',
//                   'USA',
//                   'American')
//   AND mi_idx.info < '8.5'
//   AND t.production_year > 2010
//   AND kt.id = t.kind_id
//   AND t.id = mi.movie_id
//   AND t.id = mk.movie_id
//   AND t.id = mi_idx.movie_id
//   AND mk.movie_id = mi.movie_id
//   AND mk.movie_id = mi_idx.movie_id
//   AND mi.movie_id = mi_idx.movie_id
//   AND k.id = mk.keyword_id
//   AND it1.id = mi.info_type_id
//   AND it2.id = mi_idx.info_type_id;
#[cfg(test)]
mod test_14a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q14a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q14a(&db)?;
        assert_eq!(res, Some(("$lowdown", "1.0")));
        Ok(())
    }
}
