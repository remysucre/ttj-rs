use crate::data::Data;
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
pub fn q14a(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
    let it = &db.it;
    let k = &db.k;
    let kt = &db.kt;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let t = &db.t;
    let mk = &db.mk;

    let start = Instant::now();

    let mut it1_id: &i32 = &0;
    let mut it2_id: &i32 = &0;

    for (id, info) in it.id.iter().zip(it.info.iter()) {
        if info == "countries" {
            it1_id = id;
        } else if info == "rating" {
            it2_id = id;
        }
    }

    let target_keywords: ahash::HashSet<&str> = ["murder", "murder-in-title", "blood", "violence"]
        .into_iter()
        .collect();

    let k_s: HashSet<&i32> = k
        .keyword
        .iter()
        .zip(k.id.iter())
        .filter_map(|(keyword, id)| target_keywords.contains(keyword.as_str()).then_some(id))
        .collect();

    let kt_id = kt
        .kind
        .iter()
        .zip(kt.id.iter())
        .find(|(kind, _)| *kind == "movie")
        .map(|(_, id)| id)
        .unwrap();

    let target_info: HashSet<&str> = [
        "Sweden",
        "Norway",
        "Germany",
        "Denmark",
        "Swedish",
        "Denish",
        "Norwegian",
        "German",
        "USA",
        "American",
    ]
    .into_iter()
    .collect();

    let mi_s: HashSet<&i32> = mi
        .info
        .iter()
        .zip(mi.movie_id.iter())
        .zip(mi.info_type_id.iter())
        .filter_map(|((info, movie_id), info_type_id)| {
            (target_info.contains(info.as_str()) && it1_id == info_type_id).then_some(movie_id)
        })
        .collect();

    let mk_s: HashSet<&i32> = mk
        .movie_id
        .iter()
        .zip(mk.keyword_id.iter())
        .filter_map(|(movie_id, keyword_id)| k_s.contains(keyword_id).then_some(movie_id))
        .collect();

    let t_m: HashMap<i32, &str> =
        t.id.iter()
            .zip(t.production_year.iter())
            .zip(t.title.iter())
            .zip(t.kind_id.iter())
            .filter_map(|(((movie_id, production_year), title), kind_id)| {
                if let Some(production_year) = production_year
                    && *production_year > 2010
                    && kt_id == kind_id
                {
                    Some((*movie_id, title.as_str()))
                } else {
                    None
                }
            })
            .collect();

    let mut res: Option<(&str, &str)> = None;

    for ((info_type_id, movie_id), info) in mi_idx
        .info_type_id
        .iter()
        .zip(mi_idx.movie_id.iter())
        .zip(mi_idx.info.iter())
    {
        if it2_id == info_type_id
            && info.as_str() < "8.5"
            && let Some(title) = t_m.get(&movie_id)
            && mi_s.contains(&movie_id)
            && mk_s.contains(&movie_id)
        {
            res = match res {
                Some((old_info, old_title)) => {
                    Some((info.as_str().min(old_info), title.min(&old_title)))
                }
                None => Some((info, title)),
            };
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
mod test_q14a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q14a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q14a(&data)?;
        assert_eq!(res, Some(("1.0", "$lowdown")));
        Ok(())
    }
}
