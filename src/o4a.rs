use crate::data::{Data, ImdbData};
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

// imdb.q4a_movie_info_idx(movie_id,info_type_id)
// |imdb.q4a_title(movie_id,kind_id)
// |imdb.q4a_info_type(info_type_id)
// |imdb_int.movie_keyword(movie_id,keyword_id)
// ||imdb.q4a_keyword(keyword_id)
pub fn q4a(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
    let it = &db.it;
    let k = &db.k;
    let mi_idx = &db.mi_idx;
    let mk = &db.mk;
    let t = &db.t;

    let start = Instant::now();

    let it_id: &i32 = it
        .info
        .iter()
        .zip(it.id.iter())
        .find(|(info, _)| *info == "rating")
        .map(|(_, id)| id)
        .unwrap();

    let k_id = k
        .keyword
        .iter()
        .zip(k.id.iter())
        .find(|(keyword, _)| *keyword == "sequel")
        .map(|(_, id)| id)
        .unwrap();

    let mk_s = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| (keyword_id == k_id).then_some(*movie_id))
        .collect::<HashSet<_>>();

    let t_m: HashMap<i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|((id, title), production_year)| {
                production_year
                    .filter(|production_year| *production_year > 2005 && mk_s.contains(&id))
                    .map(|_| (*id, title.as_str()))
            })
            .collect();

    let mut res: Option<(&str, &str)> = None;

    for ((movie_id, info), info_type_id) in mi_idx
        .movie_id
        .iter()
        .zip(mi_idx.info.iter())
        .zip(mi_idx.info_type_id.iter())
    {
        if it_id == info_type_id
            && info.as_str() > "5.0"
            && let Some(title) = t_m.get(&movie_id)
        {
            res = match res {
                Some((old_info, old_title)) => {
                    Some((info.as_str().min(&old_info), title.min(&old_title)))
                }
                None => Some((info, title)),
            };
        }
    }
    let duration = start.elapsed().as_secs_f32();
    println!("4a,{duration:}");

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
mod test_q4a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q4a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q4a(&data)?;
        assert_eq!(res, Some(("5.1", "& Teller 2")));
        Ok(())
    }
}
