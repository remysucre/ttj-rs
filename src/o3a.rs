use crate::data::Data;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q3a(db: &Data) -> Result<Option<&str>, PolarsError> {
    let k = &db.k;
    let mi = &db.mi;
    let mk = &db.mk;
    let t = &db.t;

    let start = Instant::now();

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

    let target_info: HashSet<&str> = [
        "Sweden",
        "Norway",
        "Germany",
        "Denmark",
        "Swedish",
        "Denish",
        "Norwegian",
        "German",
    ]
    .into_iter()
    .collect();

    let mi_s: HashSet<i32> = mi
        .info
        .iter()
        .zip(mi.movie_id.iter())
        .filter_map(|(info, movie_id)| target_info.contains(info.as_str()).then_some(*movie_id))
        .collect();

    let mut res: Option<&str> = None;

    for ((movie_id, production_year), title) in
        t.id.iter()
            .zip(t.production_year.iter())
            .zip(t.title.iter())
    {
        if let Some(production_year) = production_year
            && production_year > &2005
            && mk_s.contains(&movie_id)
            && mi_s.contains(&movie_id)
        {
            res = match res {
                Some(old_title) => Some(title.as_str().min(&old_title)),
                None => Some(title),
            };
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("3a,{duration:}");

    Ok(res)
}

// SELECT MIN(t.title) AS movie_title
// FROM keyword AS k,
//      movie_info AS mi,
//      movie_keyword AS mk,
//      title AS t
// WHERE k.keyword LIKE '%sequel%'
//   AND mi.info IN ('Sweden',
//                   'Norway',
//                   'Germany',
//                   'Denmark',
//                   'Swedish',
//                   'Denish',
//                   'Norwegian',
//                   'German')
//   AND t.production_year > 2005
//   AND t.id = mi.movie_id
//   AND t.id = mk.movie_id
//   AND mk.movie_id = mi.movie_id
//   AND k.id = mk.keyword_id;
#[cfg(test)]
mod test_q3a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q3a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q3a(&data)?;
        assert_eq!(res, Some("2 Days in New York"));
        Ok(())
    }
}
