use crate::data::Data;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

// * imdb_int.movie_keyword(movie_id,keyword_id)
// * |imdb.q3c_title(movie_id,kind_id)
// * |imdb.q3c_keyword(keyword_id)
// * |imdb.q3c_movie_info(movie_id,info_type_id)
pub fn q3c(db: &Data) -> Result<Option<&str>, PolarsError> {
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
        "USA",
        "American",
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
            && production_year > &1990
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
    println!("3c,{duration:}");

    Ok(res)
}

// -- JOB Query 3c
// SELECT MIN(t.title) AS movie_title
// FROM keyword AS k,
// movie_info AS mi,
// movie_keyword AS mk,
// title AS t
// WHERE k.keyword like '%sequel%'
// AND mi.info IN
// ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German', 'USA', 'American')
// AND t.production_year > 1990
// AND t.id = mi.movie_id
// AND t.id = mk.movie_id
// AND mk.movie_id = mi.movie_id
// AND k.id = mk.keyword_id;
#[cfg(test)]
mod test_q3c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q3c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q3c(&data)?;
        assert_eq!(res, Some("& Teller 2"));
        Ok(())
    }
}
