use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

// * imdb_int.movie_companies(movie_id,company_id,company_type_id)
// * |imdb.q2c_company_name(company_id)
// * |imdb_int.title(movie_id,kind_id)
// * |imdb_int.movie_keyword(movie_id,keyword_id)
// * ||imdb.q2c_keyword(keyword_id)
pub fn q2c(db: &ImdbData) -> Result<Option<&str>, PolarsError> {
    let cn = &db.cn;
    let k = &db.k;
    let mc = &db.mc;
    let mk = &db.mk;
    let t = &db.t;

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(t.column("title")?.str()?.into_no_null_iter())
        .collect();

    let start = Instant::now();

    let k_id = k
        .column("keyword")?
        .str()?
        .into_no_null_iter()
        .zip(k.column("id")?.i32()?.into_no_null_iter())
        .find(|(keyword, _)| *keyword == "character-name-in-title")
        .map(|(_, id)| id)
        .unwrap();

    let mk_s = mk
        .column("keyword_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mk.column("movie_id")?.i32()?.into_no_null_iter())
        .filter_map(|(keyword_id, movie_id)| {
            (k_id == keyword_id && t_m.contains_key(&movie_id)).then_some(movie_id)
        })
        .collect::<HashSet<_>>();

    let cn_id = cn
        .column("country_code")?
        .str()?
        .into_no_null_iter()
        .zip(k.column("id")?.i32()?.into_no_null_iter())
        .find(|(country_code, _)| *country_code == "[sm]")
        .map(|(_, id)| id)
        .unwrap();

    let mc_s: HashSet<i32> = mc
        .column("movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mc.column("company_id")?.i32()?.into_no_null_iter())
        .filter_map(|(movie_id, company_id)| {
            (company_id == cn_id && mk_s.contains(&movie_id)).then_some(movie_id)
        })
        .collect();

    let res: Option<&str> = t
        .column("title")?
        .str()?
        .into_no_null_iter()
        .zip(t.column("id")?.i32()?.into_no_null_iter())
        .filter(|(_, movie_id)| mc_s.contains(movie_id))
        .min_by_key(|(_, movie_id)| t_m[movie_id])
        .map(|(title, _)| title);

    println!("2c,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 2c
// SELECT MIN(t.title) AS movie_title
// FROM company_name AS cn,
// keyword AS k,
// movie_companies AS mc,
// movie_keyword AS mk,
// title AS t
// WHERE cn.country_code = '[sm]'
// AND k.keyword = 'character-name-in-title'
// AND cn.id = mc.company_id
// AND mc.movie_id = t.id
// AND t.id = mk.movie_id
// AND mk.keyword_id = k.id
// AND mc.movie_id = mk.movie_id;
#[cfg(test)]
mod test_2c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q2c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q2c(&db)?;
        assert_eq!(res, None);
        Ok(())
    }
}
