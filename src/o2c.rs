use crate::data::Data;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

// * imdb_int.movie_companies(movie_id,company_id,company_type_id)
// * |imdb.q2c_company_name(company_id)
// * |imdb_int.title(movie_id,kind_id)
// * |imdb_int.movie_keyword(movie_id,keyword_id)
// * ||imdb.q2c_keyword(keyword_id)
pub fn q2c(db: &Data) -> Result<Option<&str>, PolarsError> {
    let cn = &db.cn;
    let k = &db.k;
    let mc = &db.mc;
    let mk = &db.mk;
    let t = &db.t;

    let start = Instant::now();

    let k_id = k
        .keyword
        .iter()
        .zip(k.id.iter())
        .find(|(keyword, _)| *keyword == "character-name-in-title")
        .map(|(_, id)| id)
        .unwrap();

    let mk_s = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| (keyword_id == k_id).then_some(*movie_id))
        .collect::<HashSet<_>>();

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .filter_map(|(id, title)| mk_s.contains(id).then_some((id, title.as_str())))
            .collect();

    let cn_s: HashSet<i32> = cn
        .country_code
        .iter()
        .zip(cn.id.iter())
        .filter_map(|(country_code, id)| {
            country_code
                .as_ref()
                .filter(|country_code| *country_code == "[sm]")
                .map(|_| *id)
        })
        .collect();

    let mut res: Option<&str> = None;

    for (movie_id, company_id) in mc.movie_id.iter().zip(mc.company_id.iter()) {
        if cn_s.contains(&company_id)
            && let Some(title) = t_m.get(&movie_id)
        {
            res = match res {
                Some(old_title) => Some(title.min(&old_title)),
                None => Some(title),
            };
        }
    }

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
mod test_q2c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q2c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q2c(&data)?;
        assert_eq!(res, None);
        Ok(())
    }
}
