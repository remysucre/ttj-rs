use crate::data::Data;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q17e(db: &Data) -> Result<Option<&str>, PolarsError> {
    let ci = &db.ci;
    let k = &db.k;
    let mk = &db.mk;
    let n = &db.n;
    let cn = &db.cn;
    let mc = &db.mc;
    // let t = &db.t;

    // FK-PK optimization: FK is ci.movie_id and PK is title.id
    // let t_s: HashSet<i32> = t
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .flatten()
    //     .collect();

    let n_m: HashMap<&i32, &str> =
        n.id.iter()
            .zip(n.name.iter())
            .map(|(id, name)| (id, name.as_str()))
            .collect();

    let start = Instant::now();

    let k_id = k
        .keyword
        .iter()
        .zip(k.id.iter())
        .find(|(keyword, _)| *keyword == "character-name-in-title")
        .map(|(_, id)| id)
        .unwrap();

    let mk_s: ahash::HashSet<i32> = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| (keyword_id == k_id).then_some(*movie_id))
        .collect();

    let cn_s: HashSet<i32> = cn
        .country_code
        .iter()
        .zip(cn.id.iter())
        .filter_map(|(country_code, id)| {
            country_code
                .as_ref()
                .filter(|country_code| *country_code == "[us]")
                .map(|_| *id)
        })
        .collect();

    let mc_s: ahash::HashSet<&i32> = mc
        .movie_id
        .iter()
        .zip(mc.company_id.iter())
        .filter_map(|(movie_id, company_id)| {
            (mk_s.contains(&movie_id) && cn_s.contains(&company_id)).then_some(movie_id)
        })
        .collect();

    let res: Option<&str> = ci
        .person_id
        .iter()
        .zip(ci.movie_id.iter())
        .filter_map(|(person_id, movie_id)| {
            if let Some(name) = n_m.get(&person_id)
                && mc_s.contains(&movie_id)
            {
                Some(*name)
            } else {
                None
            }
        })
        .min();

    println!("17e,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 17e
// SELECT MIN(n.name) AS member_in_charnamed_movie
// FROM cast_info AS ci,
// company_name AS cn,
// keyword AS k,
// movie_companies AS mc,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE cn.country_code = '[us]'
// AND k.keyword = 'character-name-in-title'
// AND n.id = ci.person_id
// AND ci.movie_id = t.id
// AND t.id = mk.movie_id
// AND mk.keyword_id = k.id
// AND t.id = mc.movie_id
// AND mc.company_id = cn.id
// AND ci.movie_id = mc.movie_id
// AND ci.movie_id = mk.movie_id
// AND mc.movie_id = mk.movie_id;
#[cfg(test)]
mod test_q17e {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q17e() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q17e(&data)?;
        assert_eq!(res, Some("$hort, Too"));
        Ok(())
    }
}
