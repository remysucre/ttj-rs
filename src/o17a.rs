use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem;
use polars::prelude::*;
use std::time::Instant;

pub fn q17a(db: &Data) -> Result<Option<&str>, PolarsError> {
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
    let b_finder = memmem::Finder::new("B");

    let start = Instant::now();

    let k_id = k
        .keyword
        .iter()
        .zip(k.id.iter())
        .find(|(keyword, _)| *keyword == "character-name-in-title")
        .map(|(_, id)| id)
        .unwrap();

    let mk_s: HashSet<i32> = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| (keyword_id == k_id).then_some(*movie_id))
        .collect();

    let cn_s: HashSet<i32> = cn
        .id
        .iter()
        .zip(cn.country_code.iter())
        .filter_map(|(id, country_code)| {
            country_code
                .as_ref()
                .filter(|&country_code| country_code == "[us]")
                .map(|_| *id)
        })
        .collect();

    let mc_s: HashSet<i32> = mc
        .company_id
        .iter()
        .zip(mc.movie_id.iter())
        .filter_map(|(company_id, movie_id)| {
            (mk_s.contains(&movie_id) && cn_s.contains(&company_id)).then_some(*movie_id)
        })
        .collect();

    let n_m: HashMap<i32, &str> =
        n.id.iter()
            .zip(n.name.iter())
            .filter_map(|(id, name)| {
                (b_finder.find(name.as_bytes()) == Some(0)).then_some((*id, name.as_str()))
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

    println!("17a,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 17a
// SELECT MIN(n.name) AS member_in_charnamed_american_movie,
//        MIN(n.name) AS a1
// FROM cast_info AS ci,
//      company_name AS cn,
//      keyword AS k,
//      movie_companies AS mc,
//      movie_keyword AS mk,
//      name AS n,
//      title AS t
// WHERE cn.country_code ='[us]'
//   AND k.keyword ='character-name-in-title'
//   AND n.name LIKE 'B%'
//   AND n.id = ci.person_id
//   AND ci.movie_id = t.id
//   AND t.id = mk.movie_id
//   AND mk.keyword_id = k.id
//   AND t.id = mc.movie_id
//   AND mc.company_id = cn.id
//   AND ci.movie_id = mc.movie_id
//   AND ci.movie_id = mk.movie_id
//   AND mc.movie_id = mk.movie_id;
#[cfg(test)]
mod test_q17a {
    use super::*;
    use crate::data::{Data, ImdbData};

    #[test]
    fn test_q17a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        assert_eq!(q17a(&data)?, Some("B, Khaz"));
        Ok(())
    }
}
