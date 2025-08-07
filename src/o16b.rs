use crate::data::Data;
use ahash::{HashMap, HashMapExt, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q16b(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
    let an = &db.an;
    let ci = &db.ci;
    let cn = &db.cn;
    let k = &db.k;
    let mk = &db.mk;
    let t = &db.t;
    let mc = &db.mc;

    let an_m: HashMap<&i32, Vec<&str>> = an.person_id.iter().zip(an.name.iter()).fold(
        HashMap::<&i32, Vec<&str>>::new(),
        |mut acc, (person_id, name)| {
            acc.entry(person_id).or_default().push(name);
            acc
        },
    );

    let start = Instant::now();

    let k_id = k
        .keyword
        .iter()
        .zip(k.id.iter())
        .find(|(keyword, _)| *keyword == "character-name-in-title")
        .map(|(_, id)| id)
        .unwrap();

    let mk_s: HashSet<&i32> = mk
        .movie_id
        .iter()
        .zip(mk.keyword_id.iter())
        .filter_map(|(movie_id, keyword_id)| (keyword_id == k_id).then_some(movie_id))
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

    let mc_s: HashSet<&i32> = mc
        .company_id
        .iter()
        .zip(mc.movie_id.iter())
        .filter_map(|(company_id, movie_id)| cn_s.contains(&company_id).then_some(movie_id))
        .collect();

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .filter_map(|(movie_id, title)| {
                (mk_s.contains(&movie_id) && mc_s.contains(&movie_id))
                    .then_some((movie_id, title.as_str()))
            })
            .collect();

    let mut res: Option<(&str, &str)> = None;

    for (person_id, movie_id) in ci.person_id.iter().zip(ci.movie_id.iter()) {
        if mc_s.contains(&movie_id)
            && let Some(title) = t_m.get(&movie_id)
            && let Some(name) = an_m.get(&person_id)
        {
            res = match res {
                Some((old_name, old_title)) => Some((
                    name.iter().min().unwrap().min(&old_name),
                    title.min(&old_title),
                )),
                None => Some((name.iter().min().unwrap(), title)),
            };
        }
    }

    println!("16b,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// 16b.sql
// SELECT MIN(an.name) AS cool_actor_pseudonym,
//        MIN(t.title) AS series_named_after_char
// FROM aka_name AS an,
//      cast_info AS ci,
//      company_name AS cn,
//      keyword AS k,
//      movie_companies AS mc,
//      movie_keyword AS mk,
//      name AS n,
//      title AS t
// WHERE cn.country_code ='[us]'
//   AND k.keyword ='character-name-in-title'
//   AND an.person_id = n.id
//   AND n.id = ci.person_id
//   AND ci.movie_id = t.id
//   AND t.id = mk.movie_id
//   AND mk.keyword_id = k.id
//   AND t.id = mc.movie_id
//   AND mc.company_id = cn.id
//   AND an.person_id = ci.person_id
//   AND ci.movie_id = mc.movie_id
//   AND ci.movie_id = mk.movie_id
//   AND mc.movie_id = mk.movie_id;

#[cfg(test)]
mod test_q16b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q16b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        assert_eq!(q16b(&data)?, Some(("!!!, Toy", "& Teller")));
        Ok(())
    }
}
