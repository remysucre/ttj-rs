use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q16b(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    let an = &db.an;
    let ci = &db.ci;
    let cn = &db.cn;
    let k = &db.k;
    let mk = &db.mk;
    let t = &db.t;
    let mc = &db.mc;

    let start = Instant::now();

    let k_s: HashSet<i32> = k
        .column("id")?
        .i32()?
        .into_iter()
        .zip(k.column("keyword")?.str()?)
        .filter_map(|(id, keyword)| {
            if let (Some(id), Some(keyword)) = (id, keyword) {
                if keyword == "character-name-in-title" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut mk_s: HashSet<i32> = HashSet::default();

    for (movie_id, keyword_id) in mk
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("keyword_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(keyword_id)) = (movie_id, keyword_id) {
            if k_s.contains(&keyword_id) {
                mk_s.insert(movie_id);
            }
        }
    }

    let cn_s: HashSet<i32> = cn
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cn.column("country_code")?.str()?)
        .filter_map(|(id, country_code)| {
            if let (Some(id), Some(country_code)) = (id, country_code) {
                if country_code == "[us]" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut mc_s: HashSet<i32> = HashSet::default();

    for (movie_id, company_id) in mc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(company_id)) = (movie_id, company_id) {
            if cn_s.contains(&company_id) {
                mc_s.insert(movie_id);
            }
        }
    }

    let mut t_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (movie_id, title) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
    {
        if let (Some(movie_id), Some(title)) = (movie_id, title) {
            if mk_s.contains(&movie_id) && mc_s.contains(&movie_id) {
                t_m.entry(movie_id).or_default().push(title);
            }
        }
    }

    let mut an_m: HashMap<i32, Vec<&str>> = HashMap::default();
    for (person_id, name) in an
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(an.column("name")?.str()?.into_iter())
    {
        if let (Some(person_id), Some(name)) = (person_id, name) {
            an_m.entry(person_id).or_default().push(name);
        }
    }

    let mut res: Option<(&str, &str)> = None;

    for (x, y) in ci
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("movie_id")?.i32()?.into_iter())
    {
        if let (Some(person_id), Some(movie_id)) = (x, y) {
            if let Some(ts) = t_m.get(&movie_id) {
                if let Some(names) = an_m.get(&person_id) {
                    for name in names {
                        for title in ts {
                            if let Some((old_name, old_title)) = res.as_mut() {
                                if name < old_name {
                                    *old_name = name;
                                }
                                if title < old_title {
                                    *old_title = title;
                                }
                            } else {
                                res = Some((name, title));
                            }
                        }
                    }
                }
            }
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
mod test_16b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q16b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        assert_eq!(q16b(&db)?, Some(("!!!, Toy", "& Teller")));
        Ok(())
    }
}
