use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;
use crate::data::ImdbData;

pub fn q17a(db: &ImdbData) -> Result<(), PolarsError> {
    let ci = &db.ci;
    let k = &db.k;
    let mk = &db.mk;
    let n = &db.n;
    let cn = &db.cn;
    let mc = &db.mc;
    let t = &db.t;

    // let t_s: HashSet<i32> = t
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .flatten()
    //     .collect();

    let start = Instant::now();

    let k_s: HashSet<i32> = k
        .column("keyword")?
        .str()?
        .into_iter()
        .zip(k.column("id")?.i32()?.into_iter())
        .filter_map(|(keyword, id)| {
            if let (Some(keyword), Some(id)) = (keyword, id) {
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

    let mk_s: HashSet<i32> = mk
        .column("keyword_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("movie_id")?.i32()?.into_iter())
        .filter_map(|(keyword_id, movie_id)| {
            if let (Some(keyword_id), Some(movie_id)) = (keyword_id, movie_id) {
                if k_s.contains(&keyword_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let cn_s: HashSet<i32> = cn
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cn.column("country_code")?.str()?.into_iter())
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

    let mc_s: HashSet<i32> = mc
        .column("company_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("movie_id")?.i32()?.into_iter())
        .filter_map(|(company_id, movie_id)| {
            if let (Some(company_id), Some(movie_id)) = (company_id, movie_id) {
                if cn_s.contains(&company_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut n_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (id, name) in n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?.into_iter())
    {
        if let (Some(id), Some(name)) = (id, name) {
            if name.starts_with('B') {
                n_m.entry(id).or_default().push(name);
            }
        }
    }

    let mut res: Option<&str> = None;

    for (pid, mid) in ci
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("movie_id")?.i32()?.into_iter())
    {
        if let (Some(pid), Some(mid)) = (pid, mid) {
            if mk_s.contains(&mid) && mc_s.contains(&mid) {
                if let Some(names) = n_m.get(&pid) {
                    for name in names {
                        if let Some(old_name) = res.as_mut() {
                            if name < old_name {
                                *old_name = name;
                            }
                        } else {
                            res = Some(name);
                        }
                    }
                }
            }
        }
    }

    dbg!(res);

    let elapsed = start.elapsed();
    println!("Elapsed time: {:?}", elapsed);

    Ok(())
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
