use crate::data::ImdbData;
// use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

pub fn q13c(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cn = &db.cn;
    let ct = &db.ct;
    let it1 = &db.it;
    let it2 = &db.it;
    let kt = &db.kt;
    let mc = &db.mc;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let t = &db.t;

    let start = Instant::now();

    let kt_s: HashSet<i32> = kt
        .column("kind")?
        .str()?
        .into_iter()
        .zip(kt.column("id")?.i32()?.into_iter())
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if kind == "movie" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .zip(t.column("kind_id")?.i32()?.into_iter())
        .filter_map(|((id, title), kind_id)| {
            if let (Some(id), Some(title), Some(kind_id)) = (id, title, kind_id) {
                if title != ""
                    && (title.starts_with("Champion") || title.starts_with("Loser"))
                    && kt_s.contains(&kind_id)
                {
                    Some((id, title))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let it_s: HashSet<i32> = it1
        .column("info")?
        .str()?
        .into_iter()
        .zip(it1.column("id")?.i32()?.into_iter())
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "rating" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let mi_idx_m: HashMap<i32, Vec<&str>> = mi_idx
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi_idx.column("info_type_id")?.i32()?.into_iter())
        .zip(mi_idx.column("info")?.str()?.into_iter())
        .filter_map(|(((movie_id, info_type_id), info))| {
            if let (Some(movie_id), Some(info_type_id), Some(info)) = (movie_id, info_type_id, info)
            {
                if t_m.contains_key(&movie_id) && it_s.contains(&info_type_id) {
                    Some((movie_id, info))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (movie_id, info)| {
            acc.entry(movie_id).or_default().push(info);
            acc
        });

    let it2_s: HashSet<i32> = it2
        .column("info")?
        .str()?
        .into_iter()
        .zip(it2.column("id")?.i32()?.into_iter())
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "release dates" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mi_s: HashSet<i32> = mi
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi.column("info_type_id")?.i32()?.into_iter())
        .filter_map(|(movie_id, info_type_id)| {
            if let (Some(movie_id), Some(info_type_id)) = (movie_id, info_type_id) {
                if t_m.contains_key(&movie_id) && it2_s.contains(&info_type_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let cn_m: HashMap<i32, &str> = cn
        .column("country_code")?
        .str()?
        .into_iter()
        .zip(cn.column("id")?.i32()?.into_iter())
        .zip(cn.column("name")?.str()?.into_iter())
        .filter_map(|((country_code, id), name)| {
            if let (Some(country_code), Some(id), Some(name)) = (country_code, id, name) {
                if country_code == "[us]" {
                    Some((id, name))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let ct_s: HashSet<i32> = ct
        .column("kind")?
        .str()?
        .into_iter()
        .zip(ct.column("id")?.i32()?.into_iter())
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if kind == "production companies" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut res: Option<(&str, &str, &str)> = None;

    for ((movie_id, company_id), company_type_id) in mc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_id")?.i32()?.into_iter())
        .zip(mc.column("company_type_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(company_id), Some(company_type_id)) =
            (movie_id, company_id, company_type_id)
        {
            if mi_s.contains(&movie_id) && ct_s.contains(&company_type_id) {
                if let Some(info) = mi_idx_m.get(&movie_id) {
                    if let Some(title) = t_m.get(&movie_id) {
                        if let Some(name) = cn_m.get(&company_id) {
                            for info in info {
                                if let Some((old_name, old_info, old_title)) = res.as_mut() {
                                    if title < old_title {
                                        *old_title = title;
                                    }
                                    if info < old_info {
                                        *old_info = info;
                                    }
                                    if name < old_name {
                                        *old_name = name;
                                    }
                                } else {
                                    res = Some((name, info, title));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    dbg!(start.elapsed().as_secs_f32());

    // dbg!(res);

    Ok(res)
}

// -- JOB Query 13c
// SELECT MIN(cn.name) AS producing_company, MIN(miidx.info) AS rating, MIN(t.title) AS movie_about_winning
// FROM company_name AS cn,
// company_type AS ct,
// info_type AS it,
// info_type AS it2,
// kind_type AS kt,
// movie_companies AS mc,
// movie_info AS mi,
// movie_info_idx AS miidx,
// title AS t
// WHERE cn.country_code = '[us]'
// AND ct.kind = 'production companies'
// AND it.info = 'rating'
// AND it2.info = 'release dates'
// AND kt.kind = 'movie'
// AND t.title != '' AND (t.title LIKE 'Champion%' OR t.title LIKE 'Loser%') AND mi.movie_id = t.id AND it2.id = mi.info_type_id AND kt.id = t.kind_id AND mc.movie_id = t.id AND cn.id = mc.company_id AND ct.id = mc.company_type_id AND miidx.movie_id = t.id AND it.id = miidx.info_type_id AND mi.movie_id = miidx.movie_id AND mi.movie_id = mc.movie_id AND miidx.movie_id = mc.movie_id;
#[cfg(test)]
mod test_13c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q13c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q13c(&db)?;
        assert_eq!(res, Some(("DL Sites", "1.8", "Champion")));
        Ok(())
    }
}
