use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q23c(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    let cc = &db.cc;
    let cct1 = &db.cct;
    let cn = &db.cn;
    // FK-PK optimization
    // let ct = &db.ct;
    let it1 = &db.it;
    let k = &db.k;
    let kt = &db.kt;
    let mc = &db.mc;
    let mi = &db.mi;
    let mk = &db.mk;
    let t = &db.t;

    let k_s: HashSet<i32> = k.column("id")?.i32()?.into_iter().flatten().collect();

    let start = Instant::now();

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

    let cct1_s: HashSet<i32> = cct1
        .column("kind")?
        .str()?
        .into_iter()
        .zip(cct1.column("id")?.i32()?.into_iter())
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if kind == "complete+verified" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let cc_s: HashSet<i32> = cc
        .column("status_id")?
        .i32()?
        .into_iter()
        .zip(cc.column("movie_id")?.i32()?.into_iter())
        .filter_map(|(status_id, movie_id)| {
            if let (Some(status_id), Some(movie_id)) = (status_id, movie_id) {
                if cct1_s.contains(&status_id) {
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
        .column("country_code")?
        .str()?
        .into_iter()
        .zip(cn.column("id")?.i32()?.into_iter())
        .filter_map(|(country_code, id)| {
            if let (Some(country_code), Some(id)) = (country_code, id) {
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

    let it1_s: HashSet<i32> = it1
        .column("info")?
        .str()?
        .into_iter()
        .zip(it1.column("id")?.i32()?.into_iter())
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
        .column("note")?
        .str()?
        .into_iter()
        .zip(mi.column("info")?.str()?.into_iter())
        .zip(mi.column("movie_id")?.i32()?.into_iter())
        .zip(mi.column("info_type_id")?.i32()?.into_iter())
        .filter_map(|(((note, info), movie_id), info_type_id)| {
            if let (Some(note), Some(info), Some(movie_id), Some(info_type_id)) =
                (note, info, movie_id, info_type_id)
            {
                if note.contains("internet")
                    && ((info.starts_with("USA:") && info.contains(" 199"))
                        || (info.starts_with("USA:") && info.contains(" 200")))
                    && it1_s.contains(&info_type_id)
                {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut kt_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (id, kind) in kt
        .column("id")?
        .i32()?
        .into_iter()
        .zip(kt.column("kind")?.str()?.into_iter())
    {
        if let (Some(id), Some(kind)) = (id, kind) {
            if matches!(kind, "movie" | "tv movie" | "video movie" | "video game") {
                kt_m.entry(id).or_default().push(kind);
            }
        }
    }

    let mut res: Option<(&str, &str)> = None;

    for (((kind_id, id), production_year), title) in t
        .column("kind_id")?
        .i32()?
        .into_iter()
        .zip(t.column("id")?.i32()?.into_iter())
        .zip(t.column("production_year")?.i32()?.into_iter())
        .zip(t.column("title")?.str()?.into_iter())
    {
        if let (Some(kind_id), Some(id), Some(production_year), Some(title)) =
            (kind_id, id, production_year, title)
        {
            if mi_s.contains(&id)
                && cc_s.contains(&id)
                && mc_s.contains(&id)
                && mk_s.contains(&id)
                && production_year > 1990
            {
                if let Some(kinds) = kt_m.get(&kind_id) {
                    for kind in kinds {
                        if let Some((old_kind, old_title)) = res.as_mut() {
                            if kind < old_kind {
                                *old_kind = kind;
                            }
                            if title < *old_title {
                                *old_title = title;
                            }
                        } else {
                            res = Some((kind, title));
                        }
                    }
                }
            }
        }
    }

    dbg!(start.elapsed());

    Ok(res)
}

// -- JOB Query 23c
// SELECT MIN(kt.kind) AS movie_kind, MIN(t.title) AS complete_us_internet_movie
// FROM complete_cast AS cc,
// comp_cast_type AS cct1,
// company_name AS cn,
// company_type AS ct,
// info_type AS it1,
// keyword AS k,
// kind_type AS kt,
// movie_companies AS mc,
// movie_info AS mi,
// movie_keyword AS mk,
// title AS t
// WHERE cct1.kind = 'complete+verified'
// AND cn.country_code = '[us]'
// AND it1.info = 'release dates'
// AND kt.kind in ('movie', 'tv movie', 'video movie', 'video game')
// AND mi.note like '%internet%'
// AND mi.info is not NULL
// and (mi.info like 'USA:% 199%' or mi.info like 'USA:% 200%')
// AND t.production_year > 1990
// AND kt.id = t.kind_id
// AND t.id = mi.movie_id
// AND t.id = mk.movie_id
// AND t.id = mc.movie_id
// AND t.id = cc.movie_id
// AND mk.movie_id = mi.movie_id
// AND mk.movie_id = mc.movie_id
// AND mk.movie_id = cc.movie_id
// AND mi.movie_id = mc.movie_id
// AND mi.movie_id = cc.movie_id
// AND mc.movie_id = cc.movie_id
// AND k.id = mk.keyword_id
// AND it1.id = mi.info_type_id
// AND cn.id = mc.company_id
// AND ct.id = mc.company_type_id
// AND cct1.id = cc.status_id;
#[cfg(test)]
mod test_23c {
    use crate::data::ImdbData;
    use crate::o23c::q23c;
    use polars::error::PolarsError;

    #[test]
    fn test_q23c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q23c(&db)?;
        assert_eq!(res, Some(("movie", "Dirt Merchant")));
        Ok(())
    }
}
