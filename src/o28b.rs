use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q28b(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cc = &db.cc;
    let cct1 = &db.cct;
    let cct2 = &db.cct;
    let cn = &db.cn;
    let ct = &db.ct;
    let it1 = &db.it;
    let it2 = &db.it;
    let k = &db.k;
    let kt = &db.kt;
    let mc = &db.mc;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let mk = &db.mk;
    let t = &db.t;

    let ct_s: HashSet<i32> = ct.column("id")?.i32()?.into_iter().flatten().collect();

    let start = Instant::now();

    let cct1_s: HashSet<i32> = cct1
        .column("kind")?
        .str()?
        .into_iter()
        .zip(cct1.column("id")?.i32()?)
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if kind == "crew" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let cct2_s: HashSet<i32> = cct2
        .column("kind")?
        .str()?
        .into_iter()
        .zip(cct2.column("id")?.i32()?)
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if kind != "complete+verified" {
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
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(cc.column("subject_id")?.i32()?)
        .zip(cc.column("status_id")?.i32()?)
        .filter_map(|((movie_id, subject_id), status_id)| {
            if let (Some(movie_id), Some(subject_id), Some(status_id)) =
                (movie_id, subject_id, status_id)
            {
                if cct1_s.contains(&subject_id) && cct2_s.contains(&status_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let cn_s: HashMap<i32, Vec<&str>> = cn
        .column("country_code")?
        .str()?
        .into_iter()
        .zip(cn.column("id")?.i32()?)
        .zip(cn.column("name")?.str()?)
        .filter_map(|((country_code, id), name)| {
            if let (Some(country_code), Some(id), Some(name)) = (country_code, id, name) {
                if country_code != "[us]" {
                    Some((id, name))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (id, name)| {
            acc.entry(id).or_default().push(name);
            acc
        });

    let it1_s: HashSet<i32> = it1
        .column("info")?
        .str()?
        .into_iter()
        .zip(it1.column("id")?.i32()?)
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "countries" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let it2_s: HashSet<i32> = it2
        .column("info")?
        .str()?
        .into_iter()
        .zip(it2.column("id")?.i32()?)
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "rating" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let k_s = k
        .column("id")?
        .i32()?
        .into_iter()
        .zip(k.column("keyword")?.str()?)
        .filter_map(|(id, keyword)| {
            if let (Some(id), Some(keyword)) = (id, keyword) {
                if matches!(keyword, "murder" | "murder-in-title" | "blood" | "violence") {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let kt_s: HashSet<i32> = kt
        .column("kind")?
        .str()?
        .into_iter()
        .zip(kt.column("id")?.i32()?)
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if matches!(kind, "movie" | "episode") {
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
        .column("info")?
        .str()?
        .into_iter()
        .zip(mi.column("movie_id")?.i32()?)
        .zip(mi.column("info_type_id")?.i32()?)
        .filter_map(|((info, movie_id), info_type_id)| {
            if let (Some(info), Some(movie_id), Some(info_type_id)) = (info, movie_id, info_type_id)
            {
                if matches!(info, "Sweden" | "Germany" | "Swedish" | "German")
                    && it1_s.contains(&info_type_id)
                    && cc_s.contains(&movie_id)
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

    let mut mi_idx_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((movie_id, info), info_type_id) in mi_idx
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi_idx.column("info")?.str()?.into_iter())
        .zip(mi_idx.column("info_type_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(info), Some(info_type_id)) = (movie_id, info, info_type_id) {
            if info > "6.5" && it2_s.contains(&info_type_id) && mi_s.contains(&movie_id) {
                mi_idx_m.entry(movie_id).or_default().push(info);
            }
        }
    }

    let mk_s = mk
        .column("keyword_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("movie_id")?.i32()?)
        .filter_map(|(keyword_id, movie_id)| {
            if let (Some(keyword_id), Some(movie_id)) = (keyword_id, movie_id) {
                if k_s.contains(&keyword_id) && mi_idx_m.contains_key(&movie_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let t_m: HashMap<i32, Vec<&str>> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("kind_id")?.i32()?)
        .zip(t.column("title")?.str()?)
        .zip(t.column("production_year")?.i32()?)
        .filter_map(|(((id, kind_id), title), production_year)| {
            if let (Some(id), Some(kind_id), Some(title), Some(production_year)) =
                (id, kind_id, title, production_year)
            {
                if production_year > 2005 && kt_s.contains(&kind_id) && mk_s.contains(&id) {
                    Some((id, title))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (id, title)| {
            acc.entry(id).or_default().push(title);
            acc
        });

    let mut res: Option<(&str, &str, &str)> = None;

    for (((movie_id, company_id), company_type_id), note) in mc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_id")?.i32()?.into_iter())
        .zip(mc.column("company_type_id")?.i32()?.into_iter())
        .zip(mc.column("note")?.str()?.into_iter())
    {
        if let (Some(movie_id), Some(company_id), Some(company_type_id), Some(note)) =
            (movie_id, company_id, company_type_id, note)
        {
            if ct_s.contains(&company_type_id) && note.contains("(200") && !note.contains("(USA)") {
                if let (Some(titles), Some(names), Some(info)) = (
                    t_m.get(&movie_id),
                    cn_s.get(&company_id),
                    mi_idx_m.get(&movie_id),
                ) {
                    for title in titles {
                        for name in names {
                            for i in info {
                                if let Some((old_name, old_info, old_title)) = res.as_mut() {
                                    if name < old_name {
                                        *old_name = *name;
                                    }
                                    if title < old_title {
                                        *old_title = *title;
                                    }
                                    if i < old_info {
                                        *old_info = *i
                                    }
                                } else {
                                    res = Some((name, i, title));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    println!("{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 28b
// SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS complete_euro_dark_movie
// FROM complete_cast AS cc,
// comp_cast_type AS cct1,
// comp_cast_type AS cct2,
// company_name AS cn,
// company_type AS ct,
// info_type AS it1,
// info_type AS it2,
// keyword AS k,
// kind_type AS kt,
// movie_companies AS mc,
// movie_info AS mi,
// movie_info_idx AS mi_idx,
// movie_keyword AS mk,
// title AS t
// WHERE cct1.kind = 'crew'
// AND cct2.kind != 'complete+verified' AND cn.country_code  != '[us]' AND it1.info  = 'countries' AND it2.info  = 'rating' AND k.keyword  in ('murder', 'murder-in-title', 'blood', 'violence') AND kt.kind  in ('movie', 'episode') AND mc.note  not like '%(USA)%' and mc.note like '%(200%)%' AND mi.info  IN ('Sweden', 'Germany', 'Swedish', 'German') AND mi_idx.info  > '6.5' AND t.production_year  > 2005 AND kt.id = t.kind_id AND t.id = mi.movie_id AND t.id = mk.movie_id AND t.id = mi_idx.movie_id AND t.id = mc.movie_id AND t.id = cc.movie_id AND mk.movie_id = mi.movie_id AND mk.movie_id = mi_idx.movie_id AND mk.movie_id = mc.movie_id AND mk.movie_id = cc.movie_id AND mi.movie_id = mi_idx.movie_id AND mi.movie_id = mc.movie_id AND mi.movie_id = cc.movie_id AND mc.movie_id = mi_idx.movie_id AND mc.movie_id = cc.movie_id AND mi_idx.movie_id = cc.movie_id AND k.id = mk.keyword_id AND it1.id = mi.info_type_id AND it2.id = mi_idx.info_type_id AND ct.id = mc.company_type_id AND cn.id = mc.company_id AND cct1.id = cc.subject_id AND cct2.id = cc.status_id;
#[cfg(test)]
mod test_28b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q28b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q28b(&db)?;
        assert_eq!(res, Some(("20th Century Fox", "6.6", "(#1.1)")));
        Ok(())
    }
}
