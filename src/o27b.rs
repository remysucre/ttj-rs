use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q27b(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cc = &db.cc;
    let cct1 = &db.cct;
    let cct2 = &db.cct;
    let cn = &db.cn;
    let ct = &db.ct;
    let k = &db.k;
    let lt = &db.lt;
    let mc = &db.mc;
    let mi = &db.mi;
    let mk = &db.mk;
    let ml = &db.ml;
    let t = &db.t;

    let start = Instant::now();

    let cct1_s: HashSet<i32> = cct1
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cct1.column("kind")?.str()?.into_iter())
        .filter_map(|(id, kind)| {
            if let (Some(id), Some(kind)) = (id, kind) {
                if kind == "cast" || kind == "crew" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let cct2_s: HashSet<i32> = cct2
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cct2.column("kind")?.str()?.into_iter())
        .filter_map(|(id, kind)| {
            if let (Some(id), Some(kind)) = (id, kind) {
                if kind == "complete" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let cc_s: HashSet<i32> = cc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(cc.column("subject_id")?.i32()?.into_iter())
        .zip(cc.column("status_id")?.i32()?.into_iter())
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

    let mut cn_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, name), country_code) in cn
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cn.column("name")?.str()?.into_iter())
        .zip(cn.column("country_code")?.str()?.into_iter())
    {
        if let (Some(id), Some(name), Some(country_code)) = (id, name, country_code) {
            if country_code != "[pl]" && (name.contains("Film") || name.contains("Warner")) {
                cn_m.entry(id).or_default().push(name);
            }
        }
    }

    let ct_s: HashSet<i32> = ct
        .column("id")?
        .i32()?
        .into_iter()
        .zip(ct.column("kind")?.str()?.into_iter())
        .filter_map(|(id, kind)| {
            if let (Some(id), Some(kind)) = (id, kind) {
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

    let k_s: HashSet<i32> = k
        .column("id")?
        .i32()?
        .into_iter()
        .zip(k.column("keyword")?.str()?.into_iter())
        .filter_map(|(id, keyword)| {
            if let (Some(id), Some(keyword)) = (id, keyword) {
                if keyword == "sequel" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let mk_s: HashSet<i32> = mk
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("keyword_id")?.i32()?.into_iter())
        .filter_map(|(movie_id, keyword_id)| {
            if let (Some(movie_id), Some(keyword_id)) = (movie_id, keyword_id) {
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

    let mut lt_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (id, link) in lt
        .column("id")?
        .i32()?
        .into_iter()
        .zip(lt.column("link")?.str()?.into_iter())
    {
        if let (Some(id), Some(link)) = (id, link) {
            if link.contains("follow") {
                lt_m.entry(id).or_default().push(link);
            }
        }
    }

    let mut ml_m: HashMap<i32, Vec<i32>> = HashMap::default();

    for (movie_id, link_type_id) in ml
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ml.column("link_type_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(link_type_id)) = (movie_id, link_type_id) {
            if lt_m.contains_key(&link_type_id) {
                ml_m.entry(movie_id).or_default().push(link_type_id);
            }
        }
    }

    let mi_s: HashSet<i32> = mi
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi.column("info")?.str()?.into_iter())
        .filter_map(|(id, info)| {
            if let (Some(id), Some(info)) = (id, info) {
                if matches!(info, "Sweden" | "Germany" | "Swedish" | "German") {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut t_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, production_year), title) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("production_year")?.i32()?.into_iter())
        .zip(t.column("title")?.str()?.into_iter())
    {
        if let (Some(id), Some(production_year), Some(title)) = (id, production_year, title) {
            if mi_s.contains(&id)
                && cc_s.contains(&id)
                && mk_s.contains(&id)
                && production_year == 1998
            {
                t_m.entry(id).or_default().push(title);
            }
        }
    }

    let mut res: Option<(&str, &str, &str)> = None;

    for (((company_id, company_type_id), movie_id), note) in mc
        .column("company_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_type_id")?.i32()?.into_iter())
        .zip(mc.column("movie_id")?.i32()?.into_iter())
        .zip(mc.column("note")?.str()?.into_iter())
    {
        if let (Some(company_id), Some(company_type_id), Some(movie_id), None) =
            (company_id, company_type_id, movie_id, note)
        {
            if ct_s.contains(&company_type_id) {
                if let (Some(titles), Some(companies), Some(lt_ids)) = (
                    t_m.get(&movie_id),
                    cn_m.get(&company_id),
                    ml_m.get(&movie_id),
                ) {
                    for lt_id in lt_ids {
                        if let Some(links) = lt_m.get(lt_id) {
                            for title in titles {
                                for company in companies {
                                    for link in links {
                                        if let Some((old_company, old_link, old_title)) =
                                            res.as_mut()
                                        {
                                            if company < old_company {
                                                *old_company = company;
                                            }
                                            if link < old_link {
                                                *old_link = link;
                                            }
                                            if title < old_title {
                                                *old_title = title;
                                            }
                                        } else {
                                            res = Some((company, link, title));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    dbg!(start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 27b
// SELECT MIN(cn.name) AS producing_company, MIN(lt.link) AS link_type, MIN(t.title) AS complete_western_sequel
// FROM complete_cast AS cc,
// comp_cast_type AS cct1,
// comp_cast_type AS cct2,
// company_name AS cn,
// company_type AS ct,
// keyword AS k,
// link_type AS lt,
// movie_companies AS mc,
// movie_info AS mi,
// movie_keyword AS mk,
// movie_link AS ml,
// title AS t
// WHERE cct1.kind in ('cast', 'crew')
// AND cct2.kind = 'complete'
// AND cn.country_code !='[pl]' AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%') AND ct.kind ='production companies' AND k.keyword ='sequel' AND lt.link LIKE '%follow%' AND mc.note IS NULL AND mi.info IN ('Sweden', 'Germany','Swedish', 'German') AND t.production_year  = 1998 AND lt.id = ml.link_type_id AND ml.movie_id = t.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mc.movie_id AND mc.company_type_id = ct.id AND mc.company_id = cn.id AND mi.movie_id = t.id AND t.id = cc.movie_id AND cct1.id = cc.subject_id AND cct2.id = cc.status_id AND ml.movie_id = mk.movie_id AND ml.movie_id = mc.movie_id AND mk.movie_id = mc.movie_id AND ml.movie_id = mi.movie_id AND mk.movie_id = mi.movie_id AND mc.movie_id = mi.movie_id AND ml.movie_id = cc.movie_id AND mk.movie_id = cc.movie_id AND mc.movie_id = cc.movie_id AND mi.movie_id = cc.movie_id;
#[cfg(test)]
mod test_27b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q27b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q27b(&db)?;
        assert_eq!(
            res,
            Some(("Filmlance International AB", "followed by", "Vita nätter"))
        );
        Ok(())
    }
}
