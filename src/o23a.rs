use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q23a(db: &ImdbData) -> Result<(), PolarsError> {
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

    let start = Instant::now();

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
            if kind == "movie" {
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
                && production_year > 2000
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

    // let cn = &db.cn;
    // let ct = &db.ct;
    // let k = &db.k;
    // let lt = &db.lt;
    // let mc = &db.mc;
    // let mi = &db.mi;
    // let mk = &db.mk;
    // let ml = &db.ml;
    // let t = &db.t;

    // let start = Instant::now();

    // let mut cn_m: HashMap<i32, Vec<&str>> = HashMap::default();

    // for ((id, country_code), name) in cn
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .zip(cn.column("country_code")?.str()?.into_iter())
    //     .zip(cn.column("name")?.str()?.into_iter())
    // {
    //     if let (Some(id), Some(country_code), Some(name)) = (id, country_code, name) {
    //         if country_code != "[pl]" && (name.contains("Film") || name.contains("Warner")) {
    //             cn_m.entry(id).or_default().push(name);
    //         }
    //     }
    // }

    // let ct_s: HashSet<i32> = ct
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .zip(ct.column("kind")?.str()?.into_iter())
    //     .filter_map(|(id, kind)| {
    //         if let (Some(id), Some(kind)) = (id, kind) {
    //             if kind == "production companies" { Some(id) } else { None }
    //         } else {
    //             None
    //         }
    //     })
    //     .collect();

    // let k_s: HashSet<i32> = k
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .zip(k.column("keyword")?.str()?.into_iter())
    //     .filter_map(|(id, keyword)| {
    //         if let (Some(id), Some(keyword)) = (id, keyword) {
    //             if keyword == "sequel" { Some(id) } else { None }
    //         } else {
    //             None
    //         }
    //     })
    //     .collect();

    // let mk_s: HashSet<i32> = mk
    //     .column("movie_id")?
    //     .i32()?
    //     .into_iter()
    //     .zip(mk.column("keyword_id")?.i32()?.into_iter())
    //     .filter_map(|(movie_id, keyword_id)| {
    //         if let (Some(movie_id), Some(keyword_id)) = (movie_id, keyword_id) {
    //             if k_s.contains(&keyword_id) { Some(movie_id) } else { None }
    //         } else {
    //             None
    //         }
    //     })
    //     .collect();

    // let mut lt_m: HashMap<i32, Vec<&str>> = HashMap::default();

    // for (id, link) in lt
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .zip(lt.column("link")?.str()?.into_iter())
    // {
    //     if let (Some(id), Some(link)) = (id, link) {
    //         if link.contains("follow") {
    //             lt_m.entry(id).or_default().push(link);
    //         }
    //     }
    // }

    // let mut ml_m: HashMap<i32, Vec<i32>> = HashMap::default();

    // for (movie_id, link_type_id) in ml
    //     .column("movie_id")?
    //     .i32()?
    //     .into_iter()
    //     .zip(ml.column("link_type_id")?.i32()?.into_iter())
    // {
    //     if let (Some(movie_id), Some(link_type_id)) = (movie_id, link_type_id) {
    //         ml_m.entry(movie_id).or_default().push(link_type_id);
    //     }
    // }

    // let mi_s: HashSet<i32> = mi
    //     .column("info")?
    //     .str()?
    //     .into_iter()
    //     .zip(mi.column("movie_id")?.i32()?.into_iter())
    //     .filter_map(|(info, movie_id)| {
    //         if let (Some(info), Some(movie_id)) = (info, movie_id) {
    //             if matches!(
    //                 info,
    //                 "Sweden" | "Norway" | "Germany" | "Denmark" | "Swedish" | "Denish"
    //                     | "Norwegian" | "German"
    //             ) {
    //                 Some(movie_id)
    //             } else {
    //                 None
    //             }
    //         } else {
    //             None
    //         }
    //     })
    //     .collect();

    // let mut t_m: HashMap<i32, Vec<&str>> = HashMap::default();

    // for ((id, production_year), title) in t
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .zip(t.column("production_year")?.i32()?.into_iter())
    //     .zip(t.column("title")?.str()?.into_iter())
    // {
    //     if let (Some(id), Some(production_year), Some(title)) = (id, production_year, title) {
    //         if mk_s.contains(&id) && production_year >= 1950 && production_year <= 2000 {
    //             t_m.entry(id).or_default().push(title);
    //         }
    //     }
    // }

    // let mut res: Option<(&str, &str, &str)> = None;

    // for (((movie_id, company_id), company_type_id), note) in mc
    //     .column("movie_id")?
    //     .i32()?
    //     .into_iter()
    //     .zip(mc.column("company_id")?.i32()?.into_iter())
    //     .zip(mc.column("company_type_id")?.i32()?.into_iter())
    //     .zip(mc.column("note")?.str()?.into_iter())
    // {
    //     if let (Some(movie_id), Some(company_id), Some(company_type_id), None) =
    //         (movie_id, company_id, company_type_id, note)
    //     {
    //         if ct_s.contains(&company_type_id) && mi_s.contains(&movie_id) {
    //             if let Some(lt_ids) = ml_m.get(&movie_id) {
    //                 for lt_id in lt_ids {
    //                     if let Some(links) = lt_m.get(lt_id) {
    //                         if let Some(titles) = t_m.get(&movie_id) {
    //                             if let Some(names) = cn_m.get(&company_id) {
    //                                 for link in links {
    //                                     for title in titles {
    //                                         for name in names {
    //                                             if let Some((old_name, old_link, old_title)) = res.as_mut() {
    //                                                 if name < old_name {
    //                                                     *old_name = name;
    //                                                 }
    //                                                 if link < old_link {
    //                                                     *old_link = link;
    //                                                 }
    //                                                 if title < old_title {
    //                                                     *old_title = title;
    //                                                 }
    //                                             } else {
    //                                                 res = Some((name, link, title));
    //                                             }
    //                                         }
    //                                     }
    //                                 }
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }

    // // for ((x, y), z) in t
    // //     .column("id")?
    // //     .i32()?
    // //     .into_iter()
    // //     .zip(t.column("title")?.str()?.into_iter())
    // //     .zip(t.column("production_year")?.i32()?.into_iter())
    // // {
    // //     if let (Some(id), Some(title), Some(production_year)) = (x, y, z) {
    // //         if mi_idx_s.contains(&id) && mc_s.contains(&id) {
    // //             if let Some((old_title, old_year)) = res.as_mut() {
    // //                 if production_year < *old_year {
    // //                     *old_year = production_year;
    // //                 }
    // //                 if title < *old_title {
    // //                     *old_title = title;
    // //                 }
    // //             } else {
    // //                 res = Some((title, production_year));
    // //             }
    // //         }
    // //     }
    // // }

    // println!("{:}", res);
    let duration = start.elapsed().as_secs_f32();
    println!("{:}", duration);

    Ok(())
}

// -- JOB Query 23a
// SELECT MIN(kt.kind) AS movie_kind,
//        MIN(t.title) AS complete_us_internet_movie
// FROM complete_cast AS cc,
//      comp_cast_type AS cct1,
//      company_name AS cn,
//      company_type AS ct,
//      info_type AS it1,
//      keyword AS k,
//      kind_type AS kt,
//      movie_companies AS mc,
//      movie_info AS mi,
//      movie_keyword AS mk,
//      title AS t
// WHERE cct1.kind = 'complete+verified'
//   AND cn.country_code = '[us]'
//   AND it1.info = 'release dates'
//   AND kt.kind IN ('movie')
//   AND mi.note LIKE '%internet%'
//   AND mi.info IS NOT NULL
//   AND (mi.info LIKE 'USA:% 199%'
//        OR mi.info LIKE 'USA:% 200%')
//   AND t.production_year > 2000
//   AND kt.id = t.kind_id
//   AND t.id = mi.movie_id
//   AND t.id = mk.movie_id
//   AND t.id = mc.movie_id
//   AND t.id = cc.movie_id
//   AND mk.movie_id = mi.movie_id
//   AND mk.movie_id = mc.movie_id
//   AND mk.movie_id = cc.movie_id
//   AND mi.movie_id = mc.movie_id
//   AND mi.movie_id = cc.movie_id
//   AND mc.movie_id = cc.movie_id
//   AND k.id = mk.keyword_id
//   AND it1.id = mi.info_type_id
//   AND cn.id = mc.company_id
//   AND ct.id = mc.company_type_id
//   AND cct1.id = cc.status_id;
