use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;
use crate::data::ImdbData;

pub fn q15a(db: &ImdbData) -> Result<(), PolarsError> {

    let at = &db.at;
    let cn = &db.cn;
    let ct = &db.ct;
    let it1 = &db.it;
    let k = &db.k;
    let mc = &db.mc;
    let mi = &db.mi;
    let mk = &db.mk;
    let t = &db.t;

    let at_s: HashSet<i32> = at
        .column("movie_id")?
        .i32()?
        .into_iter()
        .flatten()
        .collect();

    let ct_s: HashSet<i32> = ct
        .column("id")?
        .i32()?
        .into_iter()
        .flatten()
        .collect();

    let k_s: HashSet<i32> = k
        .column("id")?
        .i32()?
        .into_iter()
        .flatten()
        .collect();

    let mut mk_s: HashSet<i32> = HashSet::default();

    for (movie_id, keyword) in mk
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("keyword_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(keyword)) = (movie_id, keyword) {
            if k_s.contains(&keyword) {
                mk_s.insert(movie_id);
            }
        }
    }

    let ct_s: HashSet<i32> = ct
        .column("id")?
        .i32()?
        .into_iter()
        .flatten()
        .collect();

    let start = Instant::now();

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

    let mut mi_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (((movie_id, info_type_id), info), note) in mi
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi.column("info_type_id")?.i32()?.into_iter())
        .zip(mi.column("info")?.str()?
            .into_iter())
        .zip(mi.column("note")?.str()?.into_iter())
    {
        if let (Some(movie_id), Some(info_type_id), Some(info), Some(note)) = (movie_id, info_type_id, info, note) {
            if note.contains("internet") && info.starts_with("USA:") && info.contains("200") && it1_s.contains(&info_type_id) {
                mi_m.entry(movie_id).or_default().push(info);
            }
        }
    }

    let mut t_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, production_year), title) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("production_year")?.i32()?.into_iter())
        .zip(t.column("title")?.str()?.into_iter())
    {
        if let (Some(id), Some(production_year), Some(title)) = (id, production_year, title) {
            if production_year > 2000 && mi_m.contains_key(&id) && at_s.contains(&id) && mk_s.contains(&id) {
                t_m.entry(id).or_default().push(title);
            }
        }   
    }

    let mut res: Option<(&str, &str)> = None;

    for ((mid, cid), note) in mc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_id")?.i32()?.into_iter())
        .zip(mc.column("note")?.str()?.into_iter())
    {
        if let (Some(mid), Some(cid), Some(note)) = (mid, cid, note) {
            if note.contains("(200") && note.contains("(worldwide)") && cn_s.contains(&cid) {
                if let Some(titles) = t_m.get(&mid) {
                    if let Some(info) = mi_m.get(&mid) {
                        for title in titles {
                            for info in info {
                                if let Some((old_title, old_info)) = res.as_mut() {
                                    if title < old_title {
                                        *old_title = title;
                                    }
                                    if info < old_info {
                                        *old_info = info;
                                    }
                                } else {
                                    res = Some((title, info));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let duration = start.elapsed();
    dbg!("total elapsed");
    dbg!(duration);

    dbg!(res);

    Ok(())
}

// SELECT MIN(mi.info) AS release_date,
//        MIN(miidx.info) AS rating,
//        MIN(t.title) AS german_movie
// FROM company_name AS cn,
//      company_type AS ct,
//      info_type AS it,
//      info_type AS it2,
//      kind_type AS kt,
//      movie_companies AS mc,
//      movie_info AS mi,
//      movie_info_idx AS miidx,
//      title AS t
// WHERE cn.country_code ='[de]'
//   AND ct.kind ='production companies'
//   AND it.info ='rating'
//   AND it2.info ='release dates'
//   AND kt.kind ='movie'
//   AND mi.movie_id = t.id
//   AND it2.id = mi.info_type_id
//   AND kt.id = t.kind_id
//   AND mc.movie_id = t.id
//   AND cn.id = mc.company_id
//   AND ct.id = mc.company_type_id
//   AND miidx.movie_id = t.id
//   AND it.id = miidx.info_type_id
//   AND mi.movie_id = miidx.movie_id
//   AND mi.movie_id = mc.movie_id
//   AND miidx.movie_id = mc.movie_id;
