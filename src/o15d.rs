use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q15d(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    let at = &db.at;
    let cn = &db.cn;
    // let ct = &db.ct;
    let it1 = &db.it;
    let k = &db.k;
    let mc = &db.mc;
    let mi = &db.mi;
    let mk = &db.mk;
    let t = &db.t;

    let at_m: HashMap<i32, Vec<&str>> = at
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(at.column("title")?.str()?)
        .fold(HashMap::default(), |mut acc, (id, title)| {
            if let (Some(id), Some(title)) = (id, title) {
                acc.entry(id).or_default().push(title);
                acc
            } else {
                acc
            }
        });

    let k_s: HashSet<i32> = k.column("id")?.i32()?.into_iter().flatten().collect();

    // Fk-PK optimization
    // let ct_s: HashSet<i32> = ct.column("id")?.i32()?.into_iter().flatten().collect();

    let start = Instant::now();

    let mk_s: HashSet<i32> = mk
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("keyword_id")?.i32()?)
        .filter_map(|(movie_id, keyword)| {
            if let (Some(movie_id), Some(keyword)) = (movie_id, keyword) {
                if k_s.contains(&keyword) {
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
        .zip(cn.column("id")?.i32()?)
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
        .zip(it1.column("id")?.i32()?)
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
        .zip(mi.column("info_type_id")?.i32()?)
        .zip(mi.column("note")?.str()?)
        .filter_map(|((movie_id, info_type_id), note)| {
            if let (Some(movie_id), Some(info_type_id), Some(note)) = (movie_id, info_type_id, note)
            {
                if note.contains("internet") && it1_s.contains(&info_type_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let t_m: HashMap<i32, Vec<&str>> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("production_year")?.i32()?)
        .zip(t.column("title")?.str()?)
        .filter_map(|((id, production_year), title)| {
            if let (Some(id), Some(production_year), Some(title)) = (id, production_year, title) {
                if production_year > 1990
                    && mi_s.contains(&id)
                    && at_m.contains_key(&id)
                    && mk_s.contains(&id)
                {
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

    let mut res: Option<(&str, &str)> = None;

    for (mid, cid) in mc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_id")?.i32()?.into_iter())
    {
        if let (Some(mid), Some(cid)) = (mid, cid) {
            if cn_s.contains(&cid) {
                if let (Some(titles), Some(at_titles)) = (t_m.get(&mid), at_m.get(&mid)) {
                    for title in titles {
                        for at_title in at_titles {
                            if let Some((old_at_title, old_title)) = res.as_mut() {
                                if title < old_title {
                                    *old_title = title;
                                }
                                if at_title < old_at_title {
                                    *old_at_title = at_title;
                                }
                            } else {
                                res = Some((at_title, title));
                            }
                        }
                    }
                }
            }
        }
    }

    println!("15d,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 15d
// SELECT MIN(at.title) AS aka_title, MIN(t.title) AS internet_movie_title
// FROM aka_title AS at, company_name AS cn, company_type AS ct, info_type AS it1, keyword AS k, movie_companies AS mc, movie_info AS mi, movie_keyword AS mk, title AS t
// WHERE cn.country_code = '[us]'
// AND it1.info = 'release dates'
// AND mi.note like '%internet%'
// AND t.production_year
// > 1990
// AND t.id = at.movie_id
// AND t.id = mi.movie_id
// AND t.id = mk.movie_id
// AND t.id = mc.movie_id
// AND mk.movie_id = mi.movie_id
// AND mk.movie_id = mc.movie_id
// AND mk.movie_id = at.movie_id
// AND mi.movie_id = mc.movie_id
// AND mi.movie_id = at.movie_id
// AND mc.movie_id = at.movie_id
// AND k.id = mk.keyword_id
// AND it1.id = mi.info_type_id
// AND cn.id = mc.company_id
// AND ct.id = mc.company_type_id;
#[cfg(test)]
mod test_15d {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q15d() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q15d(&db)?;
        assert_eq!(res, Some(("(Not So) Instant Photo", "06/05")));
        Ok(())
    }
}
