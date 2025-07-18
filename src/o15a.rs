use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q15a(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    let at = &db.at;
    let cn = &db.cn;
    // let ct = &db.ct;
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

    let k_s: HashSet<i32> = k.column("id")?.i32()?.into_iter().flatten().collect();

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

    // FK-PK optimization
    // let ct_s: HashSet<i32> = ct.column("id")?.i32()?.into_iter().flatten().collect();

    let start = Instant::now();

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

    let mut mi_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (((movie_id, info_type_id), info), note) in mi
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi.column("info_type_id")?.i32()?.into_iter())
        .zip(mi.column("info")?.str()?.into_iter())
        .zip(mi.column("note")?.str()?.into_iter())
    {
        if let (Some(movie_id), Some(info_type_id), Some(info), Some(note)) =
            (movie_id, info_type_id, info, note)
        {
            if note.contains("internet")
                && info.starts_with("USA:")
                && info.contains("200")
                && it1_s.contains(&info_type_id)
            {
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
            if production_year > 2000
                && mi_m.contains_key(&id)
                && at_s.contains(&id)
                && mk_s.contains(&id)
            {
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

    println!("15a,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB 15a
// SELECT MIN(mi.info) AS release_date,
// MIN(t.title) AS internet_movie
// FROM aka_title AS at,
// company_name AS cn,
// company_type AS ct,
// info_type AS it1,
// keyword AS k,
// movie_companies AS mc,
// movie_info AS mi,
// movie_keyword AS mk,
// title AS t
// WHERE cn.country_code = '[us]'
// AND it1.info = 'release dates'
// AND mc.note LIKE '%(200%)%'
// AND mc.note LIKE '%(worldwide)%'
// AND mi.note LIKE '%internet%'
// AND mi.info LIKE 'USA:% 200%'
// AND t.production_year > 2000
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
mod test_15a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q15a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q15a(&db)?;
        assert_eq!(
            res,
            Some(("Battlestar Galactica: The Resistance", "USA:1 June 2007"))
        );
        Ok(())
    }
}
