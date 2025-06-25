use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;
use crate::data::ImdbData;

pub fn q10c(db: &ImdbData) -> Result<(), PolarsError> {
    let chn = &db.chn;
    let ci = &db.ci;
    let cn = &db.cn;
    let ct = &db.ct;
    let mc = &db.mc;
    let rt = &db.rt;
    let t = &db.t;

    let mut chn_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (id, name) in chn
        .column("id")?
        .i32()?
        .into_iter()
        .zip(chn.column("name")?.str()?.into_iter())
    {
        if let (Some(id), Some(name)) = (id, name) {
            chn_m.entry(id).or_default().push(name);
        }
    }

    let start = Instant::now();

    let ct_s: Vec<i32> = ct.column("id")?.i32()?.into_iter().flatten().collect();

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
        .column("company_type_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_id")?.i32()?.into_iter())
        .zip(mc.column("movie_id")?.i32()?.into_iter())
        .filter_map(|((company_type_id, company_id), movie_id)| {
            if let (Some(company_type_id), Some(company_id), Some(movie_id)) =
                (company_type_id, company_id, movie_id)
            {
                // if ct_s.contains(&company_type_id) && cn_s.contains(&company_id) {
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

    let rt_s: HashSet<i32> = rt
        .column("id")?
        .i32()?
        .into_iter()
        .filter_map(|id| id)
        .collect();

    let mut t_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, title), production_year) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .zip(t.column("production_year")?.i32()?.into_iter())
    {
        if let (Some(id), Some(title), Some(production_year)) = (id, title, production_year) {
            if mc_s.contains(&id) && production_year > 1990 {
                t_m.entry(id).or_default().push(title);
            }
        }
    }

    let mut res: Option<(&str, &str)> = None;

    for (((mid, person_role_id), role_id), note) in ci
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("person_role_id")?.i32()?.into_iter())
        .zip(ci.column("role_id")?.i32()?.into_iter())
        .zip(ci.column("note")?.str()?.into_iter())
    {
        if let (Some(mid), Some(person_role_id), Some(role_id), Some(note)) =
            (mid, person_role_id, role_id, note)
        {
            // if note.contains("(producer)") && rt_s.contains(&role_id) {
            if note.contains("(producer)") {
                if let Some(names) = chn_m.get(&person_role_id) {
                    if let Some(titles) = t_m.get(&mid) {
                        for name in names {
                            for title in titles {
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
    }

    dbg!(res);

    let duration = start.elapsed();
    dbg!("total elapsed");
    dbg!(duration);

    Ok(())
}

// -- JOB Query 10c
// SELECT MIN(chn.name) AS character, MIN(t.title) AS movie_with_american_producer
// FROM char_name AS chn, cast_info AS ci, company_name AS cn, company_type AS ct, movie_companies AS mc, role_type AS rt, title AS t
// WHERE ci.note like '%(producer)%'
// AND cn.country_code = '[us]'
// AND t.production_year
// > 1990
// AND t.id = mc.movie_id
// AND t.id = ci.movie_id
// AND ci.movie_id = mc.movie_id
// AND chn.id = ci.person_role_id
// AND rt.id = ci.role_id
// AND cn.id = mc.company_id
// AND ct.id = mc.company_type_id;
