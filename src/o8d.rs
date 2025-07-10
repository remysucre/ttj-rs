use crate::data::ImdbData;
use ahash::HashMap;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q8d(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    let t = &db.t;
    let an = &db.an;
    let n = &db.n;
    let rt = &db.rt;
    let ci = &db.ci;
    let mc = &db.mc;
    let cn = &db.cn;

    let mut an_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (id, name) in an
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(an.column("name")?.str()?.into_iter())
    {
        if let (Some(id), Some(name)) = (id, name) {
            an_m.entry(id).or_default().push(name);
        }
    }

    let t_m: HashMap<i32, Vec<&str>> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .fold(HashMap::default(), |mut acc, (id, title)| {
            if let (Some(id), Some(title)) = (id, title) {
                acc.entry(id).or_default().push(title);
                acc
            } else {
                acc
            }
        });

    let n_s: HashSet<i32> = n.column("id")?.i32()?.into_iter().flatten().collect();

    let start = Instant::now();

    let rt_s: HashSet<i32> = rt
        .column("role")?
        .str()?
        .into_iter()
        .zip(rt.column("id")?.i32()?.into_iter())
        .filter_map(|(role, id)| {
            if let (Some(role), Some(id)) = (role, id) {
                if role == "costume designer" {
                    Some(id)
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
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("note")?.str()?.into_iter())
        .zip(mc.column("company_id")?.i32()?.into_iter())
        .filter_map(|((movie_id, note), company_id)| {
            if let (Some(movie_id), Some(note), Some(company_id)) = (movie_id, note, company_id) {
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

    let mut res: Option<(&str, &str)> = None;

    for (((movie_id, person_id), role_id), note) in ci
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("person_id")?.i32()?.into_iter())
        .zip(ci.column("role_id")?.i32()?.into_iter())
        .zip(ci.column("note")?.str()?.into_iter())
    {
        if let (Some(movie_id), Some(person_id), Some(role_id), Some(note)) =
            (movie_id, person_id, role_id, note)
        {
            if rt_s.contains(&role_id) && n_s.contains(&person_id) && mc_s.contains(&movie_id) {
                if let (Some(name), Some(title)) = (an_m.get(&person_id), t_m.get(&movie_id)) {
                    for name in name {
                        for title in title {
                            if let Some((old_name, old_title)) = res.as_mut() {
                                if *old_title > *title {
                                    *old_title = title;
                                }
                                if *old_name > *name {
                                    *old_name = name;
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

    dbg!(start.elapsed());

    Ok(res)
}

// -- JOB Query 8d
// SELECT MIN(an1.name) AS costume_designer_pseudo, MIN(t.title) AS movie_with_costumes
// FROM aka_name AS an1,
// cast_info AS ci,
// company_name AS cn,
// movie_companies AS mc,
// name AS n1,
// role_type AS rt,
// title AS t
// WHERE cn.country_code = '[us]'
// AND rt.role = 'costume designer'
// AND an1.person_id = n1.id
// AND n1.id = ci.person_id
// AND ci.movie_id = t.id
// AND t.id = mc.movie_id
// AND mc.company_id = cn.id
// AND ci.role_id = rt.id
// AND an1.person_id = ci.person_id
// AND ci.movie_id = mc.movie_id;
#[cfg(test)]
mod test_8d {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q8b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q8d(&db)?;
        assert_eq!(
            res,
            Some(("\"Jenny from the Block\"", "#1 Cheerleader Camp"))
        );
        Ok(())
    }
}
