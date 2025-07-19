use crate::data::ImdbData;
use ahash::HashMap;
use ahash::HashSet;
use polars::error::PolarsError;
use std::time::Instant;

// imdb.q10a_cast_info(person_id,movie_id,person_role_id,role_id)
// |imdb_int.char_name(person_role_id)
// |imdb.q10a_title(movie_id,kind_id)
// ||imdb_int.movie_companies(movie_id,company_id,company_type_id)
// |||imdb.q10a_company_name(company_id)
// |||imdb_int.company_type(company_type_id)
// |imdb.q10a_role_type(role_id)
pub fn q10a(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
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
        .zip(chn.column("name")?.str()?)
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
        .zip(cn.column("id")?.i32()?)
        .filter_map(|(country_code, id)| {
            if let (Some(country_code), Some(id)) = (country_code, id) {
                if country_code == "[ru]" {
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
        .zip(mc.column("company_id")?.i32()?)
        .zip(mc.column("movie_id")?.i32()?)
        .filter_map(|((company_type_id, company_id), movie_id)| {
            if let (Some(company_type_id), Some(company_id), Some(movie_id)) =
                (company_type_id, company_id, movie_id)
            {
                if cn_s.contains(&company_id) && ct_s.contains(&company_type_id) {
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
        .zip(rt.column("role")?.str()?)
        .filter_map(|(id, role)| {
            if let (Some(id), Some(role)) = (id, role) {
                if role == "actor" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let mut t_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, title), production_year) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?)
        .zip(t.column("production_year")?.i32()?)
    {
        if let (Some(id), Some(title), Some(production_year)) = (id, title, production_year) {
            if mc_s.contains(&id) && production_year > 2005 {
                t_m.entry(id).or_default().push(title);
            }
        }
    }

    let mut res: Option<(&str, &str)> = None;

    for (((mid, person_role_id), role_id), note) in ci
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("person_role_id")?.i32()?)
        .zip(ci.column("role_id")?.i32()?)
        .zip(ci.column("note")?.str()?)
    {
        if let (Some(mid), Some(person_role_id), Some(role_id), Some(note)) =
            (mid, person_role_id, role_id, note)
        {
            if note.contains("(voice)") && note.contains("(uncredited)") {
                if let Some(names) = chn_m.get(&person_role_id) {
                    if let Some(titles) = t_m.get(&mid) {
                        if rt_s.contains(&role_id) {
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
    }

    println!("10a,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 10a
// SELECT MIN(chn.name) AS uncredited_voiced_character,
//        MIN(t.title) AS russian_movie
// FROM char_name AS chn,
//      cast_info AS ci,
//      company_name AS cn,
//      company_type AS ct,
//      movie_companies AS mc,
//      role_type AS rt,
//      title AS t
// WHERE ci.note LIKE '%(voice)%'
//   AND ci.note LIKE '%(uncredited)%'
//   AND cn.country_code = '[ru]'
//   AND rt.role = 'actor'
//   AND t.production_year > 2005
//   AND t.id = mc.movie_id
//   AND t.id = ci.movie_id
//   AND ci.movie_id = mc.movie_id
//   AND chn.id = ci.person_role_id
//   AND rt.id = ci.role_id
//   AND cn.id = mc.company_id
//   AND ct.id = mc.company_type_id;
#[cfg(test)]
mod test_10a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q10a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q10a(&db)?;
        assert_eq!(res, Some(("Actor", "12 Rounds")));
        Ok(())
    }
}
