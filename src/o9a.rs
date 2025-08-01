use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q9a(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let an = &db.an;
    let chn = &db.chn;
    let ci = &db.ci;
    let cn = &db.cn;
    let mc = &db.mc;
    let n = &db.n;
    let rt = &db.rt;
    let t = &db.t;

    let mut an_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (person_id, name) in an
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(an.column("name")?.str()?.into_iter())
    {
        if let (Some(person_id), Some(name)) = (person_id, name) {
            an_m.entry(person_id).or_default().push(name);
        }
    }

    let mut chn_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (person_role_id, name) in chn
        .column("id")?
        .i32()?
        .into_iter()
        .zip(chn.column("name")?.str()?.into_iter())
    {
        if let (Some(person_role_id), Some(name)) = (person_role_id, name) {
            chn_m.entry(person_role_id).or_default().push(name);
        }
    }

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

    let mc_s: HashSet<i32> = mc
        .column("note")?
        .str()?
        .into_iter()
        .zip(mc.column("movie_id")?.i32()?)
        .zip(mc.column("company_id")?.i32()?)
        .filter_map(|((note, movie_id), company_id)| {
            if let (Some(note), Some(movie_id), Some(company_id)) = (note, movie_id, company_id) {
                if cn_s.contains(&company_id)
                    && (note.contains("(USA)") || note.contains("(worldwide)"))
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

    let n_s: HashSet<i32> = n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("gender")?.str()?)
        .zip(n.column("name")?.str()?)
        .filter_map(|((id, gender), name)| {
            if let (Some(id), Some(gender), Some(name)) = (id, gender, name) {
                if gender == "f" && name.contains("Ang") {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let rt_s: HashSet<i32> = rt
        .column("role")?
        .str()?
        .into_iter()
        .zip(rt.column("id")?.i32()?)
        .filter_map(|(role, id)| {
            if let (Some(role), Some(id)) = (role, id) {
                if role == "actress" { Some(id) } else { None }
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
            if (2005..=2015).contains(&production_year) {
                t_m.entry(id).or_default().push(title);
            }
        }
    }

    let mut res: Option<(&str, &str, &str)> = None;

    for ((((movie_id, person_id), role_id), note), person_role_id) in ci
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("person_id")?.i32()?.into_iter())
        .zip(ci.column("role_id")?.i32()?.into_iter())
        .zip(ci.column("note")?.str()?.into_iter())
        .zip(ci.column("person_role_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(person_id), Some(role_id), Some(note), Some(person_role_id)) =
            (movie_id, person_id, role_id, note, person_role_id)
        {
            if mc_s.contains(&movie_id)
                && n_s.contains(&person_id)
                && rt_s.contains(&role_id)
                && (note == "(voice)"
                    || note == "(voice: Japanese version)"
                    || note == "(voice) (uncredited)"
                    || note == "(voice: English version)")
            {
                if let Some(titles) = t_m.get(&movie_id) {
                    if let Some(alternative_names) = an_m.get(&person_id) {
                        if let Some(character_names) = chn_m.get(&person_role_id) {
                            for title in titles {
                                for alternative_name in alternative_names {
                                    for character_name in character_names {
                                        if let Some((
                                            old_alternative_name,
                                            old_character_name,
                                            old_title,
                                        )) = res.as_mut()
                                        {
                                            if title < old_title {
                                                *old_title = title;
                                            }
                                            if alternative_name < old_alternative_name {
                                                *old_alternative_name = alternative_name;
                                            }
                                            if character_name < old_character_name {
                                                *old_character_name = character_name;
                                            }
                                        } else {
                                            res = Some((alternative_name, character_name, title));
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

    let duration = start.elapsed().as_secs_f32();
    println!("9a,{duration:}");

    Ok(res)
}

// SELECT MIN(an.name) AS alternative_name,
//        MIN(chn.name) AS character_name,
//        MIN(t.title) AS movie
// FROM aka_name AS an,
//      char_name AS chn,
//      cast_info AS ci,
//      company_name AS cn,
//      movie_companies AS mc,
//      name AS n,
//      role_type AS rt,
//      title AS t
// WHERE ci.note IN ('(voice)',
//                   '(voice: Japanese version)',
//                   '(voice) (uncredited)',
//                   '(voice: English version)')
//   AND cn.country_code ='[us]'
//   AND mc.note IS NOT NULL
//   AND (mc.note LIKE '%(USA)%'
//        OR mc.note LIKE '%(worldwide)%')
//   AND n.gender ='f'
//   AND n.name LIKE '%Ang%'
//   AND rt.role ='actress'
//   AND t.production_year BETWEEN 2005 AND 2015
//   AND ci.movie_id = t.id
//   AND t.id = mc.movie_id
//   AND ci.movie_id = mc.movie_id
//   AND mc.company_id = cn.id
//   AND ci.role_id = rt.id
//   AND n.id = ci.person_id
//   AND chn.id = ci.person_role_id
//   AND an.person_id = n.id
//   AND an.person_id = ci.person_id;

#[cfg(test)]
mod test_q9a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q9a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q9a(&db)?;
        assert_eq!(res, Some(("AJ", "Airport Announcer", "Blue Harvest")));
        Ok(())
    }
}
