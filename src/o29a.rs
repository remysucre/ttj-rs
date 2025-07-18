use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q29a(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let an = &db.an;
    let cc = &db.cc;
    let cct1 = &db.cct;
    let cct2 = &db.cct;
    let chn = &db.chn;
    let ci = &db.ci;
    let cn = &db.cn;
    let it = &db.it;
    let it3 = &db.it;
    let k = &db.k;
    let mc = &db.mc;
    let mi = &db.mi;
    let mk = &db.mk;
    let n = &db.n;
    let pi = &db.pi;
    let rt = &db.rt;
    let t = &db.t;

    let an_s: HashSet<i32> = an.column("person_id")?.i32()?.into_iter().flatten().collect();

    let start = Instant::now();

    let cct1_s: HashSet<i32> = cct1
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cct1.column("kind")?.str()?)
        .filter_map(|(id, kind)| {
            if let (Some(id), Some(kind)) = (id, kind) {
                if kind == "cast" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let cct2_s: HashSet<i32> = cct2
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cct2.column("kind")?.str()?)
        .filter_map(|(id, kind)| {
            if let (Some(id), Some(kind)) = (id, kind) {
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

    let mut chn_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (id, name) in chn
        .column("id")?
        .i32()?
        .into_iter()
        .zip(chn.column("name")?.str()?.into_iter())
    {
        if let (Some(id), Some(name)) = (id, name) {
            if name == "Queen" {
                chn_m.entry(id).or_default().push(name);
            }
        }
    }

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
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_id")?.i32()?)
        .filter_map(|(movie_id, company_id)| {
            if let (Some(movie_id), Some(company_id)) = (movie_id, company_id) {
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

    let it_s: HashSet<i32> = it
        .column("info")?
        .str()?
        .into_iter()
        .zip(it.column("id")?.i32()?)
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
        .zip(mi.column("info")?.str()?)
        .zip(mi.column("info_type_id")?.i32()?)
        .filter_map(|((movie_id, info), info_type_id)| {
            if let (Some(movie_id), Some(info), Some(info_type_id)) = (movie_id, info, info_type_id)
            {
                if it_s.contains(&info_type_id)
                    && (info.starts_with("Japan:") && info.contains("200")
                        || info.starts_with("USA:") && info.contains("200"))
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

    let k_s: HashSet<i32> = k
        .column("keyword")?
        .str()?
        .into_iter()
        .zip(k.column("id")?.i32()?)
        .filter_map(|(keyword, id)| {
            if let (Some(keyword), Some(id)) = (keyword, id) {
                if keyword == "computer-animation" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mk_s: HashSet<i32> = mk
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("keyword_id")?.i32()?)
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

    let mut n_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, name), gender) in n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?.into_iter())
        .zip(n.column("gender")?.str()?.into_iter())
    {
        if let (Some(id), Some(name), Some(gender)) = (id, name, gender) {
            if gender == "f" && name.contains("An") {
                n_m.entry(id).or_default().push(name);
            }
        }
    }

    let rt_s: HashSet<i32> = rt
        .column("role")?
        .str()?
        .into_iter()
        .zip(rt.column("id")?.i32()?)
        .filter_map(|(role, id)| {
            if let (Some(role), Some(id)) = (role, id) { 
                if role == "actress" {
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
            if title == "Shrek 2" && (2000..=2010).contains(&production_year) {
                t_m.entry(id).or_default().push(title);
            }
        }
    }

    let it3_s: HashSet<i32> = it3
        .column("info")?
        .str()?
        .into_iter()
        .zip(it3.column("id")?.i32()?)
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "trivia" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let pi_s: HashSet<i32> = pi
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(pi.column("info_type_id")?.i32()?)
        .filter_map(|(person_id, info_type_id)| {
            if let (Some(person_id), Some(info_type_id)) =
                (person_id, info_type_id)
            {
                if it3_s.contains(&info_type_id) {
                    Some(person_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut res: Option<(&str, &str, &str)> = None;

    for ((((movie_id, person_id), role_id), person_role_id), note) in ci
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("person_id")?.i32()?.into_iter())
        .zip(ci.column("role_id")?.i32()?.into_iter())
        .zip(ci.column("person_role_id")?.i32()?.into_iter())
        .zip(ci.column("note")?.str()?.into_iter())
    {
        if let (Some(movie_id), Some(person_id), Some(role_id), Some(person_role_id), Some(note)) =
            (movie_id, person_id, role_id, person_role_id, note)
        {
            if matches!(
                note,
                "(voice)" | "(voice) (uncredited)" | "(voice: English version)"
            ) && cc_s.contains(&movie_id)
                && mi_s.contains(&movie_id)
                && mk_s.contains(&movie_id)
                && mc_s.contains(&movie_id)
                && rt_s.contains(&role_id)
                && pi_s.contains(&person_id)
                && an_s.contains(&person_id)
            {
                if let Some(titles) = t_m.get(&movie_id) {
                    if let Some(names) = n_m.get(&person_id) {
                        if let Some(chn_names) = chn_m.get(&person_role_id) {
                            for title in titles {
                                for name in names {
                                    for chn_name in chn_names {
                                        if let Some((old_name, old_chn_name, old_title)) =
                                            res.as_mut()
                                        {
                                            if name < old_name {
                                                *old_name = name;
                                            }
                                            if chn_name < old_chn_name {
                                                *old_chn_name = chn_name;
                                            }
                                            if title < old_title {
                                                *old_title = title;
                                            }
                                        } else {
                                            res = Some((name, chn_name, title));
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
    println!("{duration:}");

    Ok(res)
}

// -- JOB Query 29a
// SELECT MIN(chn.name) AS voiced_char,
//        MIN(n.name) AS voicing_actress,
//        MIN(t.title) AS voiced_animation
// FROM aka_name AS an,
//      complete_cast AS cc,
//      comp_cast_type AS cct1,
//      comp_cast_type AS cct2,
//      char_name AS chn,
//      cast_info AS ci,
//      company_name AS cn,
//      info_type AS it,
//      info_type AS it3,
//      keyword AS k,
//      movie_companies AS mc,
//      movie_info AS mi,
//      movie_keyword AS mk,
//      name AS n,
//      person_info AS pi,
//      role_type AS rt,
//      title AS t
// WHERE cct1.kind ='cast'
//   AND cct2.kind ='complete+verified'
//   AND chn.name = 'Queen'
//   AND ci.note IN ('(voice)',
//                   '(voice) (uncredited)',
//                   '(voice: English version)')
//   AND cn.country_code ='[us]'
//   AND it.info = 'release dates'
//   AND it3.info = 'trivia'
//   AND k.keyword = 'computer-animation'
//   AND mi.info IS NOT NULL
//   AND (mi.info LIKE 'Japan:%200%'
//        OR mi.info LIKE 'USA:%200%')
//   AND n.gender ='f'
//   AND n.name LIKE '%An%'
//   AND rt.role ='actress'
//   AND t.title = 'Shrek 2'
//   AND t.production_year BETWEEN 2000 AND 2010
//   AND t.id = mi.movie_id
//   AND t.id = mc.movie_id
//   AND t.id = ci.movie_id
//   AND t.id = mk.movie_id
//   AND t.id = cc.movie_id
//   AND mc.movie_id = ci.movie_id
//   AND mc.movie_id = mi.movie_id
//   AND mc.movie_id = mk.movie_id
//   AND mc.movie_id = cc.movie_id
//   AND mi.movie_id = ci.movie_id
//   AND mi.movie_id = mk.movie_id
//   AND mi.movie_id = cc.movie_id
//   AND ci.movie_id = mk.movie_id
//   AND ci.movie_id = cc.movie_id
//   AND mk.movie_id = cc.movie_id
//   AND cn.id = mc.company_id
//   AND it.id = mi.info_type_id
//   AND n.id = ci.person_id
//   AND rt.id = ci.role_id
//   AND n.id = an.person_id
//   AND ci.person_id = an.person_id
//   AND chn.id = ci.person_role_id
//   AND n.id = pi.person_id
//   AND ci.person_id = pi.person_id
//   AND it3.id = pi.info_type_id
//   AND k.id = mk.keyword_id
//   AND cct1.id = cc.subject_id
//   AND cct2.id = cc.status_id;


#[cfg(test)]
mod test_q29a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q29a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q29a(&db)?;

        let expected = Some(("Andrews, Julie", "Queen", "Shrek 2"));

        assert_eq!(res, expected);
        Ok(())
    }
}
