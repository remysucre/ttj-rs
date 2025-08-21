use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem;
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q29a(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let an = &db.an;
    let cc = &db.cc;
    let cct = &db.cct;
    let chn = &db.chn;
    let ci = &db.ci;
    let cn = &db.cn;
    let it = &db.it;
    let k = &db.k;
    let mc = &db.mc;
    let mi = &db.mi;
    let mk = &db.mk;
    let n = &db.n;
    let pi = &db.pi;
    let rt = &db.rt;
    let t = &db.t;

    let japan_colon = memmem::Finder::new("Japan:");
    let usa_colon = memmem::Finder::new("USA:");
    let two_hundred = memmem::Finder::new("200");
    let an_finder = Finder::new("An");

    let start = Instant::now();

    let an_s: HashSet<&i32> = an.person_id.iter().collect();

    let mut cct1_id: i32 = 0;
    let mut cct2_id: i32 = 0;

    for (id, kind) in cct.id.iter().zip(cct.kind.iter()) {
        if kind == "cast" {
            cct1_id = *id;
        } else if kind == "complete+verified" {
            cct2_id = *id;
        }
    }

    let cc_s: HashSet<i32> = cc
        .subject_id
        .iter()
        .zip(cc.status_id.iter())
        .zip(cc.movie_id.iter())
        .filter_map(|((subject_id, status_id), movie_id)| {
            (cct1_id == *subject_id && cct2_id == *status_id).then_some(*movie_id)
        })
        .collect();

    let chn_id = chn
        .id
        .iter()
        .zip(chn.name.iter())
        .find(|(_, name)| *name == "Queen")
        .map(|(id, _)| id)
        .unwrap();

    let cn_s: HashSet<&i32> = cn
        .id
        .iter()
        .zip(cn.country_code.iter())
        .filter_map(|(id, country_code)| {
            country_code
                .as_deref()
                .filter(|&code| code == "[us]")
                .map(|_| (id))
        })
        .collect();

    let mc_s: HashSet<&i32> = mc
        .movie_id
        .iter()
        .zip(mc.company_id.iter())
        .filter_map(|(movie_id, company_id)| {
            (cc_s.contains(&movie_id) && cn_s.contains(company_id)).then_some(movie_id)
        })
        .collect();

    let it_id: &i32 = it
        .info
        .iter()
        .zip(it.id.iter())
        .find(|(info, _)| *info == "release dates")
        .map(|(_, id)| id)
        .unwrap();

    let mi_s: HashSet<&i32> = mi
        .info
        .iter()
        .zip(mi.movie_id.iter())
        .zip(mi.info_type_id.iter())
        .filter_map(|((info, movie_id), info_type_id)| {
            ((japan_colon.find(info.as_bytes()).is_some()
                || usa_colon.find(info.as_bytes()).is_some())
                && two_hundred.find(info.as_bytes()).is_some()
                && it_id == info_type_id
                && mc_s.contains(&movie_id))
            .then_some(movie_id)
        })
        .collect();

    let k_id = k
        .keyword
        .iter()
        .zip(k.id.iter())
        .find(|(keyword, _)| *keyword == "computer-animation")
        .map(|(_, id)| id)
        .unwrap();

    let mk_s: HashSet<&i32> = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| {
            (k_id == keyword_id && mi_s.contains(&movie_id)).then_some(movie_id)
        })
        .collect();

    let it2_id = it
        .info
        .iter()
        .zip(it.id.iter())
        .find(|(info, _)| *info == "trivia")
        .map(|(_, id)| *id)
        .unwrap();

    let pi_s: HashSet<&i32> = pi
        .person_id
        .iter()
        .zip(pi.info_type_id.iter())
        .filter_map(|(person_id, info_type_id)| (it2_id == *info_type_id).then_some(person_id))
        .collect();

    let n_m: HashMap<&i32, &str> =
        n.id.iter()
            .zip(n.gender.iter())
            .zip(n.name.iter())
            .filter_map(|((id, gender), name)| {
                gender
                    .as_ref()
                    .filter(|gender| {
                        an_s.contains(&id)
                            && pi_s.contains(id)
                            && gender == &"f"
                            && an_finder.find(name.as_bytes()).is_some()
                    })
                    .map(|_| (id, name.as_str()))
            })
            .collect();

    let rt_id = rt
        .role
        .iter()
        .zip(rt.id.iter())
        .find(|(role, _)| *role == "actress")
        .map(|(_, id)| id)
        .unwrap();

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|((id, title), production_year)| {
                if let Some(production_year) = production_year
                    && (2000..=2010).contains(production_year)
                    && title == "Shrek 2"
                    && mk_s.contains(&id)
                {
                    Some((id, title.as_str()))
                } else {
                    None
                }
            })
            .collect();

    let target_note: ahash::HashSet<&str> = [
        "(voice)",
        "(voice) (uncredited)",
        "(voice: English version)",
    ]
    .into_iter()
    .collect();

    let mut res: Option<(&str, &str)> = None;

    for ((((movie_id, person_id), role_id), person_role_id), note) in ci
        .movie_id
        .iter()
        .zip(ci.person_id.iter())
        .zip(ci.role_id.iter())
        .zip(ci.person_role_id.iter())
        .zip(ci.note.iter())
    {
        if let Some(person_role_id) = person_role_id
            && let Some(note) = note
            && target_note.contains(note.as_str())
            && rt_id == role_id
            && let Some(title) = t_m.get(&movie_id)
            && let Some(name) = n_m.get(&person_id)
            && chn_id == person_role_id
        {
            res = match res {
                Some((old_name, old_title)) => Some((name.min(&old_name), title.min(&old_title))),
                None => Some((name, title)),
            };
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("29a,{duration:}");

    Ok(match res {
        Some((name, title)) => Some(("Queen", name, title)),
        _ => None,
    })
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
        let data = Data::new(&db);
        let res = q29a(&data)?;
        assert_eq!(res, Some(("Queen", "Andrews, Julie", "Shrek 2")));
        Ok(())
    }
}
