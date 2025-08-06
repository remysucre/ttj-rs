use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q9a(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let an = &db.an;
    let chn = &db.chn;
    let ci = &db.ci;
    let cn = &db.cn;
    let mc = &db.mc;
    let n = &db.n;
    let rt = &db.rt;
    let t = &db.t;

    let usa = Finder::new("(USA)");
    let worldwide = Finder::new("(worldwide)");
    let ang = Finder::new("Ang");

    let an_m: HashMap<&i32, Vec<&str>> = an.person_id.iter().zip(an.name.iter()).fold(
        HashMap::default(),
        |mut acc, (person_id, name)| {
            acc.entry(person_id).or_default().push(name);
            acc
        },
    );

    let mut chn_m: HashMap<&i32, Vec<&str>> =
        chn.id
            .iter()
            .zip(chn.name.iter())
            .fold(HashMap::default(), |mut acc, (chn_id, name)| {
                acc.entry(chn_id).or_default().push(name);
                acc
            });

    let start = Instant::now();

    let cn_s: HashSet<&i32> = cn
        .country_code
        .iter()
        .zip(cn.id.iter())
        .filter_map(|(country_code, id)| {
            country_code
                .as_ref()
                .filter(|code| code.as_str() == "[us]")
                .map(|_| id)
        })
        .collect();

    let mc_s: HashSet<&i32> = mc
        .movie_id
        .iter()
        .zip(mc.company_id.iter())
        .zip(mc.note.iter())
        .filter_map(|((movie_id, company_id), note)| {
            note.as_deref()
                .filter(|note| {
                    cn_s.contains(&company_id)
                        && (usa.find(note.as_bytes()).is_some()
                            || worldwide.find(note.as_bytes()).is_some())
                })
                .map(|_| movie_id)
        })
        .collect();

    let n_s: HashSet<&i32> =
        n.id.iter()
            .zip(n.gender.iter())
            .zip(n.name.iter())
            .filter_map(|((id, gender), name)| {
                gender
                    .as_deref()
                    .filter(|&gender| gender == "f" && ang.find(name.as_bytes()).is_some())
                    .map(|_| id)
            })
            .collect();

    let rt_id = rt
        .role
        .iter()
        .zip(rt.id.iter())
        .find(|(role, _)| *role == "actress")
        .map(|(_, id)| id)
        .unwrap();

    let t_m: HashMap<&i32, Vec<&str>> =
        t.id.iter()
            .zip(t.production_year.iter())
            .zip(t.title.iter())
            .filter_map(|((movie_id, production_year), title)| {
                if let Some(production_year) = production_year
                    && (2005..=2015).contains(production_year)
                {
                    Some((movie_id, title))
                } else {
                    None
                }
            })
            .fold(HashMap::default(), |mut acc, (movie_id, title)| {
                acc.entry(movie_id).or_default().push(title);
                acc
            });

    let target_note: HashSet<&str> = [
        "(voice)",
        "(voice: Japanese version)",
        "(voice) (uncredited)",
        "(voice: English version)",
    ]
    .into_iter()
    .collect();

    let mut res: Option<(&str, &str, &str)> = None;

    for ((((movie_id, person_id), role_id), note), person_role_id) in ci
        .movie_id
        .iter()
        .zip(ci.person_id.iter())
        .zip(ci.role_id.iter())
        .zip(ci.note.iter())
        .zip(ci.person_role_id.iter())
    {
        if let Some(note) = note
            && let Some(person_role_id) = person_role_id
            && mc_s.contains(&movie_id)
            && n_s.contains(&person_id)
            && rt_id == role_id
            && target_note.contains(note.as_str())
            && let Some(titles) = t_m.get(&movie_id)
            && let Some(alternative_names) = an_m.get(&person_id)
            && let Some(character_names) = chn_m.get(&person_role_id)
        {
            res = match res {
                Some((old_alternative_name, old_character_name, old_title)) => Some((
                    alternative_names
                        .iter()
                        .min()
                        .unwrap()
                        .min(&old_alternative_name),
                    character_names
                        .iter()
                        .min()
                        .unwrap()
                        .min(&old_character_name),
                    titles.iter().min().unwrap().min(&old_title),
                )),
                None => Some((
                    alternative_names.iter().min().unwrap(),
                    character_names.iter().min().unwrap(),
                    titles.iter().min().unwrap(),
                )),
            };
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
        let data = Data::new(&db);
        let res = q9a(&data)?;
        assert_eq!(res, Some(("AJ", "Airport Announcer", "Blue Harvest")));
        Ok(())
    }
}
