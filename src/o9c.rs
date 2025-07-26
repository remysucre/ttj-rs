use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem;
use polars::prelude::*;
use std::time::Instant;

pub fn q9c(db: &Data) -> Result<Option<(&str, &str, &str, &str)>, PolarsError> {
    let an = &db.an;
    let chn = &db.chn;
    let ci = &db.ci;
    let cn = &db.cn;
    let mc = &db.mc;
    let n = &db.n;
    let rt = &db.rt;
    let t = &db.t;

    let an_m: HashMap<i32, Vec<&str>> = an
        .person_id
        .iter()
        .zip(an.name.iter())
        .map(|(id, name)| (*id, name))
        .fold(HashMap::default(), |mut acc, (id, name)| {
            acc.entry(id).or_insert_with(Vec::new).push(name);
            acc
        });

    let chn_m: HashMap<i32, Vec<&str>> = chn
        .id
        .iter()
        .zip(chn.name.iter())
        .map(|(id, name)| (*id, name))
        .fold(HashMap::default(), |mut acc, (idx, name)| {
            acc.entry(idx).or_insert_with(Vec::new).push(name);
            acc
        });

    let t_m: HashMap<i32, Vec<&str>> =
        t.id.iter()
            .zip(t.title.iter())
            .map(|(id, title)| (*id, title))
            .fold(HashMap::default(), |mut acc, (idx, title)| {
                acc.entry(idx).or_insert_with(Vec::new).push(title);
                acc
            });

    let an = memmem::Finder::new("An");

    let start = Instant::now();

    let cn_s: HashSet<i32> = cn
        .country_code
        .iter()
        .zip(cn.id.iter())
        .filter_map(|(country_code, id)| {
            country_code
                .as_ref()
                .filter(|&code| code == "[us]")
                .map(|_| *id)
        })
        .collect();

    let mc_s: HashSet<i32> = mc
        .movie_id
        .iter()
        .zip(mc.company_id.iter())
        .filter_map(|(movie_id, company_id)| (cn_s.contains(company_id)).then_some(*movie_id))
        .collect();

    let n_m: HashMap<i32, Vec<&str>> =
        n.id.iter()
            .zip(n.gender.iter())
            .zip(n.name.iter())
            .filter_map(|((id, gender), name)| {
                gender
                    .as_ref()
                    .filter(|&g| g == "f" && an.find(name.as_bytes()).is_some())
                    .map(|_| (*id, name))
            })
            .fold(HashMap::default(), |mut acc, (id, name)| {
                acc.entry(id).or_default().push(name);
                acc
            });

    let rt_s: HashSet<i32> = rt
        .role
        .iter()
        .zip(rt.id.iter())
        .filter_map(|(role, id)| (role == "actress").then_some(*id))
        .collect();

    let note_keywords: HashSet<&str> = [
        "(voice)",
        "(voice: Japanese version)",
        "(voice) (uncredited)",
        "(voice: English version)",
    ]
    .into_iter()
    .collect();

    let mut res: Option<(&str, &str, &str, &str)> = None;

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
            && mc_s.contains(movie_id)
            && rt_s.contains(role_id)
            && note_keywords.contains(note.as_str())
            && let Some(alternative_names) = an_m.get(person_id)
            && let Some(character_names) = chn_m.get(person_role_id)
            && let Some(names) = n_m.get(&person_id)
            && let Some(titles) = t_m.get(&movie_id)
        {
            res = match res {
                Some((old_alternative_name, old_character_name, old_name, old_title)) => Some((
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
                    names.iter().min().unwrap().min(&old_name),
                    titles.iter().min().unwrap().min(&old_title),
                )),
                None => Some((
                    alternative_names.iter().min().unwrap(),
                    character_names.iter().min().unwrap(),
                    names.iter().min().unwrap(),
                    titles.iter().min().unwrap(),
                )),
            };
        }
    }

    println!("9c,{:}", start.elapsed().as_secs_f32());
    Ok(res)
}

// -- JOB Query 9c
// SELECT MIN(an.name)  AS alternative_name,
// MIN(chn.name) AS voiced_character_name,
// MIN(n.name)   AS voicing_actress,
// MIN(t.title)  AS american_movie
// FROM aka_name AS an,
// char_name AS chn,
// cast_info AS ci,
// company_name AS cn,
// movie_companies AS mc,
// name AS n,
// role_type AS rt,
// title AS t
// WHERE ci.note in ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')
// AND cn.country_code = '[us]'
// AND n.gender = 'f'
// and n.name like '%An%'
// AND rt.role = 'actress'
// AND ci.movie_id = t.id
// AND t.id = mc.movie_id
// AND ci.movie_id = mc.movie_id
// AND mc.company_id = cn.id
// AND ci.role_id = rt.id
// AND n.id = ci.person_id
// AND chn.id = ci.person_role_id
// AND an.person_id = n.id
// AND an.person_id = ci.person_id;
#[cfg(test)]
mod test_q9c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q9c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q9c(&data)?;
        assert_eq!(
            res,
            Some((
                "'Annette'",
                "2nd Balladeer",
                "Alborg, Ana Esther",
                "(1975-01-20)"
            ))
        );
        Ok(())
    }
}
