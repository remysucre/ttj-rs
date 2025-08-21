use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem;
use polars::prelude::*;
use std::time::Instant;

pub fn q9b(db: &Data) -> Result<Option<(&str, &str, &str, &str)>, PolarsError> {
    let an = &db.an;
    let chn = &db.chn;
    let ci = &db.ci;
    let cn = &db.cn;
    let mc = &db.mc;
    let n = &db.n;
    let rt = &db.rt;
    let t = &db.t;

    let two_hundred = memmem::Finder::new("(200");
    let usa = memmem::Finder::new("(USA)");
    let worldwide = memmem::Finder::new("(worldwide)");
    let angel = memmem::Finder::new("Angel");

    let start = Instant::now();

    let an_m: HashMap<i32, Vec<&str>> = an.person_id.iter().zip(an.name.iter()).fold(
        HashMap::default(),
        |mut acc, (person_id, name)| {
            acc.entry(*person_id).or_default().push(name);
            acc
        },
    );

    let chn_m: HashMap<&i32, &str> = chn
        .id
        .iter()
        .zip(chn.name.iter())
        .map(|(id, name)| (id, name.as_str()))
        .collect();

    let cn_s: HashSet<i32> = cn
        .country_code
        .iter()
        .zip(cn.id.iter())
        .filter_map(|(country_code, id)| {
            country_code
                .as_ref()
                .filter(|country_code| country_code == &"[us]")
                .map(|_| *id)
        })
        .collect();

    let mc_s: HashSet<i32> = mc
        .note
        .iter()
        .zip(mc.movie_id.iter())
        .zip(mc.company_id.iter())
        .filter_map(|((note, movie_id), company_id)| {
            note.as_ref()
                .filter(|note| {
                    cn_s.contains(&company_id)
                        && two_hundred.find(note.as_bytes()).is_some()
                        && (usa.find(note.as_bytes()).is_some()
                            || worldwide.find(note.as_bytes()).is_some())
                })
                .map(|_| *movie_id)
        })
        .collect();

    let n_m: HashMap<i32, &str> =
        n.id.iter()
            .zip(n.gender.iter())
            .zip(n.name.iter())
            .filter_map(|((id, gender), name)| {
                gender
                    .as_ref()
                    .filter(|gender| gender == &"f" && angel.find(name.as_bytes()).is_some())
                    .map(|_| (*id, name.as_str()))
            })
            .collect();

    let rt_s: HashSet<i32> = rt
        .role
        .iter()
        .zip(rt.id.iter())
        .filter_map(|(role, id)| (role == "actress").then_some(*id))
        .collect();

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.production_year.iter())
            .zip(t.title.iter())
            .filter_map(|((id, production_year), title)| {
                production_year
                    .filter(|production_year| (2007..=2010).contains(production_year))
                    .map(|_| (id, title.as_str()))
            })
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
            && mc_s.contains(&movie_id)
            && rt_s.contains(&role_id)
            && note == "(voice)"
            && let Some(alternative_names) = an_m.get(&person_id)
            && let Some(character_name) = chn_m.get(&person_role_id)
            && let Some(name) = n_m.get(&person_id)
            && let Some(title) = t_m.get(&movie_id)
        {
            res = match res {
                Some((old_alternative_name, old_character_name, old_name, old_title)) => Some((
                    alternative_names
                        .iter()
                        .min()
                        .unwrap()
                        .min(&old_alternative_name),
                    character_name.min(&old_character_name),
                    name.min(&old_name),
                    title.min(&old_title),
                )),
                None => Some((
                    alternative_names.iter().min().unwrap(),
                    character_name,
                    name,
                    title,
                )),
            };
        }
    }

    println!("9b,{:}", start.elapsed().as_secs_f32());
    Ok(res)
}

// -- JOB Query 9b
// SELECT MIN(an.name)  AS alternative_name,
// MIN(chn.name) AS voiced_character,
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
// WHERE ci.note = '(voice)'
// AND cn.country_code = '[us]'
// AND mc.note like '%(200%)%'
// and (mc.note like '%(USA)%' or mc.note like '%(worldwide)%')
// AND n.gender = 'f'
// and n.name like '%Angel%'
// AND rt.role = 'actress'
// AND t.production_year between 2007 and 2010
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
mod test_9b {
    use super::*;
    use crate::data::{Data, ImdbData};

    #[test]
    fn test_q9b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q9b(&data)?;
        assert_eq!(
            res,
            Some(("AJ", "Airport Announcer", "Bassett, Angela", "Blue Harvest"))
        );
        Ok(())
    }
}
