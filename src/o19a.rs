use crate::data::Data;
use ahash::HashMap;
use ahash::HashSet;
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q19a(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
    let an = &db.an;
    let chn = &db.chn;
    let ci = &db.ci;
    let cn = &db.cn;
    let it = &db.it;
    let mc = &db.mc;
    let mi = &db.mi;
    let n = &db.n;
    let rt = &db.rt;
    let t = &db.t;

    let ang = Finder::new("Ang");
    let usa = Finder::new("(USA)");
    let worldwide = Finder::new("(worldwide)");
    let japan = Finder::new("Japan:");
    let usa_colon = Finder::new("USA:");
    let two_hundred = Finder::new("200");

    let start = Instant::now();

    let chn_s: HashSet<&i32> = chn.id.iter().collect();

    let an_s: HashSet<&i32> = an.person_id.iter().collect();

    let cn_s: HashSet<i32> = cn
        .country_code
        .iter()
        .zip(cn.id.iter())
        .filter_map(|(country_code, id)| {
            country_code
                .as_ref()
                .filter(|country_code| *country_code == "[us]")
                .map(|_| *id)
        })
        .collect();

    let it_id: &i32 = it
        .info
        .iter()
        .zip(it.id.iter())
        .find(|(info, _)| *info == "release dates")
        .map(|(_, id)| id)
        .unwrap();

    let rt_id = rt
        .role
        .iter()
        .zip(rt.id.iter())
        .find(|(role, _)| *role == "actress")
        .map(|(_, id)| id)
        .unwrap();

    let n_m: HashMap<i32, &str> =
        n.id.iter()
            .zip(n.name.iter())
            .zip(n.gender.iter())
            .filter_map(|((id, name), gender)| {
                if let Some(gender) = gender
                    && an_s.contains(&id)
                    && gender == "f"
                    && ang.find(name.as_bytes()).is_some()
                {
                    Some((*id, name.as_str()))
                } else {
                    None
                }
            })
            .collect();

    let mc_s: ahash::HashSet<&i32> = mc
        .movie_id
        .iter()
        .zip(mc.company_id.iter())
        .zip(mc.note.iter())
        .filter_map(|((movie_id, company_id), note)| {
            if let Some(note) = note
                && cn_s.contains(&company_id)
                && (usa.find(note.as_bytes()).is_some()
                    || worldwide.find(note.as_bytes()).is_some())
            {
                Some(movie_id)
            } else {
                None
            }
        })
        .collect();

    let mi_s: HashSet<&i32> = mi
        .info
        .iter()
        .zip(mi.movie_id.iter())
        .zip(mi.info_type_id.iter())
        .filter_map(|((info, movie_id), info_type_id)| {
            ((japan.find(info.as_bytes()).is_some() || usa_colon.find(info.as_bytes()).is_some())
                && two_hundred.find(info.as_bytes()).is_some()
                && it_id == info_type_id
                && mc_s.contains(movie_id))
            .then_some(movie_id)
        })
        .collect();

    let t_m: HashMap<i32, &str> =
        t.id.iter()
            .zip(t.production_year.iter())
            .zip(t.title.iter())
            .filter_map(|((movie_id, production_year), title)| {
                if let Some(production_year) = production_year
                    && (2005..=2009).contains(production_year)
                    && mi_s.contains(movie_id)
                {
                    Some((*movie_id, title.as_str()))
                } else {
                    None
                }
            })
            .collect();

    let target_note: HashSet<&str> = [
        "(voice)",
        "(voice: Japanese version)",
        "(voice) (uncredited)",
        "(voice: English version)",
    ]
    .into_iter()
    .collect();

    let mut res: Option<(&str, &str)> = None;

    for ((((mid, pid), rid), prid), note) in ci
        .movie_id
        .iter()
        .zip(ci.person_id.iter())
        .zip(ci.role_id.iter())
        .zip(ci.person_role_id.iter())
        .zip(ci.note.iter())
    {
        if let Some(prid) = prid
            && let Some(note) = note
            && target_note.contains(note.as_str())
            && rt_id == rid
            && chn_s.contains(&prid)
            && let Some(title) = t_m.get(&mid)
            && let Some(name) = n_m.get(&pid)
        {
            res = match res {
                Some((old_name, old_title)) => Some((name.min(&old_name), title.min(&old_title))),
                None => Some((name, title)),
            };
        }
    }

    println!("19a,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 19a
// SELECT MIN(n.name) AS voicing_actress, MIN(t.title) AS voiced_movie
// FROM aka_name AS an,
// char_name AS chn,
// cast_info AS ci,
// company_name AS cn,
// info_type AS it,
// movie_companies AS mc,
// movie_info AS mi,
// name AS n,
// role_type AS rt,
// title AS t
// WHERE ci.note in ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')
// AND cn.country_code = '[us]'
// AND it.info = 'release dates'
// AND mc.note is not NULL
// and (mc.note like '%(USA)%' or mc.note like '%(worldwide)%')
// AND mi.info is not null
// and (mi.info like 'Japan:%200%' or mi.info like 'USA:%200%')
// AND n.gender = 'f'
// and n.name like '%Ang%'
// AND rt.role = 'actress'
// AND t.production_year between 2005 and 2009
// AND t.id = mi.movie_id
// AND t.id = mc.movie_id
// AND t.id = ci.movie_id
// AND mc.movie_id = ci.movie_id
// AND mc.movie_id = mi.movie_id
// AND mi.movie_id = ci.movie_id
// AND cn.id = mc.company_id
// AND it.id = mi.info_type_id
// AND n.id = ci.person_id
// AND rt.id = ci.role_id
// AND n.id = an.person_id
// AND ci.person_id = an.person_id
// AND chn.id = ci.person_role_id;
#[cfg(test)]
mod test_q19a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q19a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q19a(&data)?;
        assert_eq!(res, Some(("Angeline, Moriah", "Blue Harvest")));
        Ok(())
    }
}
