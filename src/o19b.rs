use crate::data::Data;
use ahash::HashMap;
use ahash::HashSet;
use memchr::memmem;
use polars::prelude::*;
use std::time::Instant;

pub fn q19b(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
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

    let angel = memmem::Finder::new("Angel");
    let two_hundred = memmem::Finder::new("(200");
    let worldwide = memmem::Finder::new("(worldwide)");
    let japan = memmem::Finder::new("Japan:");
    let usa = memmem::Finder::new("(USA)");
    let usa2 = memmem::Finder::new("USA:");
    let y2008 = memmem::Finder::new("2008");
    let y2007 = memmem::Finder::new("2007");
    let kung_fu_panda = memmem::Finder::new("Kung Fu Panda");

    let start = Instant::now();

    let chn_s: HashSet<i32> = chn.id.iter().map(|id| *id).collect();

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

    let it_id = it
        .info
        .iter()
        .zip(it.id.iter())
        .find(|(info, _)| *info == "release dates")
        .map(|(_, id)| *id)
        .unwrap();

    let an_s: HashSet<i32> = an.person_id.iter().map(|id| *id).collect();

    let rt_id = rt
        .role
        .iter()
        .zip(rt.id.iter())
        .find(|(role, _)| *role == "actress")
        .map(|(_, id)| *id)
        .unwrap();

    let n_m: HashMap<i32, Vec<&str>> = n
        .id
        .iter()
        .zip(n.gender.iter())
        .zip(n.name.iter())
        .filter_map(|((id, gender), name)| {
            gender
                .as_ref()
                .filter(|gender| {
                    an_s.contains(&id) && gender == &"f" && angel.find(name.as_bytes()).is_some()
                })
                .map(|_| (*id, name))
        })
        .fold(HashMap::default(), |mut acc, (id, name)| {
            acc.entry(id).or_default().push(name);
            acc
        });

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

    let mi_s: HashSet<i32> = mi
        .movie_id
        .iter()
        .zip(mi.info.iter())
        .zip(mi.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            let info_bytes = info.as_bytes();
            (it_id == *info_type_id
                && ((japan.find(info_bytes).is_some() && y2007.find(info_bytes).is_some())
                    || (usa2.find(info_bytes).is_some() && y2008.find(info_bytes).is_some()))
                && mc_s.contains(movie_id))
            .then_some(*movie_id)
        })
        .collect::<HashSet<_>>();

    let t_m: HashMap<i32, Vec<&str>> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|((id, title), production_year)| {
                production_year
                    .filter(|production_year| {
                        kung_fu_panda.find(title.as_bytes()).is_some()
                            && (2007..=2008).contains(production_year)
                            && mi_s.contains(&id)
                    })
                    .map(|_| (*id, title))
            })
            .fold(HashMap::default(), |mut acc, (id, title)| {
                acc.entry(id).or_default().push(title);
                acc
            });

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
            && note == "(voice)"
            && rt_id == *rid
            && chn_s.contains(&prid)
            && let Some(titles) = t_m.get(&mid)
            && let Some(names) = n_m.get(&pid)
        {
            res = match res {
                Some((old_name, old_title)) => Some((
                    names.iter().min().unwrap().min(&old_name),
                    titles.iter().min().unwrap().min(&old_title),
                )),
                None => Some((names.iter().min().unwrap(), titles.iter().min().unwrap())),
            };
        }
    }

    println!("19b,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 19b
// SELECT MIN(n.name) AS voicing_actress, MIN(t.title) AS kung_fu_panda
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
// WHERE ci.note = '(voice)'
// AND cn.country_code = '[us]'
// AND it.info = 'release dates'
// AND mc.note like '%(200%)%'
// and (mc.note like '%(USA)%' or mc.note like '%(worldwide)%')
// AND mi.info is not null
// and (mi.info like 'Japan:%2007%' or mi.info like 'USA:%2008%')
// AND n.gender = 'f'
// and n.name like '%Angel%'
// AND rt.role = 'actress'
// AND t.production_year between 2007 and 2008
// and t.title like '%Kung%Fu%Panda%'
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
mod test_19b {
    use super::*;
    use crate::data::{Data, ImdbData};

    #[test]
    fn test_q19b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q19b(&data)?;
        assert_eq!(res, Some(("Jolie, Angelina", "Kung Fu Panda")));
        Ok(())
    }
}
