use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q15b(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
    let at = &db.at;
    let cn = &db.cn;
    // let ct = &db.ct;
    let it = &db.it;
    // let k = &db.k;
    let mc = &db.mc;
    let mi = &db.mi;
    let mk = &db.mk;
    let t = &db.t;

    let internet = Finder::new("internet");
    let usa = Finder::new("USA:");
    let two_hundred = Finder::new(" 200");
    let p_two_hundred = Finder::new("(200");
    let worldwide = Finder::new("(worldwide)");

    // Fk-PK optimization
    // let ct_s: HashSet<i32> = ct.column("id")?.i32()?.into_iter().flatten().collect();

    let start = Instant::now();

    let at_s: HashSet<&i32> = at.movie_id.iter().collect();

    let mk_s: HashSet<&i32> = mk
        .movie_id
        .iter()
        .filter_map(|movie_id| at_s.contains(movie_id).then_some(movie_id))
        .collect();

    let cn_s: HashSet<i32> = cn
        .country_code
        .iter()
        .zip(cn.id.iter())
        .zip(cn.name.iter())
        .filter_map(|((country_code, id), name)| {
            country_code
                .as_ref()
                .filter(|country_code| *country_code == "[us]" && name == "YouTube")
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

    let mi_m: HashMap<&i32, Vec<&str>> = mi
        .movie_id
        .iter()
        .zip(mi.info.iter())
        .zip(mi.info_type_id.iter())
        .zip(mi.note.iter())
        .filter_map(|(((movie_id, info), info_type_id), note)| {
            if let Some(note) = note
                && it_id == info_type_id
                && internet.find(note.as_bytes()).is_some()
                && usa.find(info.as_bytes()) == Some(0)
                && two_hundred.find(info.as_bytes()).is_some()
                && mk_s.contains(movie_id)
            {
                Some((movie_id, info))
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (movie_id, info)| {
            acc.entry(movie_id).or_default().push(info);
            acc
        });

    let t_m: HashMap<&i32, Vec<&str>> =
        t.id.iter()
            .zip(t.production_year.iter())
            .zip(t.title.iter())
            .filter_map(|((movie_id, production_year), title)| {
                if let Some(production_year) = production_year
                    && (2005..=2010).contains(production_year)
                    && mi_m.contains_key(&movie_id)
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

    let mut res: Option<(&str, &str)> = None;

    for ((mid, cid), note) in mc
        .movie_id
        .iter()
        .zip(mc.company_id.iter())
        .zip(mc.note.iter())
    {
        if let Some(note) = note
            && p_two_hundred.find(note.as_bytes()).is_some()
            && worldwide.find(note.as_bytes()).is_some()
            && cn_s.contains(&cid)
            && let Some(titles) = t_m.get(&mid)
            && let Some(info) = mi_m.get(&mid)
        {
            res = match res {
                Some((old_info, old_title)) => Some((
                    info.iter().min().unwrap().min(&old_info),
                    titles.iter().min().unwrap().min(&old_title),
                )),
                None => Some((info.iter().min().unwrap(), titles.iter().min().unwrap())),
            };
        }
    }

    println!("15b,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 15b
// SELECT MIN(mi.info) AS release_date, MIN(t.title) AS youtube_movie
// FROM aka_title AS at, company_name AS cn, company_type AS ct, info_type AS it1, keyword AS k, movie_companies AS mc, movie_info AS mi, movie_keyword AS mk, title AS t
// WHERE cn.country_code = '[us]'
// and cn.name = 'YouTube'
// AND it1.info = 'release dates'
// AND mc.note like '%(200%)%'
// and mc.note like '%(worldwide)%'
// AND mi.note like '%internet%'
// AND mi.info like 'USA:% 200%'
// AND t.production_year between 2005
// and 2010
// AND t.id = at.movie_id
// AND t.id = mi.movie_id
// AND t.id = mk.movie_id
// AND t.id = mc.movie_id
// AND mk.movie_id = mi.movie_id
// AND mk.movie_id = mc.movie_id
// AND mk.movie_id = at.movie_id
// AND mi.movie_id = mc.movie_id
// AND mi.movie_id = at.movie_id
// AND mc.movie_id = at.movie_id
// AND k.id = mk.keyword_id
// AND it1.id = mi.info_type_id
// AND cn.id = mc.company_id
// AND ct.id = mc.company_type_id;
#[cfg(test)]
mod test_q15b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q15b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q15b(&data)?;
        assert_eq!(res, Some(("USA:27 April 2007", "RoboCop vs Terminator")));
        Ok(())
    }
}
