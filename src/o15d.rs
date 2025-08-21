use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q15d(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
    let at = &db.at;
    let cn = &db.cn;
    // let ct = &db.ct;
    let it = &db.it;
    // let k = &db.k;
    let mc = &db.mc;
    let mi = &db.mi;
    let mk = &db.mk;
    let t = &db.t;

    // Fk-PK optimization
    // let ct_s: HashSet<i32> = ct.column("id")?.i32()?.into_iter().flatten().collect();

    let internet = Finder::new("internet");

    let start = Instant::now();

    let at_m: HashMap<&i32, Vec<&str>> =
        at.movie_id
            .iter()
            .zip(at.title.iter())
            .fold(HashMap::default(), |mut acc, (id, title)| {
                acc.entry(id).or_default().push(title);
                acc
            });

    let mk_s: HashSet<&i32> = mk
        .movie_id
        .iter()
        .filter_map(|movie_id| at_m.contains_key(movie_id).then_some(movie_id))
        .collect();

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

    let mi_s: HashSet<&i32> = mi
        .movie_id
        .iter()
        .zip(mi.info_type_id.iter())
        .zip(mi.note.iter())
        .filter_map(|((movie_id, info_type_id), note)| {
            if let Some(note) = note
                && internet.find(note.as_bytes()).is_some()
                && it_id == info_type_id
                && mk_s.contains(movie_id)
            {
                Some(movie_id)
            } else {
                None
            }
        })
        .collect();

    let t_m: HashMap<&i32, Vec<&str>> =
        t.id.iter()
            .zip(t.production_year.iter())
            .zip(t.title.iter())
            .filter_map(|((movie_id, production_year), title)| {
                if let Some(production_year) = production_year
                    && *production_year > 1990
                    && mi_s.contains(&movie_id)
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

    for (mid, cid) in mc.movie_id.iter().zip(mc.company_id.iter()) {
        if cn_s.contains(&cid)
            && let Some(titles) = t_m.get(&mid)
            && let Some(at_titles) = at_m.get(&mid)
        {
            res = match res {
                Some((old_at_title, old_title)) => Some((
                    at_titles.iter().min().unwrap().min(&old_at_title),
                    titles.iter().min().unwrap().min(&old_title),
                )),
                None => Some((
                    at_titles.iter().min().unwrap(),
                    titles.iter().min().unwrap(),
                )),
            };
        }
    }

    println!("15d,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 15d
// SELECT MIN(at.title) AS aka_title, MIN(t.title) AS internet_movie_title
// FROM aka_title AS at, company_name AS cn, company_type AS ct, info_type AS it1, keyword AS k, movie_companies AS mc, movie_info AS mi, movie_keyword AS mk, title AS t
// WHERE cn.country_code = '[us]'
// AND it1.info = 'release dates'
// AND mi.note like '%internet%'
// AND t.production_year
// > 1990
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
mod test_q15d {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q15d() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q15d(&data)?;
        assert_eq!(res, Some(("(Not So) Instant Photo", "06/05")));
        Ok(())
    }
}
