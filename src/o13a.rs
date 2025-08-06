use crate::data::Data;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q13a(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cn = &db.cn;
    let ct = &db.ct;
    let it = &db.it;
    let kt = &db.kt;
    let mc = &db.mc;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let t = &db.t;

    let start = Instant::now();

    let mut it1_id: &i32 = &0;
    let mut it2_id: &i32 = &0;

    for (id, info) in it.id.iter().zip(it.info.iter()) {
        if info == "rating" {
            it1_id = id;
        } else if info == "release dates" {
            it2_id = id;
        }
    }

    let mi_idx_m: ahash::HashMap<&i32, Vec<&str>> = mi_idx
        .movie_id
        .iter()
        .zip(mi_idx.info.iter())
        .zip(mi_idx.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            (it1_id == info_type_id).then_some((movie_id, info))
        })
        .fold(ahash::HashMap::default(), |mut acc, (idx, info)| {
            acc.entry(idx).or_insert_with(Vec::new).push(info);
            acc
        });

    let kt_id = kt
        .kind
        .iter()
        .zip(kt.id.iter())
        .find(|(kind, _)| *kind == "movie")
        .map(|(_, id)| id)
        .unwrap();

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.kind_id.iter())
            .filter_map(|((id, title), kind_id)| {
                (kt_id == kind_id && mi_idx_m.contains_key(&id)).then_some((id, title.as_str()))
            })
            .collect();

    let mi_m: HashMap<i32, Vec<&str>> = mi
        .movie_id
        .iter()
        .zip(mi.info.iter())
        .zip(mi.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            (it2_id == info_type_id && t_m.contains_key(&movie_id))
                .then_some((*movie_id, info.as_str()))
        })
        .fold(HashMap::default(), |mut acc, (movie_id, info)| {
            acc.entry(movie_id).or_default().push(info);
            acc
        });

    let cn_s: HashSet<&i32> = cn
        .country_code
        .iter()
        .zip(cn.id.iter())
        .filter_map(|(country_code, id)| {
            country_code
                .as_ref()
                .filter(|&code| code == "[de]")
                .map(|_| id)
        })
        .collect();

    let ct_id = ct
        .kind
        .iter()
        .zip(ct.id.iter())
        .find(|(kind, _)| *kind == "production companies")
        .map(|(_, id)| id)
        .unwrap();

    let mut res: Option<(&str, &str, &str)> = None;

    for ((movie_id, company_id), company_type_id) in mc
        .movie_id
        .iter()
        .zip(mc.company_id.iter())
        .zip(mc.company_type_id.iter())
    {
        if let Some(title) = t_m.get(&movie_id)
            && ct_id == company_type_id
            && let Some(release_dates) = mi_m.get(&movie_id)
            && cn_s.contains(&company_id)
            && let Some(info) = mi_idx_m.get(&movie_id)
        {
            res = match res {
                Some((old_name, old_info, old_title)) => Some((
                    release_dates.iter().min().unwrap().min(&old_name),
                    info.iter().min().unwrap().min(&old_info),
                    title.min(&old_title),
                )),
                None => Some((
                    release_dates.iter().min().unwrap(),
                    info.iter().min().unwrap(),
                    title,
                )),
            };
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("13a,{duration:}");

    Ok(res)
}

// SELECT MIN(mi.info) AS release_date,
//        MIN(miidx.info) AS rating,
//        MIN(t.title) AS german_movie
// FROM company_name AS cn,
//      company_type AS ct,
//      info_type AS it,
//      info_type AS it2,
//      kind_type AS kt,
//      movie_companies AS mc,
//      movie_info AS mi,
//      movie_info_idx AS miidx,
//      title AS t
// WHERE cn.country_code ='[de]'
//   AND ct.kind ='production companies'
//   AND it.info ='rating'
//   AND it2.info ='release dates'
//   AND kt.kind ='movie'
//   AND mi.movie_id = t.id
//   AND it2.id = mi.info_type_id
//   AND kt.id = t.kind_id
//   AND mc.movie_id = t.id
//   AND cn.id = mc.company_id
//   AND ct.id = mc.company_type_id
//   AND miidx.movie_id = t.id
//   AND it.id = miidx.info_type_id
//   AND mi.movie_id = miidx.movie_id
//   AND mi.movie_id = mc.movie_id
//   AND miidx.movie_id = mc.movie_id;

#[cfg(test)]
mod test_q13a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q13a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        assert_eq!(
            q13a(&data)?,
            Some(("Afghanistan:24 June 2012", "1.0", "&Me"))
        );
        Ok(())
    }
}
