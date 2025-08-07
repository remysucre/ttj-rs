use crate::data::Data;
use ahash::HashSet;
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q13b(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cn = &db.cn;
    let ct = &db.ct;
    let it = &db.it;
    let kt = &db.kt;
    let mc = &db.mc;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let t = &db.t;

    let champion = Finder::new("Champion");
    let loser = Finder::new("Loser");

    let start = Instant::now();

    let kt_id = kt
        .kind
        .iter()
        .zip(kt.id.iter())
        .find(|(kind, _)| *kind == "movie")
        .map(|(_, id)| id)
        .unwrap();

    let t_m: ahash::HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.kind_id.iter())
            .filter_map(|((id, title), kind_id)| {
                (!title.is_empty()
                    && kt_id == kind_id
                    && (champion.find(title.as_bytes()).is_some()
                        || loser.find(title.as_bytes()).is_some()))
                .then_some((id, title.as_str()))
            })
            .collect();

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
            (it1_id == info_type_id && t_m.contains_key(&movie_id)).then_some((movie_id, info))
        })
        .fold(ahash::HashMap::default(), |mut acc, (idx, info)| {
            acc.entry(idx).or_insert_with(Vec::new).push(info);
            acc
        });

    let mi_s: HashSet<&i32> = mi
        .movie_id
        .iter()
        .zip(mi.info_type_id.iter())
        .filter_map(|(movie_id, info_type_id)| {
            (it2_id == info_type_id && t_m.contains_key(movie_id)).then_some(movie_id)
        })
        .collect();

    let cn_m: ahash::HashMap<&i32, Vec<&str>> = cn
        .id
        .iter()
        .zip(cn.name.iter())
        .zip(cn.country_code.iter())
        .filter_map(|((movie_id, name), country_code)| {
            country_code
                .as_ref()
                .filter(|&code| code == "[us]")
                .map(|_| (movie_id, name))
        })
        .fold(ahash::HashMap::default(), |mut acc, (idx, name)| {
            acc.entry(idx).or_insert_with(Vec::new).push(name);
            acc
        });

    let ct_id = ct
        .kind
        .iter()
        .zip(ct.id.iter())
        .find(|(kind, _)| *kind == "production companies")
        .map(|(_, id)| id)
        .unwrap();

    // let mut res: Option<(&str, &str, &str)> = None;
    //
    // for ((movie_id, company_id), company_type_id) in mc
    //     .column("movie_id")?
    //     .i32()?
    //     .into_iter()
    //     .zip(mc.column("company_id")?.i32()?.into_iter())
    //     .zip(mc.column("company_type_id")?.i32()?.into_iter())
    // {
    //     if let (Some(movie_id), Some(company_id), Some(company_type_id)) =
    //         (movie_id, company_id, company_type_id)
    //     {
    //         if mi_s.contains(&movie_id) && ct_s.contains(&company_type_id) {
    //             if let Some(info) = mi_idx_m.get(&movie_id) {
    //                 if let Some(title) = t_m.get(&movie_id) {
    //                     if let Some(name) = cn_m.get(&company_id) {
    //                         for info in info {
    //                             if let Some((old_name, old_info, old_title)) = res.as_mut() {
    //                                 if title < old_title {
    //                                     *old_title = title;
    //                                 }
    //                                 if info < old_info {
    //                                     *old_info = info;
    //                                 }
    //                                 if name < old_name {
    //                                     *old_name = name;
    //                                 }
    //                             } else {
    //                                 res = Some((name, info, title));
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }

    let mut res: Option<(&str, &str, &str)> = None;

    for ((movie_id, company_id), company_type_id) in mc
        .movie_id
        .iter()
        .zip(mc.company_id.iter())
        .zip(mc.company_type_id.iter())
    {
        if let Some(title) = t_m.get(&movie_id)
            && ct_id == company_type_id
            && mi_s.contains(&movie_id)
            && let Some(name) = cn_m.get(&company_id)
            && let Some(info) = mi_idx_m.get(&movie_id)
        {
            res = match res {
                Some((old_name, old_info, old_title)) => Some((
                    name.iter().min().unwrap().min(&old_name),
                    info.iter().min().unwrap().min(&old_info),
                    title.min(&old_title),
                )),
                None => Some((
                    name.iter().min().unwrap(),
                    info.iter().min().unwrap(),
                    title,
                )),
            };
        }
    }

    println!("13b,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 13b
// SELECT MIN(cn.name) AS producing_company, MIN(miidx.info) AS rating, MIN(t.title) AS movie_about_winning
// FROM company_name AS cn,
// company_type AS ct,
// info_type AS it,
// info_type AS it2,
// kind_type AS kt,
// movie_companies AS mc,
// movie_info AS mi,
// movie_info_idx AS miidx,
// title AS t
// WHERE cn.country_code = '[us]'
// AND ct.kind = 'production companies'
// AND it.info = 'rating'
// AND it2.info = 'release dates'
// AND kt.kind = 'movie'
// AND t.title != '' AND (t.title LIKE '%Champion%' OR t.title LIKE '%Loser%') AND mi.movie_id = t.id AND it2.id = mi.info_type_id AND kt.id = t.kind_id AND mc.movie_id = t.id AND cn.id = mc.company_id AND ct.id = mc.company_type_id AND miidx.movie_id = t.id AND it.id = miidx.info_type_id AND mi.movie_id = miidx.movie_id AND mi.movie_id = mc.movie_id AND miidx.movie_id = mc.movie_id;
#[cfg(test)]
mod test_q13b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q13b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q13b(&data)?;
        assert_eq!(res, Some(("501audio", "1.8", "5 Time Champion")));
        Ok(())
    }
}
