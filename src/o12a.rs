use std::time::Instant;

use ahash::{HashMap, HashSet};
use polars::error::PolarsError;

use crate::data::ImdbData;

// imdb_int.movie_companies(movie_id,company_id,company_type_id)
// |imdb.q12a_title(movie_id,kind_id)
// |imdb.q12a_company_name(company_id)
// |imdb.q12a_company_type(company_type_id)
// |imdb.q12a_movie_info(movie_id,info_type_id)
// ||imdb.q12a_info_type1(info_type_id)
// |imdb.q12a_movie_info_idx2(movie_id,info_type_id2)
// ||imdb.q12a_info_type2(info_type_id2)
pub fn q12a(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cn = &db.cn;
    let ct = &db.ct;
    let it1 = &db.it;
    let it2 = &db.it;
    let mc = &db.mc;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let t = &db.t;

    let start = Instant::now();

    let it1: HashSet<i32> = it1
        .column("info")?
        .str()?
        .into_iter()
        .zip(it1.column("id")?.i32()?)
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "genres" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let it2: HashSet<i32> = it2
        .column("info")?
        .str()?
        .into_iter()
        .zip(it2.column("id")?.i32()?)
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "rating" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let mut mi_idx_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((movie_id, info), info_type_id) in mi_idx
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi_idx.column("info")?.str()?.into_iter())
        .zip(mi_idx.column("info_type_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(info), Some(info_type_id)) = (movie_id, info, info_type_id) {
            if info > "8.0" && it2.contains(&info_type_id) {
                mi_idx_m.entry(movie_id).or_default().push(info);
            }
        }
    }

    let mut mi_s: HashSet<i32> = HashSet::default();

    for ((movie_id, info), info_type_id) in mi
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi.column("info")?.str()?.into_iter())
        .zip(mi.column("info_type_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(info), Some(info_type_id)) = (movie_id, info, info_type_id) {
            if matches!(info, "Drama" | "Horror") && it1.contains(&info_type_id) {
                mi_s.insert(movie_id);
            }
        }
    }

    let ct_s: HashSet<i32> = ct
        .column("kind")?
        .str()?
        .into_iter()
        .zip(ct.column("id")?.i32()?)
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if kind == "production companies" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut cn_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, name), country_code) in cn
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cn.column("name")?.str()?.into_iter())
        .zip(cn.column("country_code")?.str()?.into_iter())
    {
        if let (Some(id), Some(name), Some(country_code)) = (id, name, country_code) {
            if country_code == "[us]" {
                cn_m.entry(id).or_default().push(name);
            }
        }
    }

    let mut t_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, production_year), title) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("production_year")?.i32()?.into_iter())
        .zip(t.column("title")?.str()?.into_iter())
    {
        if let (Some(id), Some(production_year), Some(title)) = (id, production_year, title) {
            if (2005..=2008).contains(&production_year) {
                t_m.entry(id).or_default().push(title);
            }
        }
    }

    let mut res: Option<(&str, &str, &str)> = None;

    for ((movie_id, company_id), company_type_id) in mc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_id")?.i32()?.into_iter())
        .zip(mc.column("company_type_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(company_id), Some(company_type_id)) =
            (movie_id, company_id, company_type_id)
        {
            if let Some(titles) = t_m.get(&movie_id) {
                if let Some(names) = cn_m.get(&company_id) {
                    if ct_s.contains(&company_type_id) && mi_s.contains(&movie_id) {
                        if let Some(info) = mi_idx_m.get(&movie_id) {
                            for title in titles {
                                for name in names {
                                    for i in info {
                                        if let Some((old_title, old_name, old_info)) = res.as_mut()
                                        {
                                            if name < old_name {
                                                *old_name = name;
                                            }
                                            if title < old_title {
                                                *old_title = title;
                                            }
                                            if i < old_info {
                                                *old_info = i;
                                            }
                                        } else {
                                            res = Some((name, title, i));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("12a,{duration:}");

    Ok(res)
}

// JOB Query 12a
// SELECT MIN(cn.name) AS movie_company,
//        MIN(mi_idx.info) AS rating,
//        MIN(t.title) AS drama_horror_movie
// FROM company_name AS cn,
//      company_type AS ct,
//      info_type AS it1,
//      info_type AS it2,
//      movie_companies AS mc,
//      movie_info AS mi,
//      movie_info_idx AS mi_idx,
//      title AS t
// WHERE cn.country_code = '[us]'
//   AND ct.kind = 'production companies'
//   AND it1.info = 'genres'
//   AND it2.info = 'rating'
//   AND mi.info IN ('Drama',
//                   'Horror')
//   AND mi_idx.info > '8.0'
//   AND t.production_year BETWEEN 2005 AND 2008
//   AND t.id = mi.movie_id
//   AND t.id = mi_idx.movie_id
//   AND mi.info_type_id = it1.id
//   AND mi_idx.info_type_id = it2.id
//   AND t.id = mc.movie_id
//   AND ct.id = mc.company_type_id
//   AND cn.id = mc.company_id
//   AND mc.movie_id = mi.movie_id
//   AND mc.movie_id = mi_idx.movie_id
//   AND mi.movie_id = mi_idx.movie_id;
#[cfg(test)]
mod test_12a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q12a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q12a(&db)?;
        assert_eq!(res, Some(("3:20", "10th Grade Reunion Films", "8.1")));
        Ok(())
    }
}
