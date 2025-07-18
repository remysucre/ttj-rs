use std::time::Instant;

use ahash::{HashMap, HashSet};
use polars::error::PolarsError;

use crate::data::ImdbData;

pub fn q12c(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cn = &db.cn;
    let ct = &db.ct;
    let it1 = &db.it;
    let it2 = &db.it;
    let mc = &db.mc;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let t = &db.t;

    let start = Instant::now();

    let it1_s: HashSet<i32> = it1
        .column("info")?
        .str()?
        .into_iter()
        .zip(it1.column("id")?.i32()?.into_iter())
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "genres" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let it2_s: HashSet<i32> = it2
        .column("info")?
        .str()?
        .into_iter()
        .zip(it2.column("id")?.i32()?.into_iter())
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
            if info > "7.0" && it2_s.contains(&info_type_id) {
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
            if matches!(info, "Drama" | "Horror" | "Western" | "Family")
                && it1_s.contains(&info_type_id)
            {
                mi_s.insert(movie_id);
            }
        }
    }

    let ct_s: HashSet<i32> = ct
        .column("kind")?
        .str()?
        .into_iter()
        .zip(ct.column("id")?.i32()?.into_iter())
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

    let cn_m: HashMap<i32, Vec<&str>> = cn
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cn.column("country_code")?.str()?.into_iter())
        .zip(cn.column("name")?.str()?.into_iter())
        .filter_map(|((id, country_code), name)| {
            if let (Some(id), Some(country_code), Some(name)) = (id, country_code, name) {
                if country_code == "[us]" {
                    Some((id, name))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (id, name)| {
            acc.entry(id).or_default().push(name);
            acc
        });

    let mut t_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, production_year), title) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("production_year")?.i32()?.into_iter())
        .zip(t.column("title")?.str()?.into_iter())
    {
        if let (Some(id), Some(production_year), Some(title)) = (id, production_year, title) {
            if (2000..=2010).contains(&production_year) {
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
            if ct_s.contains(&company_type_id) && mi_s.contains(&movie_id) {
                if let (Some(titles), Some(info), Some(name)) = (
                    t_m.get(&movie_id),
                    mi_idx_m.get(&movie_id),
                    cn_m.get(&company_id),
                ) {
                    for title in titles {
                        for i in info {
                            for name in name {
                                if let Some((old_name, old_info, old_title)) = res.as_mut() {
                                    if title < old_title {
                                        *old_title = title;
                                    }
                                    if i < old_info {
                                        *old_info = i;
                                    }
                                    if name < old_name {
                                        *old_name = name;
                                    }
                                } else {
                                    res = Some((name, i, title));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    dbg!(start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 12c
// SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS mainstream_movie
// FROM company_name AS cn,
// company_type AS ct,
// info_type AS it1,
// info_type AS it2,
// movie_companies AS mc,
// movie_info AS mi,
// movie_info_idx AS mi_idx,
// title AS t
// WHERE cn.country_code = '[us]'
// AND ct.kind = 'production companies'
// AND it1.info = 'genres'
// AND it2.info = 'rating'
// AND mi.info in ('Drama', 'Horror', 'Western', 'Family')
// AND mi_idx.info > '7.0'
// AND t.production_year between 2000 and 2010
// AND t.id = mi.movie_id
// AND t.id = mi_idx.movie_id
// AND mi.info_type_id = it1.id
// AND mi_idx.info_type_id = it2.id
// AND t.id = mc.movie_id
// AND ct.id = mc.company_type_id
// AND cn.id = mc.company_id
// AND mc.movie_id = mi.movie_id
// AND mc.movie_id = mi_idx.movie_id
// AND mi.movie_id = mi_idx.movie_id;
#[cfg(test)]
mod test_12c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q12c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q12c(&db)?;
        assert_eq!(res, Some(("\"Oh That Gus!\"", "7.1", "$1.11")));
        Ok(())
    }
}
