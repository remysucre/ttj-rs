use crate::data::Data;
use ahash::{HashMap, HashSet};
use polars::error::PolarsError;
use std::time::Instant;

pub fn q12c(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cn = &db.cn;
    let ct = &db.ct;
    let it = &db.it;
    let mc = &db.mc;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let t = &db.t;

    let start = Instant::now();

    let mut it1_id: &i32 = &0;
    let mut it2_id: &i32 = &0;

    for (id, info) in it.id.iter().zip(it.info.iter()) {
        if info == "genres" {
            it1_id = id;
        } else if info == "rating" {
            it2_id = id;
        }
    }

    let mi_idx_m: HashMap<&i32, Vec<&str>> = mi_idx
        .movie_id
        .iter()
        .zip(mi_idx.info.iter())
        .zip(mi_idx.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            (info.as_str() > "7.0" && it2_id == info_type_id).then_some((movie_id, info))
        })
        .fold(HashMap::default(), |mut acc, (idx, info)| {
            acc.entry(idx).or_insert_with(Vec::new).push(info);
            acc
        });

    let target_info: HashSet<&str> = ["Drama", "Horror", "Western", "Family"]
        .into_iter()
        .collect();

    let mi_s: HashSet<&i32> = mi
        .movie_id
        .iter()
        .zip(mi.info.iter())
        .zip(mi.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            (target_info.contains(info.as_str()) && it1_id == info_type_id).then_some(movie_id)
        })
        .collect();

    let ct_id = ct
        .kind
        .iter()
        .zip(ct.id.iter())
        .find(|(kind, _)| *kind == "production companies")
        .map(|(_, id)| id)
        .unwrap();

    let cn_m: HashMap<&i32, Vec<&str>> = cn
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
        .fold(HashMap::default(), |mut acc, (idx, name)| {
            acc.entry(idx).or_insert_with(Vec::new).push(name);
            acc
        });

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|((id, title), production_year)| {
                if let Some(production_year) = production_year
                    && (2000..=2010).contains(production_year)
                {
                    Some((id, title.as_str()))
                } else {
                    None
                }
            })
            .collect();

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
            && let Some(names) = cn_m.get(company_id)
            && let Some(info) = mi_idx_m.get(&movie_id)
        {
            res = match res {
                Some((old_name, old_info, old_title)) => Some((
                    names.iter().min().unwrap().min(&old_name),
                    info.iter().min().unwrap().min(&old_info),
                    title.min(&old_title),
                )),
                None => Some((
                    names.iter().min().unwrap(),
                    info.iter().min().unwrap(),
                    title,
                )),
            };
        }
    }

    println!("12c,{:}", start.elapsed().as_secs_f32());

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
        let data = Data::new(&db);
        let res = q12c(&data)?;
        assert_eq!(res, Some(("\"Oh That Gus!\"", "7.1", "$1.11")));
        Ok(())
    }
}
