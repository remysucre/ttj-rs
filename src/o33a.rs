use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

#[allow(clippy::type_complexity)]
pub fn q33a(db: &ImdbData) -> Result<Option<(&str, &str, &str, &str, &str, &str)>, PolarsError> {
    let cn = &db.cn;
    let it = &db.it;
    let kt = &db.kt;
    let lt = &db.lt;
    let mc = &db.mc;
    let mi_idx = &db.mi_idx;
    let ml = &db.ml;
    let t = &db.t;

    // let cn2_m: HashMap<i32, &str> = cn
    //     .column("id")?
    //     .i32()?
    //     .into_no_null_iter()
    //     .zip(cn.column("name")?.str()?.into_no_null_iter())
    //     .collect();
    //
    // let mc2_m: HashMap<i32, i32> = mc
    //     .column("movie_id")?
    //     .i32()?
    //     .into_no_null_iter()
    //     .zip(mc.column("company_id")?.i32()?.into_no_null_iter())
    //     .collect();

    let start = Instant::now();

    let cn1_m: HashMap<i32, &str> = cn
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(cn.column("name")?.str()?.into_no_null_iter())
        .zip(cn.column("country_code")?.str()?.into_no_null_iter())
        .filter_map(|((id, name), country_code)| (country_code == "[us]").then_some((id, name)))
        .collect();

    let it1_s: i32 = it
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(it.column("info")?.str()?.into_no_null_iter())
        .find(|(_, info)| *info == "rating")
        .map(|(id, _)| id)
        .unwrap();
    // let it2_s: i32 = it1_s;

    let mut mi_idx1_m: HashMap<i32, &str> = HashMap::default();
    let mut mi_idx2_m: HashMap<i32, &str> = HashMap::default();

    for ((it_id, info), movie_id) in mi_idx
        .column("info_type_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mi_idx.column("info")?.str()?.into_no_null_iter())
        .zip(mi_idx.column("movie_id")?.i32()?.into_no_null_iter())
    {
        if it1_s == it_id {
            mi_idx1_m
                .entry(movie_id)
                .and_modify(|e| *e = (*e).min(info))
                .or_insert(info);
            if info < "3.0" {
                mi_idx2_m
                    .entry(movie_id)
                    .and_modify(|e| *e = (*e).min(info))
                    .or_insert(info);
            }
        }
    }

    let kt1_s: i32 = kt
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(kt.column("kind")?.str()?.into_no_null_iter())
        .find(|(_, kind)| *kind == "tv series")
        .map(|(id, _)| id)
        .unwrap();
    // let kt2_s: i32 = kt1_s;

    let mut t1_m: HashMap<i32, &str> = HashMap::default();
    let mut t2_m: HashMap<i32, &str> = HashMap::default();

    for (((id, kind_id), title), production_year) in t
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(t.column("kind_id")?.i32()?.into_no_null_iter())
        .zip(t.column("title")?.str()?.into_no_null_iter())
        .zip(t.column("production_year")?.i32()?)
    {
        if kt1_s == kind_id {
            t1_m.insert(id, title);
            if let Some(production_year) = production_year
                && (2005..=2008).contains(&production_year)
            {
                t2_m.insert(id, title);
            }
        }
    }

    let target_links: HashSet<&str> = ["sequel", "follows", "followed by"].into_iter().collect();

    let lt_s: HashSet<i32> = lt
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(lt.column("link")?.str()?.into_no_null_iter())
        .filter_map(|(id, link)| target_links.contains(link).then_some(id))
        .collect();

    let mc1_m: HashMap<i32, &str> = mc
        .column("company_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mc.column("movie_id")?.i32()?.into_no_null_iter())
        .filter_map(|(company_id, movie_id)| cn1_m.get(&company_id).map(|&name| (movie_id, name)))
        .fold(HashMap::default(), |mut acc, (movie_id, name)| {
            acc.entry(movie_id)
                .and_modify(|existing| {
                    if name < *existing {
                        *existing = name;
                    }
                })
                .or_insert(name);
            acc
        });

    // Should be cn2_m but cn1_m also works
    let mc2_m: HashMap<i32, &str> = mc
        .column("company_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mc.column("movie_id")?.i32()?.into_no_null_iter())
        .filter_map(|(company_id, movie_id)| cn1_m.get(&company_id).map(|&name| (movie_id, name)))
        .fold(HashMap::default(), |mut acc, (movie_id, name)| {
            acc.entry(movie_id)
                .and_modify(|e| *e = (*e).min(name))
                .or_insert(name);
            acc
        });

    let mut res: Option<(&str, &str, &str, &str, &str, &str)> = None;

    for ((link_type_id, movie_id), linked_movie_id) in ml
        .column("link_type_id")?
        .i32()?
        .into_no_null_iter()
        .zip(ml.column("movie_id")?.i32()?.into_no_null_iter())
        .zip(ml.column("linked_movie_id")?.i32()?.into_no_null_iter())
    {
        if lt_s.contains(&link_type_id) {
            if let Some(mi_idx1_info) = mi_idx1_m.get(&movie_id)
                && let Some(mi_idx2_info) = mi_idx2_m.get(&linked_movie_id)
                && let Some(t1_title) = t1_m.get(&movie_id)
                && let Some(t2_title) = t2_m.get(&linked_movie_id)
                && let Some(mc1_min_name) = mc1_m.get(&movie_id)
                && let Some(mc2_min_name) = mc2_m.get(&linked_movie_id)
            {
                res = match res {
                    Some((old_n1, old_n2, old_r1, old_r2, old_t1, old_t2)) => Some((
                        old_n1.min(mc1_min_name),
                        old_n2.min(mc2_min_name),
                        old_r1.min(mi_idx1_info),
                        old_r2.min(mi_idx2_info),
                        old_t1.min(t1_title),
                        old_t2.min(t2_title),
                    )),
                    None => Some((
                        mc1_min_name,
                        mc2_min_name,
                        mi_idx1_info,
                        mi_idx2_info,
                        t1_title,
                        t2_title,
                    )),
                };
            }
        }
    }

    println!("33a,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 33a
// SELECT MIN(cn1.name)     AS first_company,
// MIN(cn2.name)     AS second_company,
// MIN(mi_idx1.info) AS first_rating,
// MIN(mi_idx2.info) AS second_rating,
// MIN(t1.title)     AS first_movie,
// MIN(t2.title)     AS second_movie
// FROM company_name AS cn1,
// company_name AS cn2,
// info_type AS it1,
// info_type AS it2,
// kind_type AS kt1,
// kind_type AS kt2,
// link_type AS lt,
// movie_companies AS mc1,
// movie_companies AS mc2,
// movie_info_idx AS mi_idx1,
// movie_info_idx AS mi_idx2,
// movie_link AS ml,
// title AS t1,
// title AS t2
// WHERE cn1.country_code = '[us]'
// AND it1.info = 'rating'
// AND it2.info = 'rating'
// AND kt1.kind in ('tv series')
// AND kt2.kind in ('tv series')
// AND lt.link in ('sequel', 'follows', 'followed by')
// AND mi_idx2.info < '3.0'
// AND t2.production_year between 2005 and 2008
// AND lt.id = ml.link_type_id
// AND t1.id = ml.movie_id
// AND t2.id = ml.linked_movie_id
// AND it1.id = mi_idx1.info_type_id
// AND t1.id = mi_idx1.movie_id
// AND kt1.id = t1.kind_id
// AND cn1.id = mc1.company_id
// AND t1.id = mc1.movie_id
// AND ml.movie_id = mi_idx1.movie_id
// AND ml.movie_id = mc1.movie_id
// AND mi_idx1.movie_id = mc1.movie_id
// AND it2.id = mi_idx2.info_type_id
// AND t2.id = mi_idx2.movie_id
// AND kt2.id = t2.kind_id
// AND cn2.id = mc2.company_id
// AND t2.id = mc2.movie_id
// AND ml.linked_movie_id = mi_idx2.movie_id
// AND ml.linked_movie_id = mc2.movie_id
// AND mi_idx2.movie_id = mc2.movie_id;
#[cfg(test)]
mod test_q33a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q33a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q33a(&db)?;

        let expected = Some((
            "495 Productions",
            "495 Productions",
            "3.3",
            "2.7",
            "A Double Shot at Love",
            "A Shot at Love with Tila Tequila",
        ));

        assert_eq!(res, expected);
        Ok(())
    }
}
