use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

#[allow(clippy::type_complexity)]
pub fn q33a(db: &ImdbData) -> Result<Option<(&str, &str, &str, &str, &str, &str)>, PolarsError> {
    let cn1 = &db.cn;
    let cn2 = &db.cn;
    let it1 = &db.it;
    let it2 = &db.it;
    let kt1 = &db.kt;
    let kt2 = &db.kt;
    let lt = &db.lt;
    let mc1 = &db.mc;
    let mc2 = &db.mc;
    let mi_idx1 = &db.mi_idx;
    let mi_idx2 = &db.mi_idx;
    let ml = &db.ml;
    let t1 = &db.t;
    let t2 = &db.t;

    let mut cn2_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (id, name) in cn2
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cn2.column("name")?.str()?.into_iter())
    {
        if let (Some(id), Some(name)) = (id, name) {
            cn2_m.entry(id).or_default().push(name);
        }
    }

    let mut mc2_m: HashMap<i32, Vec<i32>> = HashMap::default();

    for (company_id, movie_id) in mc2
        .column("company_id")?
        .i32()?
        .into_iter()
        .zip(mc2.column("movie_id")?.i32()?)
    {
        if let (Some(company_id), Some(movie_id)) = (company_id, movie_id) {
            mc2_m.entry(movie_id).or_default().push(company_id);
        }
    }

    let start = Instant::now();

    let mut cn1_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, name), country_code) in cn1
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cn1.column("name")?.str()?.into_iter())
        .zip(cn1.column("country_code")?.str()?.into_iter())
    {
        if let (Some(id), Some(name), Some(country_code)) = (id, name, country_code) {
            if country_code == "[us]" {
                cn1_m.entry(id).or_default().push(name);
            }
        }
    }

    let it1_s: HashSet<i32> = it1
        .column("id")?
        .i32()?
        .into_iter()
        .zip(it1.column("info")?.str()?)
        .filter_map(|(id, info)| {
            if let (Some(id), Some(info)) = (id, info) {
                if info == "rating" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let mut mi_idx1_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((it_id, info), movie_id) in mi_idx1
        .column("info_type_id")?
        .i32()?
        .into_iter()
        .zip(mi_idx1.column("info")?.str()?.into_iter())
        .zip(mi_idx1.column("movie_id")?.i32()?)
    {
        if let (Some(it_id), Some(info), Some(movie_id)) = (it_id, info, movie_id) {
            if it1_s.contains(&it_id) {
                mi_idx1_m.entry(movie_id).or_default().push(info);
            }
        }
    }

    let it2_s: HashSet<i32> = it2
        .column("id")?
        .i32()?
        .into_iter()
        .zip(it2.column("info")?.str()?)
        .filter_map(|(id, info)| {
            if let (Some(id), Some(info)) = (id, info) {
                if info == "rating" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let mut mi_idx2_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((it_id, info), movie_id) in mi_idx2
        .column("info_type_id")?
        .i32()?
        .into_iter()
        .zip(mi_idx2.column("info")?.str()?.into_iter())
        .zip(mi_idx2.column("movie_id")?.i32()?)
    {
        if let (Some(it_id), Some(info), Some(movie_id)) = (it_id, info, movie_id) {
            if it2_s.contains(&it_id) {
                mi_idx2_m.entry(movie_id).or_default().push(info);
            }
        }
    }

    let kt1_s: HashSet<i32> = kt1
        .column("id")?
        .i32()?
        .into_iter()
        .zip(kt1.column("kind")?.str()?)
        .filter_map(|(id, kind)| {
            if let (Some(id), Some(kind)) = (id, kind) {
                if kind == "tv series" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let mut t1_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, kind_id), title) in t1
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t1.column("kind_id")?.i32()?)
        .zip(t1.column("title")?.str()?.into_iter())
    {
        if let (Some(id), Some(kind_id), Some(title)) = (id, kind_id, title) {
            if kt1_s.contains(&kind_id) {
                t1_m.entry(id).or_default().push(title);
            }
        }
    }

    let kt2_s: HashSet<i32> = kt2
        .column("id")?
        .i32()?
        .into_iter()
        .zip(kt2.column("kind")?.str()?)
        .filter_map(|(id, kind)| {
            if let (Some(id), Some(kind)) = (id, kind) {
                if kind == "tv series" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let mut t2_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (((id, kind_id), title), production_year) in t2
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t2.column("kind_id")?.i32()?)
        .zip(t2.column("title")?.str()?.into_iter())
        .zip(t2.column("production_year")?.i32()?)
    {
        if let (Some(id), Some(kind_id), Some(title), Some(production_year)) =
            (id, kind_id, title, production_year)
        {
            if kt2_s.contains(&kind_id) && (2005..=2008).contains(&production_year) {
                t2_m.entry(id).or_default().push(title);
            }
        }
    }

    let lt_s: HashSet<i32> = lt
        .column("id")?
        .i32()?
        .into_iter()
        .zip(lt.column("link")?.str()?)
        .filter_map(|(id, link)| {
            if let (Some(id), Some(link)) = (id, link) {
                if link == "sequel" || link == "follows" || link == "followed by" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut mi_idx2_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((it_id, info), movie_id) in mi_idx2
        .column("info_type_id")?
        .i32()?
        .into_iter()
        .zip(mi_idx2.column("info")?.str()?.into_iter())
        .zip(mi_idx2.column("movie_id")?.i32()?)
    {
        if let (Some(it_id), Some(info), Some(movie_id)) = (it_id, info, movie_id) {
            if it2_s.contains(&it_id) && info < "3.0" {
                mi_idx2_m.entry(movie_id).or_default().push(info);
            }
        }
    }

    let mut mc1_m: HashMap<i32, Vec<i32>> = HashMap::default();

    for (company_id, movie_id) in mc1
        .column("company_id")?
        .i32()?
        .into_iter()
        .zip(mc1.column("movie_id")?.i32()?)
    {
        if let (Some(company_id), Some(movie_id)) = (company_id, movie_id) {
            if cn1_m.contains_key(&company_id) {
                mc1_m.entry(movie_id).or_default().push(company_id);
            }
        }
    }

    let mut res: Option<(&str, &str, &str, &str, &str, &str)> = None;

    for ((link_type_id, movie_id), linked_movie_id) in ml
        .column("link_type_id")?
        .i32()?
        .into_iter()
        .zip(ml.column("movie_id")?.i32()?)
        .zip(ml.column("linked_movie_id")?.i32()?)
    {
        if let (Some(link_type_id), Some(movie_id), Some(linked_movie_id)) =
            (link_type_id, movie_id, linked_movie_id)
        {
            if lt_s.contains(&link_type_id) {
                if let Some(mi_idx1_info) = mi_idx1_m.get(&movie_id) {
                    if let Some(mi_idx2_info) = mi_idx2_m.get(&linked_movie_id) {
                        if let Some(t1_title) = t1_m.get(&movie_id) {
                            if let Some(t2_title) = t2_m.get(&linked_movie_id) {
                                if let Some(c1ids) = mc1_m.get(&movie_id) {
                                    if let Some(c2ids) = mc2_m.get(&linked_movie_id) {
                                        for c1 in c1ids {
                                            if let Some(c1_names) = cn1_m.get(c1) {
                                                for c2 in c2ids {
                                                    if let Some(c2_names) = cn2_m.get(c2) {
                                                        for n1 in c1_names {
                                                            for n2 in c2_names {
                                                                for r1 in mi_idx1_info {
                                                                    for r2 in mi_idx2_info {
                                                                        for t1 in t1_title {
                                                                            for t2 in t2_title {
                                                                                if let Some((
                                                                                    old_n1,
                                                                                    old_n2,
                                                                                    old_r1,
                                                                                    old_r2,
                                                                                    old_t1,
                                                                                    old_t2,
                                                                                )) = res.as_mut()
                                                                                {
                                                                                    if n1 < old_n1 {
                                                                                        *old_n1 =
                                                                                            n1;
                                                                                    }
                                                                                    if n2 < old_n2 {
                                                                                        *old_n2 =
                                                                                            n2;
                                                                                    }
                                                                                    if r1 < old_r1 {
                                                                                        *old_r1 =
                                                                                            r1;
                                                                                    }
                                                                                    if r2 < old_r2 {
                                                                                        *old_r2 =
                                                                                            r2;
                                                                                    }
                                                                                    if t1 < old_t1 {
                                                                                        *old_t1 =
                                                                                            t1;
                                                                                    }
                                                                                    if t2 < old_t2 {
                                                                                        *old_t2 =
                                                                                            t2;
                                                                                    }
                                                                                } else {
                                                                                    res = Some((
                                                                                        n1, n2, r1,
                                                                                        r2, t1, t2,
                                                                                    ));
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
