use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem;
use polars::prelude::*;
use std::time::Instant;

#[allow(clippy::type_complexity)]
pub fn q33b(db: &Data) -> Result<Option<(&str, &str, &str, &str, &str, &str)>, PolarsError> {
    let cn = &db.cn;
    let it = &db.it;
    let kt = &db.kt;
    let lt = &db.lt;
    let mc = &db.mc;
    let mi_idx = &db.mi_idx;
    let ml = &db.ml;
    let t = &db.t;

    let cn2_m: HashMap<i32, &str> =
        cn.id
            .iter()
            .zip(cn.name.iter())
            .fold(HashMap::default(), |mut acc, (id, name)| {
                acc.insert(*id, name.as_str());
                acc
            });

    let follow = memmem::Finder::new("follow");

    let start = Instant::now();

    let cn1_m: HashMap<i32, &str> = cn
        .id
        .iter()
        .zip(cn.name.iter())
        .zip(cn.country_code.iter())
        .filter_map(|((id, name), country_code)| {
            country_code
                .as_ref()
                .filter(|country_code| country_code == &"[nl]")
                .map(|_| (*id, name.as_str()))
        })
        .fold(HashMap::default(), |mut acc, (id, name)| {
            acc.insert(id, name);
            acc
        });

    let it1_s: i32 = it
        .id
        .iter()
        .zip(it.info.iter())
        .find(|(_, info)| *info == "rating")
        .map(|(id, _)| *id)
        .unwrap();

    let mut mi_idx1_m: HashMap<i32, Vec<&str>> = HashMap::default();
    let mut mi_idx2_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((it_id, info), movie_id) in mi_idx
        .info_type_id
        .iter()
        .zip(mi_idx.info.iter())
        .zip(mi_idx.movie_id.iter())
    {
        if it1_s == *it_id {
            mi_idx1_m.entry(*movie_id).or_default().push(info.as_str());
            if info.as_str() < "3.0" {
                mi_idx2_m.entry(*movie_id).or_default().push(info.as_str());
            }
        }
    }

    let kt1_s: i32 = kt
        .id
        .iter()
        .zip(kt.kind.iter())
        .find(|(_, kind)| *kind == "tv series")
        .map(|(id, _)| *id)
        .unwrap();

    let mut t1_m: HashMap<i32, Vec<&str>> = HashMap::default();
    let mut t2_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (((id, kind_id), title), production_year) in
        t.id.iter()
            .zip(t.kind_id.iter())
            .zip(t.title.iter())
            .zip(t.production_year.iter())
    {
        if kt1_s == *kind_id {
            if mi_idx1_m.contains_key(id) {
                t1_m.entry(*id).or_default().push(title.as_str());
            }
            if mi_idx2_m.contains_key(id)
                && let Some(production_year) = production_year
                && *production_year == 2007
            {
                t2_m.entry(*id).or_default().push(title.as_str());
            }
        }
    }

    let lt_s: HashSet<i32> = lt
        .id
        .iter()
        .zip(lt.link.iter())
        .filter_map(|(id, link)| follow.find(link.as_bytes()).is_some().then_some(*id))
        .collect();

    let mut mc1_m: HashMap<i32, Vec<&str>> = HashMap::default();
    let mut mc2_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (company_id, movie_id) in mc.company_id.iter().zip(mc.movie_id.iter()) {
        if cn1_m.contains_key(&company_id) && t1_m.contains_key(movie_id) {
            mc1_m
                .entry(*movie_id)
                .or_default()
                .push(cn1_m.get(&company_id).unwrap());
        } else if t2_m.contains_key(movie_id) && cn2_m.contains_key(&company_id) {
            mc2_m
                .entry(*movie_id)
                .or_default()
                .push(cn2_m.get(&company_id).unwrap());
        }
    }

    let mut res: Option<(&str, &str, &str, &str, &str, &str)> = None;

    for ((link_type_id, movie_id), linked_movie_id) in ml
        .link_type_id
        .iter()
        .zip(ml.movie_id.iter())
        .zip(ml.linked_movie_id.iter())
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
                        old_n1.min(mc1_min_name.iter().min().unwrap()),
                        old_n2.min(mc2_min_name.iter().min().unwrap()),
                        old_r1.min(mi_idx1_info.iter().min().unwrap()),
                        old_r2.min(mi_idx2_info.iter().min().unwrap()),
                        old_t1.min(t1_title.iter().min().unwrap()),
                        old_t2.min(t2_title.iter().min().unwrap()),
                    )),
                    None => Some((
                        mc1_min_name.iter().min().unwrap(),
                        mc2_min_name.iter().min().unwrap(),
                        mi_idx1_info.iter().min().unwrap(),
                        mi_idx2_info.iter().min().unwrap(),
                        t1_title.iter().min().unwrap(),
                        t2_title.iter().min().unwrap(),
                    )),
                };
            }
        }
    }

    println!("33b,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 33b
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
// WHERE cn1.country_code = '[nl]'
// AND it1.info = 'rating'
// AND it2.info = 'rating'
// AND kt1.kind in ('tv series')
// AND kt2.kind in ('tv series')
// AND lt.link LIKE '%follow%'
// AND mi_idx2.info < '3.0'
// AND t2.production_year = 2007
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
mod test_q33b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q33b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q33b(&data)?;
        assert_eq!(
            res,
            Some((
                "MTV Netherlands",
                "495 Productions",
                "3.3",
                "2.7",
                "A Double Shot at Love",
                "A Shot at Love with Tila Tequila"
            ))
        );
        Ok(())
    }
}
