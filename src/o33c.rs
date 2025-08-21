use crate::data::Data;
use ahash::HashMap;
use polars::prelude::*;
use std::time::Instant;

#[allow(clippy::type_complexity)]
pub fn q33c(db: &Data) -> Result<Option<(&str, &str, &str, &str, &str, &str)>, PolarsError> {
    let cn = &db.cn;
    let it = &db.it;
    let kt = &db.kt;
    let lt = &db.lt;
    let mc = &db.mc;
    let mi_idx = &db.mi_idx;
    let ml = &db.ml;
    let t = &db.t;

    let start = Instant::now();

    let mut cn1_m: HashMap<&i32, &str> = HashMap::default();
    let mut cn2_m: HashMap<&i32, &str> = HashMap::default();

    for ((id, name), country_code) in cn.id.iter().zip(cn.name.iter()).zip(cn.country_code.iter()) {
        cn2_m.insert(id, name.as_str());
        if let Some(country_code) = country_code
            && country_code != "[us]"
        {
            cn1_m.insert(id, name.as_str());
        }
    }

    let it1_id: &i32 = it
        .info
        .iter()
        .zip(it.id.iter())
        .find(|(info, _)| *info == "rating")
        .map(|(_, id)| id)
        .unwrap();

    let mut mi_idx1_m: ahash::HashMap<&i32, &str> = ahash::HashMap::default();
    let mut mi_idx2_m: ahash::HashMap<&i32, &str> = ahash::HashMap::default();

    for ((it_id, info), movie_id) in mi_idx
        .info_type_id
        .iter()
        .zip(mi_idx.info.iter())
        .zip(mi_idx.movie_id.iter())
    {
        if it1_id == it_id {
            mi_idx1_m.insert(movie_id, info);
            if info.as_str() < "3.5" {
                mi_idx2_m.insert(movie_id, info);
            }
        }
    }

    let target_kind_type: ahash::HashSet<&str> = ["tv series", "episode"].into_iter().collect();

    let kt_s: ahash::HashSet<&i32> = kt
        .kind
        .iter()
        .zip(kt.id.iter())
        .filter_map(|(kind, id)| target_kind_type.contains(kind.as_str()).then_some(id))
        .collect();

    let mut mc1_m: HashMap<i32, Vec<&str>> = HashMap::default();
    let mut mc2_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (company_id, movie_id) in mc.company_id.iter().zip(mc.movie_id.iter()) {
        if let Some(name) = cn1_m.get(&company_id)
            && mi_idx1_m.contains_key(&movie_id)
        {
            mc1_m.entry(*movie_id).or_default().push(name);
        }
        if mi_idx2_m.contains_key(&movie_id)
            && let Some(name) = cn2_m.get(&company_id)
        {
            mc2_m.entry(*movie_id).or_default().push(name);
        }
    }

    let mut t1_m: HashMap<&i32, &str> = HashMap::default();
    let mut t2_m: HashMap<&i32, &str> = HashMap::default();

    for (((id, kind_id), title), production_year) in
        t.id.iter()
            .zip(t.kind_id.iter())
            .zip(t.title.iter())
            .zip(t.production_year.iter())
    {
        if kt_s.contains(&kind_id) {
            if mc1_m.contains_key(&id) {
                t1_m.insert(id, title.as_str());
            }
            if let Some(production_year) = production_year
                && (2000..=2010).contains(production_year)
                && mc2_m.contains_key(&id)
            {
                t2_m.insert(id, title.as_str());
            }
        }
    }

    let target_link: ahash::HashSet<&str> =
        ["sequel", "follows", "followed by"].into_iter().collect();

    let lt_s: Vec<i32> = lt
        .id
        .iter()
        .zip(lt.link.iter())
        .filter_map(|(id, link)| target_link.contains(link.as_str()).then_some(*id))
        .collect();

    let mut res: Option<(&str, &str, &str, &str, &str, &str)> = None;

    for ((link_type_id, movie_id), linked_movie_id) in ml
        .link_type_id
        .iter()
        .zip(ml.movie_id.iter())
        .zip(ml.linked_movie_id.iter())
    {
        if lt_s.contains(&link_type_id)
            && let Some(mi_idx1_info) = mi_idx1_m.get(&movie_id)
            && let Some(mi_idx2_info) = mi_idx2_m.get(&linked_movie_id)
            && let Some(t1) = t1_m.get(&movie_id)
            && let Some(t2) = t2_m.get(&linked_movie_id)
            && let Some(c1s) = mc1_m.get(&movie_id)
            && let Some(c2s) = mc2_m.get(&linked_movie_id)
        {
            res = match res {
                Some((old_n1, old_n2, old_r1, old_r2, old_t1, old_t2)) => Some((
                    c1s.iter().min().unwrap().min(&old_n1),
                    c2s.iter().min().unwrap().min(&old_n2),
                    old_r1.min(mi_idx1_info),
                    old_r2.min(mi_idx2_info),
                    old_t1.min(t1),
                    old_t2.min(t2),
                )),
                None => Some((
                    c1s.iter().min().unwrap(),
                    c2s.iter().min().unwrap(),
                    mi_idx1_info,
                    mi_idx2_info,
                    t1,
                    t2,
                )),
            };
        }
    }

    println!("33c,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 33c
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
// WHERE cn1.country_code != '[us]' AND it1.info  = 'rating' AND it2.info  = 'rating' AND kt1.kind  in ('tv series', 'episode') AND kt2.kind  in ('tv series', 'episode') AND lt.link  in ('sequel', 'follows', 'followed by') AND mi_idx2.info  < '3.5' AND t2.production_year  between 2000 and 2010 AND lt.id = ml.link_type_id AND t1.id = ml.movie_id AND t2.id = ml.linked_movie_id AND it1.id = mi_idx1.info_type_id AND t1.id = mi_idx1.movie_id AND kt1.id = t1.kind_id AND cn1.id = mc1.company_id AND t1.id = mc1.movie_id AND ml.movie_id = mi_idx1.movie_id AND ml.movie_id = mc1.movie_id AND mi_idx1.movie_id = mc1.movie_id AND it2.id = mi_idx2.info_type_id AND t2.id = mi_idx2.movie_id AND kt2.id = t2.kind_id AND cn2.id = mc2.company_id AND t2.id = mc2.movie_id AND ml.linked_movie_id = mi_idx2.movie_id AND ml.linked_movie_id = mc2.movie_id AND mi_idx2.movie_id = mc2.movie_id;
#[cfg(test)]
mod test_q33c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q33c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q33c(&data)?;
        assert_eq!(
            res,
            Some((
                "2BE",
                "495 Productions",
                "1.3",
                "1.0",
                "A Double Shot at Love",
                "A Double Shot at Love"
            ))
        );
        Ok(())
    }
}
