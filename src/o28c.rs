use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q28c(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cc = &db.cc;
    let cct = &db.cct;
    let cn = &db.cn;
    // let ct = &db.ct;
    let it = &db.it;
    let k = &db.k;
    let kt = &db.kt;
    let mc = &db.mc;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let mk = &db.mk;
    let t = &db.t;

    let two_hundred_p = Finder::new("(200");
    let usa_p = Finder::new("(USA)");

    let start = Instant::now();

    let mut cct1_id: i32 = 0;
    let mut cct2_id: i32 = 0;

    for (id, kind) in cct.id.iter().zip(cct.kind.iter()) {
        if kind == "cast" {
            cct1_id = *id;
        } else if kind == "complete" {
            cct2_id = *id;
        }
    }

    let cc_s: HashSet<i32> = cc
        .subject_id
        .iter()
        .zip(cc.status_id.iter())
        .zip(cc.movie_id.iter())
        .filter_map(|((subject_id, status_id), movie_id)| {
            (cct1_id == *subject_id && cct2_id == *status_id).then_some(*movie_id)
        })
        .collect();

    let cn_m: HashMap<&i32, &str> = cn
        .id
        .iter()
        .zip(cn.name.iter())
        .zip(cn.country_code.iter())
        .filter_map(|((id, name), country_code)| {
            country_code
                .as_deref()
                .filter(|&code| code != "[us]")
                .map(|_| (id, name.as_str()))
        })
        .collect();

    let mut it1_id: &i32 = &0;
    let mut it2_id: &i32 = &0;

    for (id, info) in it.id.iter().zip(it.info.iter()) {
        if info == "countries" {
            it1_id = id;
        } else if info == "rating" {
            it2_id = id;
        }
    }

    let target_keywords: HashSet<&str> = ["murder", "murder-in-title", "blood", "violence"]
        .into_iter()
        .collect();

    let k_s: HashSet<&i32> =
        k.id.iter()
            .zip(k.keyword.iter())
            .filter_map(|(id, keyword)| target_keywords.contains(keyword.as_str()).then_some(id))
            .collect();

    let target_kind_type: ahash::HashSet<&str> = ["movie", "episode"].into_iter().collect();

    let kt_s: HashSet<&i32> = kt
        .kind
        .iter()
        .zip(kt.id.iter())
        .filter_map(|(kind, id)| target_kind_type.contains(kind.as_str()).then_some(id))
        .collect();

    let target_info: ahash::HashSet<&str> = [
        "Sweden",
        "Norway",
        "Germany",
        "Denmark",
        "Swedish",
        "Danish",
        "Norwegian",
        "German",
        "USA",
        "American",
    ]
    .into_iter()
    .collect();

    let mi_s: HashSet<&i32> = mi
        .info
        .iter()
        .zip(mi.movie_id.iter())
        .zip(mi.info_type_id.iter())
        .filter_map(|((info, movie_id), info_type_id)| {
            (target_info.contains(info.as_str())
                && cc_s.contains(&movie_id)
                && it1_id == info_type_id)
                .then_some(movie_id)
        })
        .collect();

    let mi_idx_m: ahash::HashMap<&i32, Vec<&str>> = mi_idx
        .movie_id
        .iter()
        .zip(mi_idx.info.iter())
        .zip(mi_idx.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            (it2_id == info_type_id && info.as_str() < "8.5" && mi_s.contains(&movie_id))
                .then_some((movie_id, info))
        })
        .fold(ahash::HashMap::default(), |mut acc, (idx, info)| {
            acc.entry(idx).or_insert_with(Vec::new).push(info);
            acc
        });

    let mk_s: HashSet<&i32> = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| {
            (k_s.contains(keyword_id) && mi_idx_m.contains_key(&movie_id)).then_some(movie_id)
        })
        .collect();

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .zip(t.kind_id.iter())
            .filter_map(|(((id, title), production_year), kind_id)| {
                if let Some(production_year) = production_year
                    && *production_year > 2005
                    && kt_s.contains(&kind_id)
                    && mk_s.contains(&id)
                {
                    Some((id, title.as_str()))
                } else {
                    None
                }
            })
            .collect();

    let mut res: Option<(&str, &str, &str)> = None;

    for ((movie_id, company_id), note) in mc
        .movie_id
        .iter()
        .zip(mc.company_id.iter())
        .zip(mc.note.iter())
    {
        if let Some(note) = note
            && two_hundred_p.find(note.as_bytes()).is_some()
            && usa_p.find(note.as_bytes()).is_none()
            && let Some(title) = t_m.get(&movie_id)
            && let Some(name) = cn_m.get(&company_id)
            && let Some(info) = mi_idx_m.get(&movie_id)
        {
            res = match res {
                Some((old_name, old_info, old_title)) => Some((
                    name.min(&old_name),
                    info.iter().min().unwrap().min(&old_info),
                    title.min(&old_title),
                )),
                None => Some((name, info.iter().min().unwrap(), title)),
            };
        }
    }

    println!("28c,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 28c
// SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS complete_euro_dark_movie
// FROM complete_cast AS cc,
// comp_cast_type AS cct1,
// comp_cast_type AS cct2,
// company_name AS cn,
// company_type AS ct,
// info_type AS it1,
// info_type AS it2,
// keyword AS k,
// kind_type AS kt,
// movie_companies AS mc,
// movie_info AS mi,
// movie_info_idx AS mi_idx,
// movie_keyword AS mk,
// title AS t
// WHERE cct1.kind = 'cast'
// AND cct2.kind = 'complete'
// AND cn.country_code != '[us]' AND it1.info  = 'countries' AND it2.info  = 'rating' AND k.keyword  in ('murder', 'murder-in-title', 'blood', 'violence') AND kt.kind  in ('movie', 'episode') AND mc.note  not like '%(USA)%' and mc.note like '%(200%)%' AND mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Danish', 'Norwegian', 'German', 'USA', 'American') AND mi_idx.info  < '8.5' AND t.production_year  > 2005 AND kt.id = t.kind_id AND t.id = mi.movie_id AND t.id = mk.movie_id AND t.id = mi_idx.movie_id AND t.id = mc.movie_id AND t.id = cc.movie_id AND mk.movie_id = mi.movie_id AND mk.movie_id = mi_idx.movie_id AND mk.movie_id = mc.movie_id AND mk.movie_id = cc.movie_id AND mi.movie_id = mi_idx.movie_id AND mi.movie_id = mc.movie_id AND mi.movie_id = cc.movie_id AND mc.movie_id = mi_idx.movie_id AND mc.movie_id = cc.movie_id AND mi_idx.movie_id = cc.movie_id AND k.id = mk.keyword_id AND it1.id = mi.info_type_id AND it2.id = mi_idx.info_type_id AND ct.id = mc.company_type_id AND cn.id = mc.company_id AND cct1.id = cc.subject_id AND cct2.id = cc.status_id;
#[cfg(test)]
mod test_q28c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q28c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q28c(&data)?;
        assert_eq!(res, Some(("01 Distribuzione", "1.9", "(#1.1)")));
        Ok(())
    }
}
