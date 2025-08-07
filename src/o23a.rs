use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem;
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q23a(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
    let cc = &db.cc;
    let cct = &db.cct;
    let cn = &db.cn;
    // FK-PK optimization
    // let ct = &db.ct;
    let it = &db.it;
    let k = &db.k;
    let kt = &db.kt;
    let mc = &db.mc;
    let mi = &db.mi;
    let mk = &db.mk;
    let t = &db.t;

    let internet = Finder::new("internet");
    let usa_colon = memmem::Finder::new("USA:");
    let two_hundred = memmem::Finder::new(" 200");
    let one_nine_nine = memmem::Finder::new(" 199");

    let k_s: HashSet<&i32> = k.id.iter().collect();

    let start = Instant::now();

    let mk_s: HashSet<&i32> = mk
        .movie_id
        .iter()
        .zip(mk.keyword_id.iter())
        .filter_map(|(movie_id, keyword_id)| k_s.contains(&keyword_id).then_some(movie_id))
        .collect();

    let cct_id = cct
        .kind
        .iter()
        .zip(cct.id.iter())
        .find(|(kind, _)| *kind == "complete+verified")
        .map(|(_, id)| id)
        .unwrap();

    let cc_s: HashSet<i32> = cc
        .status_id
        .iter()
        .zip(cc.movie_id.iter())
        .filter_map(|(status_id, movie_id)| {
            (status_id == cct_id && mk_s.contains(&movie_id)).then_some(*movie_id)
        })
        .collect();

    let cn_s: HashSet<i32> = cn
        .country_code
        .iter()
        .zip(cn.id.iter())
        .filter_map(|(country_code, id)| {
            country_code
                .as_ref()
                .filter(|country_code| *country_code == "[us]")
                .map(|_| *id)
        })
        .collect();

    let mc_s: HashSet<i32> = mc
        .movie_id
        .iter()
        .zip(mc.company_type_id.iter())
        .filter_map(|(movie_id, company_type_id)| {
            if cn_s.contains(&company_type_id) && cc_s.contains(&movie_id) {
                Some(*movie_id)
            } else {
                None
            }
        })
        .collect();

    let it_id = it
        .info
        .iter()
        .zip(it.id.iter())
        .find(|(info, _)| *info == "release dates")
        .map(|(_, id)| id)
        .unwrap();

    let mi_s: HashSet<&i32> = mi
        .info
        .iter()
        .zip(mi.movie_id.iter())
        .zip(mi.note.iter())
        .zip(mi.info_type_id.iter())
        .filter_map(|(((info, movie_id), note), info_type_id)| {
            if let Some(note) = note
                && internet.find(note.as_bytes()).is_some()
                && ((usa_colon.find(info.as_bytes()) == Some(0)
                    && one_nine_nine.find(info.as_bytes()).is_some())
                    || (usa_colon.find(info.as_bytes()) == Some(0)
                        && two_hundred.find(info.as_bytes()).is_some()))
                && mc_s.contains(&movie_id)
                && it_id == info_type_id
            {
                Some(movie_id)
            } else {
                None
            }
        })
        .collect();

    let kt_m: HashMap<&i32, Vec<&str>> = kt
        .id
        .iter()
        .zip(kt.kind.iter())
        .filter_map(|(id, kind)| (kind == "movie").then_some((id, kind)))
        .fold(HashMap::default(), |mut acc, (id, kind)| {
            acc.entry(id).or_insert_with(Vec::new).push(kind);
            acc
        });

    let mut res: Option<(&str, &str)> = None;

    for (((kind_id, id), production_year), title) in t
        .kind_id
        .iter()
        .zip(t.id.iter())
        .zip(t.production_year.iter())
        .zip(t.title.iter())
    {
        if let Some(production_year) = production_year
            && mi_s.contains(&id)
            && *production_year > 2000
            && let Some(kinds) = kt_m.get(&kind_id)
        {
            res = match res {
                Some((old_kind, old_title)) => Some((
                    kinds.iter().min().unwrap().min(&old_kind),
                    title.as_str().min(old_title),
                )),
                None => Some((kinds.iter().min().unwrap(), title)),
            };
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("23a,{duration:}");

    Ok(res)
}

// -- JOB Query 23a
// SELECT MIN(kt.kind) AS movie_kind,
//        MIN(t.title) AS complete_us_internet_movie
// FROM complete_cast AS cc,
//      comp_cast_type AS cct1,
//      company_name AS cn,
//      company_type AS ct,
//      info_type AS it1,
//      keyword AS k,
//      kind_type AS kt,
//      movie_companies AS mc,
//      movie_info AS mi,
//      movie_keyword AS mk,
//      title AS t
// WHERE cct1.kind = 'complete+verified'
//   AND cn.country_code = '[us]'
//   AND it1.info = 'release dates'
//   AND kt.kind IN ('movie')
//   AND mi.note LIKE '%internet%'
//   AND mi.info IS NOT NULL
//   AND (mi.info LIKE 'USA:% 199%'
//        OR mi.info LIKE 'USA:% 200%')
//   AND t.production_year > 2000
//   AND kt.id = t.kind_id
//   AND t.id = mi.movie_id
//   AND t.id = mk.movie_id
//   AND t.id = mc.movie_id
//   AND t.id = cc.movie_id
//   AND mk.movie_id = mi.movie_id
//   AND mk.movie_id = mc.movie_id
//   AND mk.movie_id = cc.movie_id
//   AND mi.movie_id = mc.movie_id
//   AND mi.movie_id = cc.movie_id
//   AND mc.movie_id = cc.movie_id
//   AND k.id = mk.keyword_id
//   AND it1.id = mi.info_type_id
//   AND cn.id = mc.company_id
//   AND ct.id = mc.company_type_id
//   AND cct1.id = cc.status_id;

#[cfg(test)]
mod test_q23a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q23a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q23a(&data)?;

        assert_eq!(res, Some(("movie", "The Analysts")));
        Ok(())
    }
}
