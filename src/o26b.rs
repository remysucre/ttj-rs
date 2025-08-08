use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem;
use polars::prelude::*;
use std::time::Instant;

pub fn q26b(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cc = &db.cc;
    let cct = &db.cct;
    let chn = &db.chn;
    let ci = &db.ci;
    let it = &db.it;
    let k = &db.k;
    let kt = &db.kt;
    let mi_idx = &db.mi_idx;
    let mk = &db.mk;
    // let n = &db.n;
    let t = &db.t;

    // FK-PK optimization
    // let n_m: HashMap<i32, Vec<&str>> = n
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .zip(n.column("name")?.str()?.into_iter())
    //     .fold(HashMap::default(), |mut acc, (id, name)| {
    //         if let (Some(id), Some(name)) = (id, name) {
    //             acc.entry(id).or_default().push(name);
    //         }
    //         acc
    //     });

    let man = memmem::Finder::new(b"man");
    let big_man = memmem::Finder::new(b"Man");

    let start = Instant::now();

    let chn_m: HashMap<&i32, &str> = chn
        .name
        .iter()
        .zip(chn.id.iter())
        .filter_map(|(name, id)| {
            (man.find(name.as_bytes()).is_some() || big_man.find(name.as_bytes()).is_some())
                .then_some((id, name.as_str()))
        })
        .collect();

    let mut cct1_id: i32 = 0;
    let mut cct2_s = HashSet::default();

    for (id, kind) in cct.id.iter().zip(cct.kind.iter()) {
        if kind == "cast" {
            cct1_id = *id;
        } else if kind.contains("complete") {
            cct2_s.insert(id);
        }
    }

    let cc_s: HashSet<i32> = cc
        .subject_id
        .iter()
        .zip(cc.status_id.iter())
        .zip(cc.movie_id.iter())
        .filter_map(|((subject_id, status_id), movie_id)| {
            (*subject_id == cct1_id && cct2_s.contains(status_id)).then_some(*movie_id)
        })
        .collect();

    let it_id: &i32 = it
        .info
        .iter()
        .zip(it.id.iter())
        .find(|(info, _)| *info == "rating")
        .map(|(_, id)| id)
        .unwrap();

    let target_keywords: HashSet<&str> = ["superhero", "marvel-comics", "based-on-comic", "fight"]
        .into_iter()
        .collect();

    let k_s: HashSet<&i32> =
        k.id.iter()
            .zip(k.keyword.iter())
            .filter_map(|(id, keyword)| target_keywords.contains(keyword.as_str()).then_some(id))
            .collect();

    let mk_s: HashSet<&i32> = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| {
            (k_s.contains(&keyword_id) && cc_s.contains(&movie_id)).then_some(movie_id)
        })
        .collect();

    let mi_idx_m: ahash::HashMap<&i32, Vec<&str>> = mi_idx
        .movie_id
        .iter()
        .zip(mi_idx.info.iter())
        .zip(mi_idx.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            (it_id == info_type_id && info.as_str() > "8.0" && mk_s.contains(&movie_id))
                .then_some((movie_id, info))
        })
        .fold(ahash::HashMap::default(), |mut acc, (idx, info)| {
            acc.entry(idx).or_insert_with(Vec::new).push(info);
            acc
        });

    let kt_id = kt
        .kind
        .iter()
        .zip(kt.id.iter())
        .find(|(kind, _)| *kind == "movie")
        .map(|(_, id)| id)
        .unwrap();

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .zip(t.kind_id.iter())
            .filter_map(|(((id, title), production_year), kind_id)| {
                if let Some(production_year) = production_year
                    && mk_s.contains(&id)
                    && mi_idx_m.contains_key(&id)
                    && kt_id == kind_id
                    && *production_year > 2005
                {
                    Some((id, title.as_str()))
                } else {
                    None
                }
            })
            .collect();

    let mut res: Option<(&str, &str, &str)> = None;

    for (movie_id, person_role_id) in ci.movie_id.iter().zip(ci.person_role_id.iter()) {
        if let Some(person_role_id) = person_role_id
            && let Some(title) = t_m.get(&movie_id)
            && let Some(char_name) = chn_m.get(person_role_id)
            && let Some(info) = mi_idx_m.get(&movie_id)
        {
            res = match res {
                Some((old_name, old_info, old_title)) => Some((
                    char_name.min(&old_name),
                    info.iter().min().unwrap().min(&old_info),
                    title.min(&old_title),
                )),
                None => Some((char_name, info.iter().min().unwrap(), title)),
            };
        }
    }

    println!("26b,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 26b
// SELECT MIN(chn.name) AS character_name, MIN(mi_idx.info) AS rating, MIN(t.title) AS complete_hero_movie
// FROM complete_cast AS cc,
// comp_cast_type AS cct1,
// comp_cast_type AS cct2,
// char_name AS chn,
// cast_info AS ci,
// info_type AS it2,
// keyword AS k,
// kind_type AS kt,
// movie_info_idx AS mi_idx,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE cct1.kind = 'cast'
// AND cct2.kind like '%complete%'
// AND chn.name is not NULL
// and (chn.name like '%man%' or chn.name like '%Man%')
// AND it2.info = 'rating'
// AND k.keyword in ('superhero', 'marvel-comics', 'based-on-comic', 'fight')
// AND kt.kind = 'movie'
// AND mi_idx.info > '8.0'
// AND t.production_year > 2005
// AND kt.id = t.kind_id
// AND t.id = mk.movie_id
// AND t.id = ci.movie_id
// AND t.id = cc.movie_id
// AND t.id = mi_idx.movie_id
// AND mk.movie_id = ci.movie_id
// AND mk.movie_id = cc.movie_id
// AND mk.movie_id = mi_idx.movie_id
// AND ci.movie_id = cc.movie_id
// AND ci.movie_id = mi_idx.movie_id
// AND cc.movie_id = mi_idx.movie_id
// AND chn.id = ci.person_role_id
// AND n.id = ci.person_id
// AND k.id = mk.keyword_id
// AND cct1.id = cc.subject_id
// AND cct2.id = cc.status_id
// AND it2.id = mi_idx.info_type_id;
#[cfg(test)]
mod test_q26b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q26b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q26b(&data)?;
        assert_eq!(res, Some(("Bank Manager", "8.2", "Inception")));
        Ok(())
    }
}
