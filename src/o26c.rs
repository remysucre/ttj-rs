use crate::data::ImdbData;
use ahash::{HashMap, HashSet, HashSetExt};
use polars::prelude::*;
use std::time::Instant;

pub fn q26c(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
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

    let start = Instant::now();

    let chn_m: HashMap<i32, &str> = chn
        .column("name")?
        .str()?
        .into_no_null_iter()
        .zip(chn.column("id")?.i32()?.into_no_null_iter())
        .filter_map(|(name, id)| {
            (name.contains("man") || name.contains("Man")).then_some((name, id))
        })
        .fold(HashMap::default(), |mut acc, (name, id)| {
            acc.entry(id)
                .and_modify(|existing| {
                    if name < *existing {
                        *existing = name;
                    }
                })
                .or_insert(name);
            acc
        });

    let mut cct1_id: i32 = 0;
    let mut cct2_s = HashSet::new();

    for (id, kind) in cct
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(cct.column("kind")?.str()?.into_no_null_iter())
    {
        if kind == "cast" {
            cct1_id = id;
        } else if kind.contains("complete") {
            cct2_s.insert(id);
        }
    }

    let cc_s: HashSet<i32> = cc
        .column("movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(cc.column("subject_id")?.i32()?.into_no_null_iter())
        .zip(cc.column("status_id")?.i32()?.into_no_null_iter())
        .filter_map(|((movie_id, subject_id), status_id)| {
            (cct1_id == subject_id && cct2_s.contains(&status_id)).then_some(movie_id)
        })
        .collect();

    let it_id = it
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(it.column("info")?.str()?.into_no_null_iter())
        .find(|(_, info)| *info == "rating")
        .map(|(id, _)| id)
        .unwrap();

    let target_keywords: HashSet<&str> = [
        "superhero",
        "marvel-comics",
        "based-on-comic",
        "tv-special",
        "fight",
        "violence",
        "magnet",
        "web",
        "claw",
        "laser",
    ]
    .into_iter()
    .collect();

    let k_s: HashSet<i32> = k
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(k.column("keyword")?.str()?.into_no_null_iter())
        .filter_map(|(id, keyword)| target_keywords.contains(keyword).then_some(id))
        .collect();

    let mk_s: HashSet<i32> = mk
        .column("keyword_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mk.column("movie_id")?.i32()?.into_no_null_iter())
        .filter_map(|(keyword_id, movie_id)| {
            (k_s.contains(&keyword_id) && cc_s.contains(&movie_id)).then_some(movie_id)
        })
        .collect();

    let mi_idx_m: HashMap<i32, &str> = mi_idx
        .column("movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mi_idx.column("info")?.str()?.into_no_null_iter())
        .zip(mi_idx.column("info_type_id")?.i32()?.into_no_null_iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            (it_id == info_type_id && mk_s.contains(&movie_id)).then_some((movie_id, info))
        })
        .fold(HashMap::default(), |mut acc, (movie_id, info)| {
            acc.entry(movie_id)
                .and_modify(|existing| {
                    if info < *existing {
                        *existing = info;
                    }
                })
                .or_insert(info);
            acc
        });

    let kt_id = kt
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(kt.column("kind")?.str()?.into_no_null_iter())
        .find(|(_, kind)| *kind == "movie")
        .map(|(id, _)| id)
        .unwrap();

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(t.column("kind_id")?.i32()?.into_no_null_iter())
        .zip(t.column("title")?.str()?.into_no_null_iter())
        .zip(t.column("production_year")?.i32()?)
        .filter_map(|(((id, kind_id), title), production_year)| {
            (production_year? > 2000 && kt_id == kind_id && mi_idx_m.contains_key(&id))
                .then_some((id, title))
        })
        .fold(HashMap::default(), |mut acc, (id, title)| {
            acc.entry(id)
                .and_modify(|existing| {
                    if title < *existing {
                        *existing = title;
                    }
                })
                .or_insert(title);
            acc
        });

    let mut res: Option<(&str, &str, &str)> = None;

    for (movie_id, person_role_id) in ci
        .column("movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(ci.column("person_role_id")?.i32()?.into_no_null_iter())
    {
        if let Some(title) = t_m.get(&movie_id)
            && let Some(char_name) = chn_m.get(&person_role_id)
            && let Some(info) = mi_idx_m.get(&movie_id)
        {
            res = match res {
                Some((old_name, old_info, old_title)) => Some((
                    char_name.min(&old_name),
                    info.min(&old_info),
                    title.min(&old_title),
                )),
                None => Some((char_name, info, title)),
            };
        }
    }

    println!("26c,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 26c
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
// AND k.keyword in
// ('superhero', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence', 'magnet', 'web', 'claw',
// 'laser')
// AND kt.kind = 'movie'
// AND t.production_year > 2000
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
mod test_26c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q26c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q26c(&db)?;
        assert_eq!(res, Some(("'Agua' Man", "1.9", "12 Rounds")));
        Ok(())
    }
}
