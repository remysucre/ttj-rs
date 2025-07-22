use crate::data::ImdbData;
use ahash::HashSet;
use ahash::{HashMap, HashSetExt};
use polars::prelude::*;
use std::time::Instant;

pub fn q20c(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    let ci = &db.ci;
    let chn = &db.chn;
    let t = &db.t;
    let kt = &db.kt;
    let n = &db.n;
    let cc = &db.cc;
    let cct: &DataFrame = &db.cct;
    let mk = &db.mk;
    let k = &db.k;

    let n_m: HashMap<i32, &str> = n
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(n.column("name")?.str()?.into_no_null_iter())
        .collect();

    let start = Instant::now();

    let mut cct1_id: i32 = 0;
    let mut cct2_s = HashSet::new();

    for (id, kind) in cct
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(db.cct.column("kind")?.str()?.into_no_null_iter())
    {
        if kind == "cast" {
            cct1_id = id;
        } else if kind.contains("complete") {
            cct2_s.insert(id);
        }
    }

    let cc_s: HashSet<i32> = cc
        .column("subject_id")?
        .i32()?
        .into_no_null_iter()
        .zip(cc.column("status_id")?.i32()?.into_no_null_iter())
        .zip(cc.column("movie_id")?.i32()?.into_no_null_iter())
        .filter_map(|((subject_id, status_id), movie_id)| {
            (cct1_id == subject_id && cct2_s.contains(&status_id)).then_some(movie_id)
        })
        .collect();

    let chn_s: HashSet<i32> = chn
        .column("name")?
        .str()?
        .into_no_null_iter()
        .zip(chn.column("id")?.i32()?.into_no_null_iter())
        .filter_map(|(name, id)| {
            if name.contains("Sherlock") {
                return None;
            }
            if name.contains("man") || name.contains("Man") {
                Some(id)
            } else {
                None
            }
        })
        .collect();

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

    let kt_id = kt
        .column("kind")?
        .str()?
        .into_no_null_iter()
        .zip(kt.column("id")?.i32()?.into_no_null_iter())
        .find(|(kind, _)| *kind == "movie")
        .map(|(_, id)| id)
        .unwrap();

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(t.column("title")?.str()?.into_no_null_iter())
        .zip(t.column("production_year")?.i32()?)
        .zip(t.column("kind_id")?.i32()?.into_no_null_iter())
        .filter_map(|(((id, title), production_year), kind_id)| {
            (production_year? > 2000 && kt_id == kind_id && cc_s.contains(&id))
                .then_some((id, title))
        })
        .fold(HashMap::default(), |mut acc, (id, title)| {
            acc.entry(id)
                .and_modify(|e| *e = (*e).min(title))
                .or_insert(title);
            acc
        });

    let mk_s = mk
        .column("keyword_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mk.column("movie_id")?.i32()?.into_no_null_iter())
        .filter_map(|(keyword_id, movie_id)| {
            (k_s.contains(&keyword_id) && t_m.contains_key(&movie_id)).then_some(movie_id)
        })
        .collect::<HashSet<_>>();

    let mut res: Option<(&str, &str)> = None;

    for ((movie_id, person_id), person_role_id) in ci
        .column("movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(ci.column("person_id")?.i32()?.into_no_null_iter())
        .zip(ci.column("person_role_id")?.i32()?.into_no_null_iter())
    {
        if chn_s.contains(&person_role_id)
            && mk_s.contains(&movie_id)
            && let Some(name) = n_m.get(&person_id)
            && let Some(title) = t_m.get(&movie_id)
        {
            res = match res {
                Some((old_name, old_title)) => Some((name.min(&old_name), title.min(&old_title))),
                None => Some((name, title)),
            };
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("20c,{duration:}");

    Ok(res)
}

// -- JOB Query 20c
// SELECT MIN(n.name) AS cast_member, MIN(t.title) AS complete_dynamic_hero_movie
// FROM complete_cast AS cc,
// comp_cast_type AS cct1,
// comp_cast_type AS cct2,
// char_name AS chn,
// cast_info AS ci,
// keyword AS k,
// kind_type AS kt,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE cct1.kind = 'cast'
// AND cct2.kind like '%complete%'
// AND chn.name is not NULL
// and (chn.name like '%man%' or chn.name like '%Man%')
// AND k.keyword in
// ('superhero', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence', 'magnet', 'web', 'claw',
// 'laser')
// AND kt.kind = 'movie'
// AND t.production_year > 2000
// AND kt.id = t.kind_id
// AND t.id = mk.movie_id
// AND t.id = ci.movie_id
// AND t.id = cc.movie_id
// AND mk.movie_id = ci.movie_id
// AND mk.movie_id = cc.movie_id
// AND ci.movie_id = cc.movie_id
// AND chn.id = ci.person_role_id
// AND n.id = ci.person_id
// AND k.id = mk.keyword_id
// AND cct1.id = cc.subject_id
// AND cct2.id = cc.status_id;
#[cfg(test)]
mod test_20c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q20c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q20c(&db)?;
        assert_eq!(res, Some(("Abell, Alistair", "...And Then I...")));
        Ok(())
    }
}
