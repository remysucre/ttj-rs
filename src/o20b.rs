use crate::data::ImdbData;
use ahash::HashMap;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q20b(db: &ImdbData) -> Result<Option<&str>, PolarsError> {
    let ci = &db.ci;
    let chn = &db.chn;
    let t = &db.t;
    let kt = &db.kt;
    let n = &db.n;
    let cc = &db.cc;
    let cct1: &DataFrame = &db.cct;
    let cct2: &DataFrame = &db.cct;
    let mk = &db.mk;
    let k = &db.k;

    let start = Instant::now();

    let cct1_id = cct1
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(cct1.column("kind")?.str()?.into_no_null_iter())
        .find(|(_, kind)| *kind == "cast")
        .map(|(id, _)| id)
        .unwrap();

    let cct2_s: HashSet<i32> = cct2
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(cct2.column("kind")?.str()?.into_no_null_iter())
        .filter_map(|(id, kind)| kind.contains("complete").then_some(id))
        .collect();

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
            if (name.contains("Tony") && name.contains("Stark"))
                || (name.contains("Iron") && name.contains("Man"))
            {
                Some(id)
            } else {
                None
            }
        })
        .collect();

    let target_keywords: HashSet<&str> = [
        "superhero",
        "sequel",
        "second-part",
        "marvel-comics",
        "based-on-comic",
        "tv-special",
        "fight",
        "violence",
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

    let t_m: HashMap<i32, Vec<&str>> = t
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(t.column("title")?.str()?.into_no_null_iter())
        .zip(t.column("production_year")?.i32()?)
        .zip(t.column("kind_id")?.i32()?.into_no_null_iter())
        .filter_map(|(((id, title), production_year), kind_id)| {
            if let Some(production_year) = production_year
                && production_year > 2000
                && kt_id == kind_id
                && cc_s.contains(&id)
            {
                Some((id, title))
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (id, title)| {
            acc.entry(id).or_default().push(title);
            acc
        });

    let n_s: HashSet<i32> = n
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(n.column("name")?.str()?.into_no_null_iter())
        .filter_map(|(id, name)| {
            if name.contains("Downey") && name.contains("Robert") {
                Some(id)
            } else {
                None
            }
        })
        .collect();

    let mk_s = mk
        .column("keyword_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mk.column("movie_id")?.i32()?.into_no_null_iter())
        .filter_map(|(keyword_id, movie_id)| {
            (k_s.contains(&keyword_id) && t_m.contains_key(&movie_id)).then_some(movie_id)
        })
        .collect::<HashSet<_>>();

    let mut res: Option<&str> = None;

    for ((movie_id, person_id), person_role_id) in ci
        .column("movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(ci.column("person_id")?.i32()?.into_no_null_iter())
        .zip(ci.column("person_role_id")?.i32()?.into_no_null_iter())
    {
        // Early exit conditions
        if !mk_s.contains(&movie_id)
            || !n_s.contains(&person_id)
            || !chn_s.contains(&person_role_id)
        {
            continue;
        }
        // Found a match, get the title(s)
        if let Some(titles) = t_m.get(&movie_id) {
            for &title in titles {
                match res {
                    None => res = Some(title),
                    Some(current_min) if title < current_min => res = Some(title),
                    _ => {}
                }
            }
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("20b,{duration:}");

    Ok(res)
}

// -- JOB Query 20b
// SELECT MIN(t.title) AS complete_downey_ironman_movie
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
// AND chn.name not like '%Sherlock%'
// and (chn.name like '%Tony%Stark%' or chn.name like '%Iron%Man%')
// AND k.keyword in
// ('superhero', 'sequel', 'second-part', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence')
// AND kt.kind = 'movie'
// AND n.name LIKE '%Downey%Robert%'
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
mod test_20b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q20b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q20b(&db)?;
        assert_eq!(res, Some("Iron Man"));
        Ok(())
    }
}
