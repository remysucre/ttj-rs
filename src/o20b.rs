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

    let cct1_s = cct1
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cct1.column("kind")?.str()?.into_iter())
        .filter_map(|(id, kind)| {
            if let (Some(id), Some(kind)) = (id, kind) {
                if kind == "cast" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let cct2_s = cct2
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cct2.column("kind")?.str()?.into_iter())
        .filter_map(|(id, kind)| {
            if let (Some(id), Some(kind)) = (id, kind) {
                if kind.contains("complete") {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let cc_s = cc
        .column("subject_id")?
        .i32()?
        .into_iter()
        .zip(cc.column("status_id")?.i32()?.into_iter())
        .zip(cc.column("movie_id")?.i32()?.into_iter())
        .filter_map(|((subject_id, status_id), movie_id)| {
            if let (Some(subject_id), Some(status_id), Some(movie_id)) =
                (subject_id, status_id, movie_id)
            {
                if cct1_s.contains(&subject_id) && cct2_s.contains(&status_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let chn_s = chn
        .column("name")?
        .str()?
        .into_iter()
        .zip(chn.column("id")?.i32()?.into_iter())
        .filter_map(|(name, id)| {
            if let (Some(name), Some(id)) = (name, id) {
                // Could be further optimized by reordering depending on the selectivity of the filters
                if (name.contains("Tony") && name.contains("Stark")
                    || name.contains("Iron") && name.contains("Man"))
                    && !name.contains("Sherlock")
                {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let k_s = k
        .column("id")?
        .i32()?
        .into_iter()
        .zip(k.column("keyword")?.str()?.into_iter())
        .filter_map(|(id, keyword)| {
            if let (Some(id), Some(keyword)) = (id, keyword) {
                if matches!(
                    keyword,
                    "superhero"
                        | "sequel"
                        | "second-part"
                        | "marvel-comics"
                        | "based-on-comic"
                        | "tv-special"
                        | "fight"
                        | "violence"
                ) {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let kt_s = kt
        .column("kind")?
        .str()?
        .into_iter()
        .zip(kt.column("id")?.i32()?.into_iter())
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if kind == "movie" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let t_m: HashMap<i32, Vec<&str>> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .zip(t.column("production_year")?.i32()?.into_iter())
        .zip(t.column("kind_id")?.i32()?.into_iter())
        .filter_map(|(((id, title), production_year), kind_id)| {
            if let (Some(id), Some(title), Some(production_year), Some(kind_id)) =
                (id, title, production_year, kind_id)
            {
                if production_year > 2000 && kt_s.contains(&kind_id) {
                    Some((id, title))
                } else {
                    None
                }
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
        .into_iter()
        .zip(n.column("name")?.str()?.into_iter())
        .filter_map(|(id, name)| {
            if let (Some(id), Some(name)) = (id, name) {
                if name.contains("Downey") && name.contains("Robert") {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mk_s = mk
        .column("keyword_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("movie_id")?.i32()?.into_iter())
        .filter_map(|(keyword_id, movie_id)| {
            if let (Some(keyword_id), Some(movie_id)) = (keyword_id, movie_id) {
                if k_s.contains(&keyword_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let mut res = None;

    for ((movie_id, person_id), person_role_id) in ci
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("person_id")?.i32()?.into_iter())
        .zip(ci.column("person_role_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(person_id), Some(person_role_id)) =
            (movie_id, person_id, person_role_id)
        {
            if chn_s.contains(&person_role_id)
                && n_s.contains(&person_id)
                && cc_s.contains(&movie_id)
                && mk_s.contains(&movie_id)
            {
                if let Some(titles) = t_m.get(&movie_id) {
                    for title in titles {
                        if let Some(old_title) = res.as_ref() {
                            if title < old_title {
                                res = Some(*title);
                            }
                        } else {
                            res = Some(*title);
                        }
                    }
                }
            }
        }
    }

    let duration = start.elapsed().as_secs_f32();
    dbg!(duration);

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
