use crate::data::ImdbData;
use ahash::HashMap;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q20c(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
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

    let n_m: HashMap<i32, Vec<&str>> = n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?.into_iter())
        .fold(HashMap::default(), |mut acc, (id, name)| {
            if let (Some(id), Some(name)) = (id, name) {
                acc.entry(id).or_default().push(name);
                acc
            } else {
                acc
            }
        });

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
                if name.contains("man") || name.contains("Man") {
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
                        | "marvel-comics"
                        | "based-on-comic"
                        | "tv-special"
                        | "fight"
                        | "violence"
                        | "magnet"
                        | "web"
                        | "claw"
                        | "laser"
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

    let mut res: Option<(&str, &str)> = None;

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
                && cc_s.contains(&movie_id)
                && mk_s.contains(&movie_id)
            {
                if let (Some(name), Some(titles)) = (n_m.get(&person_id), t_m.get(&movie_id)) {
                    for title in titles {
                        for name in name {
                            if let Some((old_name, old_title)) = res.as_mut() {
                                if title < old_title {
                                    *old_title = *title;
                                }
                                if name < old_name {
                                    *old_name = name;
                                }
                            } else {
                                res = Some((name, title));
                            }
                        }
                    }
                }
            }
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("{:}", duration);

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
