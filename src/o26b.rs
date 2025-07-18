use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q26b(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cc = &db.cc;
    let cct1 = &db.cct;
    let cct2 = &db.cct;
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

    let start = Instant::now();

    let chn_m: HashMap<i32, Vec<&str>> = chn
        .column("name")?
        .str()?
        .into_iter()
        .zip(chn.column("id")?.i32()?.into_iter())
        .filter_map(|(name, id)| {
            if let (Some(name), Some(id)) = (name, id) {
                if name.contains("man") || name.contains("Man") {
                    Some((name, id))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (name, id)| {
            acc.entry(id).or_default().push(name);
            acc
        });

    let cct1_s: HashSet<i32> = cct1
        .column("kind")?
        .str()?
        .into_iter()
        .zip(cct1.column("id")?.i32()?.into_iter())
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if kind == "cast" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let cct2_s: HashSet<i32> = cct2
        .column("kind")?
        .str()?
        .into_iter()
        .zip(cct2.column("id")?.i32()?.into_iter())
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if kind.contains("complete") {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let cc_s: HashSet<i32> = cc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(cc.column("subject_id")?.i32()?.into_iter())
        .zip(cc.column("status_id")?.i32()?.into_iter())
        .filter_map(|((movie_id, subject_id), status_id)| {
            if let (Some(movie_id), Some(subject_id), Some(status_id)) =
                (movie_id, subject_id, status_id)
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
        .collect();

    let it_s: HashSet<i32> = it
        .column("info")?
        .str()?
        .into_iter()
        .zip(it.column("id")?.i32()?.into_iter())
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "rating" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let k_s = k
        .column("id")?
        .i32()?
        .into_iter()
        .zip(k.column("keyword")?.str()?.into_iter())
        .filter_map(|(id, keyword)| {
            if let (Some(id), Some(keyword)) = (id, keyword) {
                if matches!(
                    keyword,
                    "superhero" | "marvel-comics" | "based-on-comic" | "fight"
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

    let mk_s = mk
        .column("keyword_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("movie_id")?.i32()?.into_iter())
        .filter_map(|(keyword_id, movie_id)| {
            if let (Some(keyword_id), Some(movie_id)) = (keyword_id, movie_id) {
                if k_s.contains(&keyword_id) && cc_s.contains(&movie_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let mut mi_idx_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((movie_id, info), info_type_id) in mi_idx
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi_idx.column("info")?.str()?.into_iter())
        .zip(mi_idx.column("info_type_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(info), Some(info_type_id)) = (movie_id, info, info_type_id) {
            if info > "8.0" && it_s.contains(&info_type_id) && mk_s.contains(&movie_id) {
                mi_idx_m.entry(movie_id).or_default().push(info);
            }
        }
    }

    let kt_s: HashSet<i32> = kt
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
        .collect();

    let t_m: HashMap<i32, Vec<&str>> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("kind_id")?.i32()?.into_iter())
        .zip(t.column("title")?.str()?.into_iter())
        .zip(t.column("production_year")?.i32()?.into_iter())
        .filter_map(|(((id, kind_id), title), production_year)| {
            if let (Some(id), Some(kind_id), Some(title), Some(production_year)) =
                (id, kind_id, title, production_year)
            {
                if production_year > 2005 && kt_s.contains(&kind_id) && mi_idx_m.contains_key(&id) {
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

    let mut res: Option<(&str, &str, &str)> = None;

    for (movie_id, person_role_id) in ci
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("person_role_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(person_role_id)) = (movie_id, person_role_id) {
            if let (Some(titles), Some(char_names), Some(info)) = (
                t_m.get(&movie_id),
                chn_m.get(&person_role_id),
                mi_idx_m.get(&movie_id),
            ) {
                for title in titles {
                    for char_name in char_names {
                        for i in info {
                            if let Some((old_char_name, old_info, old_title)) = res.as_mut() {
                                if title < old_title {
                                    *old_title = *title;
                                }
                                if char_name < old_char_name {
                                    *old_char_name = *char_name;
                                }
                                if i < old_info {
                                    *old_info = *i
                                }
                            } else {
                                res = Some((char_name, i, title));
                            }
                        }
                    }
                }
            }
        }
    }

    println!("{:}", start.elapsed().as_secs_f32());

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
mod test_26b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q26b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q26b(&db)?;
        assert_eq!(res, Some(("Bank Manager", "8.2", "Inception")));
        Ok(())
    }
}
