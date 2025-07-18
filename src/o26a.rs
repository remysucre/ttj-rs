use crate::data::ImdbData;
use polars::prelude::*;
use rustc_hash::{FxBuildHasher, FxHashMap as HashMap, FxHashSet as HashSet};
use std::time::Instant;

// imdb_int.cast_info(person_id,movie_id,person_role_id,role_id)
// |imdb.q26a_char_name(person_role_id)
// |imdb_int.name(person_id)
// |imdb.q26a_title(movie_id,kind_id)
// ||imdb.q26a_kind_type(kind_id)
// |imdb_int.complete_cast(movie_id,subject_id,status_id)
// ||imdb.q26a_comp_cast_type1(subject_id)
// ||imdb.q26a_comp_cast_type2(status_id)
// |imdb_int.movie_keyword(movie_id,keyword_id)
// ||imdb.q26a_keyword(keyword_id)
// |imdb.q26a_movie_info_idx(movie_id,info_type_id)
// ||imdb.q26a_info_type(info_type_id)
pub fn q26a(db: &ImdbData) -> Result<Option<(&str, &str, &str, &str)>, PolarsError> {
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
    let n = &db.n;
    let t = &db.t;

    let n_m: HashMap<i32, &str> = n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?)
        .fold(
            HashMap::with_capacity_and_hasher(n.height(), FxBuildHasher::default()),
            |mut acc, (id, name)| {
                if let (Some(id), Some(name)) = (id, name) {
                    acc.insert(id, name);
                }
                acc
            },
        );

    let start = Instant::now();

    let cct1_s: HashSet<i32> = cct1
        .column("kind")?
        .str()?
        .into_iter()
        .zip(cct1.column("id")?.i32()?)
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
        .zip(cct2.column("id")?.i32()?)
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

    let mut cc_s = HashSet::with_capacity_and_hasher(cc.height(), FxBuildHasher::default());

    cc.column("movie_id")?
        .i32()?
        .into_iter()
        .zip(cc.column("subject_id")?.i32()?)
        .zip(cc.column("status_id")?.i32()?)
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
        .for_each(|movie_id| {
            cc_s.insert(movie_id);
        });

    let kt_s: HashSet<i32> = kt
        .column("kind")?
        .str()?
        .into_iter()
        .zip(kt.column("id")?.i32()?)
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if kind == "movie" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("kind_id")?.i32()?)
        .zip(t.column("title")?.str()?)
        .zip(t.column("production_year")?.i32()?)
        .filter_map(|(((id, kind_id), title), production_year)| {
            if let (Some(id), Some(kind_id), Some(title), Some(production_year)) =
                (id, kind_id, title, production_year)
            {
                if production_year > 2000 && kt_s.contains(&kind_id) && cc_s.contains(&id) {
                    Some((id, title))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(
            HashMap::with_capacity_and_hasher(t.height(), FxBuildHasher::default()),
            |mut acc, (id, title)| {
                acc.insert(id, title);
                acc
            },
        );

    let mut chn_m = HashMap::with_capacity_and_hasher(chn.height(), FxBuildHasher::default());

    chn.column("name")?
        .str()?
        .into_iter()
        .zip(chn.column("id")?.i32()?)
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
        .for_each(|(name, id)| {
            chn_m.insert(id, name);
        });

    let it_s: HashSet<i32> = it
        .column("info")?
        .str()?
        .into_iter()
        .zip(it.column("id")?.i32()?)
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
        .zip(k.column("keyword")?.str()?)
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

    let mut mk_s = HashSet::with_capacity_and_hasher(mk.height(), FxBuildHasher::default());

    mk.column("keyword_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("movie_id")?.i32()?)
        .filter_map(|(keyword_id, movie_id)| {
            if let (Some(keyword_id), Some(movie_id)) = (keyword_id, movie_id) {
                if k_s.contains(&keyword_id) && t_m.contains_key(&movie_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .for_each(|movie_id| {
            mk_s.insert(movie_id);
        });

    let mi_idx_m: HashMap<i32, Vec<&str>> = mi_idx
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi_idx.column("info")?.str()?)
        .zip(mi_idx.column("info_type_id")?.i32()?)
        .filter_map(|((movie_id, info), info_type_id)| {
            if let (Some(movie_id), Some(info), Some(info_type_id)) = (movie_id, info, info_type_id)
            {
                if info > "7.0" && it_s.contains(&info_type_id) && mk_s.contains(&movie_id) {
                    Some((movie_id, info))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(
            HashMap::with_capacity_and_hasher(mi_idx.height(), FxBuildHasher::default()),
            |mut acc, (movie_id, info)| {
                acc.entry(movie_id).or_default().push(info);
                acc
            },
        );

    let mut res: Option<(&str, &str, &str, &str)> = None;

    for ((movie_id, person_id), person_role_id) in ci
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("person_id")?.i32()?)
        .zip(ci.column("person_role_id")?.i32()?)
    {
        if let (Some(movie_id), Some(person_id), Some(person_role_id)) =
            (movie_id, person_id, person_role_id)
        {
            if let Some(info) = mi_idx_m.get(&movie_id)
                && let Some(title) = t_m.get(&movie_id)
                && let Some(char_name) = chn_m.get(&person_role_id)
                && let Some(name) = n_m.get(&person_id)
            {
                for i in info {
                    if let Some((old_name, old_title, old_char_name, old_info)) = res.as_mut() {
                        if name < old_name {
                            *old_name = *name;
                        }
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
                        res = Some((name, title, char_name, i));
                    }
                }
            }
        }
    }

    dbg!(start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB 26a
// SELECT MIN(chn.name) AS character_name,
//        MIN(mi_idx.info) AS rating,
//        MIN(n.name) AS playing_actor,
//        MIN(t.title) AS complete_hero_movie
// FROM complete_cast AS cc,
//      comp_cast_type AS cct1,
//      comp_cast_type AS cct2,
//      char_name AS chn,
//      cast_info AS ci,
//      info_type AS it2,
//      keyword AS k,
//      kind_type AS kt,
//      movie_info_idx AS mi_idx,
//      movie_keyword AS mk,
//      name AS n,
//      title AS t
// WHERE cct1.kind = 'cast'
//   AND cct2.kind LIKE '%complete%'
//   AND chn.name IS NOT NULL
//   AND (chn.name LIKE '%man%'
//        OR chn.name LIKE '%Man%')
//   AND it2.info = 'rating'
//   AND k.keyword IN ('superhero',
//                     'marvel-comics',
//                     'based-on-comic',
//                     'tv-special',
//                     'fight',
//                     'violence',
//                     'magnet',
//                     'web',
//                     'claw',
//                     'laser')
//   AND kt.kind = 'movie'
//   AND mi_idx.info > '7.0'
//   AND t.production_year > 2000
//   AND kt.id = t.kind_id
//   AND t.id = mk.movie_id
//   AND t.id = ci.movie_id
//   AND t.id = cc.movie_id
//   AND t.id = mi_idx.movie_id
//   AND mk.movie_id = ci.movie_id
//   AND mk.movie_id = cc.movie_id
//   AND mk.movie_id = mi_idx.movie_id
//   AND ci.movie_id = cc.movie_id
//   AND ci.movie_id = mi_idx.movie_id
//   AND cc.movie_id = mi_idx.movie_id
//   AND chn.id = ci.person_role_id
//   AND n.id = ci.person_id
//   AND k.id = mk.keyword_id
//   AND cct1.id = cc.subject_id
//   AND cct2.id = cc.status_id
//   AND it2.id = mi_idx.info_type_id;
#[cfg(test)]
mod test_26a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q26a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q26a(&db)?;
        assert_eq!(
            res,
            Some(("Acereda, Hermie", "3:10 to Yuma", "'Agua' Man", "7.1"))
        );
        Ok(())
    }
}
