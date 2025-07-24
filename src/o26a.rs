use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem;
use polars::prelude::*;
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
pub fn q26a(db: &Data) -> Result<Option<(&str, &str, &str, &str)>, PolarsError> {
    let cc = &db.cc;
    let cct = &db.cct;
    let chn = &db.chn;
    let ci = &db.ci;
    let it = &db.it;
    let k = &db.k;
    let kt = &db.kt;
    let mi_idx = &db.mi_idx;
    let mk = &db.mk;
    let n = &db.n;
    let t = &db.t;

    let complete_finder = memmem::Finder::new("complete");
    let man_finder = memmem::Finder::new("man");
    let man2_finder = memmem::Finder::new("Man");

    let n_m: HashMap<i32, &str> =
        n.id.iter()
            .zip(n.name.iter())
            .map(|(id, name)| (*id, name.as_str()))
            .collect();

    let start = Instant::now();

    let mut cct1_id: i32 = 0;
    let mut cct2_s = HashSet::default();

    for (id, kind) in cct.id.iter().zip(cct.kind.iter()) {
        if kind == "cast" {
            cct1_id = *id;
        } else if complete_finder.find(kind.as_bytes()).is_some() {
            cct2_s.insert(*id);
        }
    }

    let cc_s: HashSet<i32> = cc
        .movie_id
        .iter()
        .zip(cc.subject_id.iter())
        .zip(cc.status_id.iter())
        .filter_map(|((movie_id, subject_id), status_id)| {
            (cct1_id == *subject_id && cct2_s.contains(&status_id)).then_some(*movie_id)
        })
        .collect();

    let kt_s: HashSet<i32> = kt
        .kind
        .iter()
        .zip(kt.id.iter())
        .filter_map(|(kind, id)| (kind == "movie").then_some(*id))
        .collect();

    let t_m: HashMap<i32, &str> =
        t.id.iter()
            .zip(t.kind_id.iter())
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|(((id, kind_id), title), production_year)| {
                production_year
                    .filter(|&production_year| {
                        production_year > 2000 && kt_s.contains(&kind_id) && cc_s.contains(&id)
                    })
                    .map(|_| (*id, title.as_str()))
            })
            .collect();

    let chn_m: HashMap<i32, &str> = chn
        .name
        .iter()
        .zip(chn.id.iter())
        .filter_map(|(name, id)| {
            (man_finder.find(name.as_bytes()).is_some()
                || man2_finder.find(name.as_bytes()).is_some())
            .then_some((*id, name.as_str()))
        })
        .collect();

    let it_s: HashSet<i32> = it
        .info
        .iter()
        .zip(it.id.iter())
        .filter_map(|(info, id)| (info == "rating").then_some(*id))
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

    let k_s: HashSet<i32> =
        k.id.iter()
            .zip(k.keyword.iter())
            .filter_map(|(id, keyword)| target_keywords.contains(keyword.as_str()).then_some(*id))
            .collect();

    let mk_s: HashSet<i32> = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| {
            (k_s.contains(&keyword_id) && t_m.contains_key(&movie_id)).then_some(*movie_id)
        })
        .collect();

    let mi_idx_m: HashMap<i32, Vec<&str>> = mi_idx
        .movie_id
        .iter()
        .zip(mi_idx.info.iter())
        .zip(mi_idx.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            (info.as_str() > "7.0" && it_s.contains(&info_type_id) && mk_s.contains(&movie_id))
                .then_some((*movie_id, info))
        })
        .fold(HashMap::default(), |mut acc, (movie_id, info)| {
            acc.entry(movie_id).or_default().push(info);
            acc
        });

    let mut res: Option<(&str, &str, &str, &str)> = None;

    for ((movie_id, person_id), person_role_id) in ci
        .movie_id
        .iter()
        .zip(ci.person_id.iter())
        .zip(ci.person_role_id.iter())
    {
        if let Some(person_role_id) = person_role_id
            && let Some(info) = mi_idx_m.get(&movie_id)
            && let Some(title) = t_m.get(&movie_id)
            && let Some(char_name) = chn_m.get(&person_role_id)
            && let Some(name) = n_m.get(&person_id)
        {
            res = match res {
                Some((old_name, old_title, old_char_name, old_info)) => Some((
                    name.min(&old_name),
                    title.min(&old_title),
                    char_name.min(&old_char_name),
                    info.iter().min().unwrap().min(&old_info),
                )),
                None => Some((name, title, char_name, info.iter().min().unwrap())),
            };
        }
    }

    println!("26a,{:}", start.elapsed().as_secs_f32());

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
        let data = Data::new(&db);
        let res = q26a(&data)?;
        assert_eq!(
            res,
            Some(("Acereda, Hermie", "3:10 to Yuma", "'Agua' Man", "7.1"))
        );
        Ok(())
    }
}
