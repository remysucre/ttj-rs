use crate::data::Data;
use ahash::HashMap;
use ahash::HashSet;
use memchr::memmem;
use polars::prelude::*;
use std::time::Instant;

// imdb_int.cast_info(person_id,movie_id,person_role_id,role_id)
// |imdb.q20a_char_name(person_role_id)
// |imdb.q20a_title(movie_id,kind_id)
// ||imdb.q20a_kind_type(kind_id)
// |imdb_int.name(person_id)
// |imdb_int.complete_cast(movie_id,subject_id,status_id)
// ||imdb.q20a_comp_cast_type1(subject_id)
// ||imdb.q20a_comp_cast_type2(status_id)
// |imdb_int.movie_keyword(movie_id,keyword_id)
// ||imdb.q20a_keyword(keyword_id)
pub fn q20a(db: &Data) -> Result<Option<&str>, PolarsError> {
    let ci = &db.ci;
    let chn = &db.chn;
    let t = &db.t;
    let kt = &db.kt;
    // let n = &db.n;
    let cc = &db.cc;
    let cct = &db.cct;
    let mk = &db.mk;
    let k = &db.k;

    let complete = memmem::Finder::new("complete");
    let iron = memmem::Finder::new(b"Iron");
    let man = memmem::Finder::new(b"Man");
    let sherlock = memmem::Finder::new(b"Sherlock");
    let tony = memmem::Finder::new(b"Tony");
    let stark = memmem::Finder::new(b"Stark");

    let start = Instant::now();

    let mut cct1_id: i32 = 0;
    let mut cct2_s = HashSet::default();

    for (id, kind) in cct.id.iter().zip(cct.kind.iter()) {
        if kind == "cast" {
            cct1_id = *id;
        } else if complete.find(kind.as_bytes()).is_some() {
            cct2_s.insert(*id);
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

    let chn_s = chn
        .name
        .iter()
        .zip(chn.id.iter())
        .filter_map(|(name, id)| {
            (sherlock.find(name.as_bytes()).is_none()
                && ((iron.find(name.as_bytes()).is_some() && man.find(name.as_bytes()).is_some())
                    || (tony.find(name.as_bytes()).is_some()
                        && stark.find(name.as_bytes()).is_some())))
            .then_some(id)
        })
        .collect::<HashSet<_>>();

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

    let k_s: HashSet<i32> =
        k.id.iter()
            .zip(k.keyword.iter())
            .filter_map(|(id, keyword)| target_keywords.contains(keyword.as_str()).then_some(*id))
            .collect();

    let kt_id = kt
        .kind
        .iter()
        .zip(kt.id.iter())
        .find(|(kind, _)| *kind == "movie")
        .map(|(_, id)| id)
        .unwrap();

    let t_m: HashMap<i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .zip(t.kind_id.iter())
            .filter_map(|(((id, title), production_year), kind_id)| {
                production_year
                    .filter(|&year| year > 1950 && kt_id == kind_id && cc_s.contains(&id))
                    .map(|_| (*id, title.as_str()))
            })
            .collect();

    // let n_s = n
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .flatten()
    //     .collect::<HashSet<_>>();

    let mk_s = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| {
            (k_s.contains(&keyword_id) && t_m.contains_key(&movie_id)).then_some(movie_id)
        })
        .collect::<HashSet<_>>();

    let matching_movies: Vec<i32> = ci
        .movie_id
        .iter()
        .zip(ci.person_role_id.iter())
        .filter_map(|(movie_id, person_role_id)| {
            person_role_id
                .filter(|id| chn_s.contains(id) && mk_s.contains(movie_id))
                .map(|_| *movie_id)
        })
        .collect();

    let res = matching_movies
        .iter()
        .filter_map(|movie_id| t_m.get(movie_id))
        .min()
        .copied();

    let duration = start.elapsed().as_secs_f32();
    println!("20a,{duration:}");

    Ok(res)
}

// -- JOB Query 20a
// SELECT MIN(t.title) AS complete_downey_ironman_movie
// FROM complete_cast AS cc,
//      comp_cast_type AS cct1,
//      comp_cast_type AS cct2,
//      char_name AS chn,
//      cast_info AS ci,
//      keyword AS k,
//      kind_type AS kt,
//      movie_keyword AS mk,
//      name AS n,
//      title AS t
// WHERE cct1.kind = 'cast'
//   AND cct2.kind LIKE '%complete%'
//   AND chn.name NOT LIKE '%Sherlock%'
//   AND (chn.name LIKE '%Tony%Stark%'
//        OR chn.name LIKE '%Iron%Man%')
//   AND k.keyword IN ('superhero',
//                     'sequel',
//                     'second-part',
//                     'marvel-comics',
//                     'based-on-comic',
//                     'tv-special',
//                     'fight',
//                     'violence')
//   AND kt.kind = 'movie'
//   AND t.production_year > 1950
//   AND kt.id = t.kind_id
//   AND t.id = mk.movie_id
//   AND t.id = ci.movie_id
//   AND t.id = cc.movie_id
//   AND mk.movie_id = ci.movie_id
//   AND mk.movie_id = cc.movie_id
//   AND ci.movie_id = cc.movie_id
//   AND chn.id = ci.person_role_id
//   AND n.id = ci.person_id
//   AND k.id = mk.keyword_id
//   AND cct1.id = cc.subject_id
//   AND cct2.id = cc.status_id;
#[cfg(test)]
mod test_20a {
    use super::*;
    use crate::data::{Data, ImdbData};

    #[test]
    fn test_q20a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q20a(&data)?;
        assert_eq!(res, Some("Disaster Movie"));
        Ok(())
    }
}
