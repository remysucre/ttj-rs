use crate::data::Data;
use ahash::HashSet;
use ahash::{HashMap, HashSetExt};
use memchr::memmem;
use polars::prelude::*;
use std::time::Instant;

pub fn q20c(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
    let ci = &db.ci;
    let chn = &db.chn;
    let t = &db.t;
    let kt = &db.kt;
    let n = &db.n;
    let cc = &db.cc;
    let cct = &db.cct;
    let mk = &db.mk;
    let k = &db.k;

    let n_m: HashMap<&i32, &str> =
        n.id.iter()
            .zip(n.name.iter())
            .map(|(id, name)| (id, name.as_str()))
            .collect();

    let man = memmem::Finder::new(b"man");
    let big_man = memmem::Finder::new(b"Man");

    let start = Instant::now();

    let mut cct1_id: i32 = 0;
    let mut cct2_s = HashSet::new();

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

    let chn_s = chn
        .name
        .iter()
        .zip(chn.id.iter())
        .filter_map(|(name, id)| {
            (big_man.find(name.as_bytes()).is_some() || man.find(name.as_bytes()).is_some())
                .then_some(id)
        })
        .collect::<HashSet<_>>();

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
                    .filter(|&year| year > 2000 && kt_id == kind_id && cc_s.contains(&id))
                    .map(|_| (*id, title.as_str()))
            })
            .collect();

    let mk_s = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| {
            (k_s.contains(&keyword_id) && t_m.contains_key(&movie_id)).then_some(movie_id)
        })
        .collect::<HashSet<_>>();

    let mut res: Option<(&str, &str)> = None;

    for ((movie_id, person_id), person_role_id) in ci
        .movie_id
        .iter()
        .zip(ci.person_id.iter())
        .zip(ci.person_role_id.iter())
    {
        if let Some(person_role_id) = person_role_id
            && chn_s.contains(person_role_id)
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
mod test_q20c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q20c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q20c(&data)?;
        assert_eq!(res, Some(("Abell, Alistair", "...And Then I...")));
        Ok(())
    }
}
