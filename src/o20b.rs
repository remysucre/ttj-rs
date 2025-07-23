use crate::data::Data;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

use memchr::memmem;

#[inline]
fn matches(haystack: &str, finder: &memmem::Finder) -> bool {
    finder.find(haystack.as_bytes()).is_some()
}

pub fn q20b(db: &Data) -> Result<Option<&str>, PolarsError> {
    let ci = &db.ci;
    let chn = &db.chn;
    let t = &db.t;
    let kt = &db.kt;
    let n = &db.n;
    let cc = &db.cc;
    let cct = &db.cct;
    let mk = &db.mk;
    let k = &db.k;

    let downey = memmem::Finder::new(b"Downey");
    let robert = memmem::Finder::new(b"Robert");
    let iron = memmem::Finder::new(b"Iron");
    let man = memmem::Finder::new(b"Man");
    let sherlock = memmem::Finder::new(b"Sherlock");
    let tony = memmem::Finder::new(b"Tony");
    let stark = memmem::Finder::new(b"Stark");

    let start = Instant::now();

    let n_s: HashSet<i32> =
        n.id.iter()
            .zip(n.name.iter())
            .filter_map(|(id, name)| {
                if matches(name, &downey) && matches(name, &robert) {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();

    let chn_s: HashSet<i32> = chn
        .name
        .iter()
        .zip(chn.id.iter())
        .filter_map(|(name, id)| {
            (!matches(name, &sherlock)
                && (matches(name, &tony) && matches(name, &stark)
                    || matches(name, &iron) && matches(name, &man)))
            .then_some(*id)
        })
        .collect();

    let ci_s: HashSet<i32> = ci
        .person_id
        .iter()
        .zip(ci.person_role_id.iter())
        .zip(ci.movie_id.iter())
        .filter_map(|((person_id, person_role_id), movie_id)| {
            (n_s.contains(person_id) && chn_s.contains(&(*person_role_id)?)).then_some(*movie_id)
        })
        .collect();

    let mut cct1_id: i32 = 0;
    let mut cct2_s = HashSet::default();

    for (id, kind) in cct.id.iter().zip(cct.kind.iter()) {
        if kind == "cast" {
            cct1_id = *id;
        } else if kind.contains("complete") {
            cct2_s.insert(*id);
        }
    }

    let cc_s: HashSet<i32> = cc
        .subject_id
        .iter()
        .zip(cc.status_id.iter())
        .zip(cc.movie_id.iter())
        .filter_map(|((subject_id, status_id), movie_id)| {
            movie_id.and_then(|mid| {
                (ci_s.contains(&mid) && *subject_id == cct1_id && cct2_s.contains(status_id))
                    .then_some(mid)
            })
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

    let k_s: HashSet<i32> =
        k.id.iter()
            .zip(k.keyword.iter())
            .filter_map(|(id, keyword)| target_keywords.contains(keyword.as_str()).then_some(*id))
            .collect();

    let mk_s = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| {
            (cc_s.contains(movie_id) && k_s.contains(&keyword_id)).then_some(*movie_id)
        })
        .collect::<HashSet<_>>();

    let kt_id = kt
        .kind
        .iter()
        .zip(kt.id.iter())
        .find(|(kind, _)| *kind == "movie")
        .map(|(_, id)| id)
        .unwrap();

    let mut res: Option<&str> = None;

    for (((id, title), production_year), kind_id) in
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .zip(t.kind_id.iter())
    {
        if let Some(production_year) = production_year
            && mk_s.contains(id)
            && *production_year > 2000
            && kt_id == kind_id
        {
            res = match res {
                Some(old_title) => Some(title.as_str().min(old_title)),
                None => Some(title),
            };
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
        let data = Data::new(&db);
        let res = q20b(&data)?;
        assert_eq!(res, Some("Iron Man"));
        Ok(())
    }
}
