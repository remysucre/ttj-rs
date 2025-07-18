use crate::data::ImdbData;
// use foldhash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

pub fn q16a(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    let an = &db.an;
    let ci = &db.ci;
    let cn = &db.cn;
    let k = &db.k;
    let mk = &db.mk;
    let t = &db.t;
    let mc = &db.mc;

    let start = Instant::now();

    let k_s: HashSet<i32> = k
        .column("id")?
        .i32()?
        .into_iter()
        .zip(k.column("keyword")?.str()?.into_iter())
        .filter_map(|(id, keyword)| {
            if keyword? == "character-name-in-title" {
                id
            } else {
                None
            }
        })
        .collect();

    let mk_s: HashSet<i32> = mk
        .column("keyword_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("movie_id")?.i32()?.into_iter())
        .filter_map(|(keyword_id, movie_id)| {
            if k_s.contains(&keyword_id?) {
                movie_id
            } else {
                None
            }
        })
        .collect();

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .zip(t.column("episode_nr")?.i32()?.into_iter())
        .filter_map(|((movie_id, title), episode_nr)| {
            if mk_s.contains(&movie_id?) && (50..100).contains(&episode_nr?) {
                Some((movie_id?, title?))
            } else {
                None
            }
        })
        .collect();

    let cn_s: HashSet<i32> = cn
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cn.column("country_code")?.str()?.into_iter())
        .filter_map(|(id, country_code)| if country_code? == "[us]" { id } else { None })
        .collect();

    let mc_s: HashSet<i32> = mc
        .column("company_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("movie_id")?.i32()?.into_iter())
        .filter_map(|(company_id, movie_id)| {
            if t_m.contains_key(&movie_id?) && cn_s.contains(&company_id?) {
                movie_id
            } else {
                None
            }
        })
        .collect();

    let an_m: HashMap<i32, Vec<&str>> = an
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(an.column("name")?.str()?.into_iter())
        .filter_map(|(person_id, name)| Some((person_id?, name?)))
        .fold(HashMap::default(), |mut acc, (person_id, name)| {
            acc.entry(person_id).or_default().push(name);
            acc
        });

    let mut res: Option<(&str, &str)> = None;

    for (x, y) in ci
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("movie_id")?.i32()?.into_iter())
    {
        if let (Some(person_id), Some(movie_id)) = (x, y) {
            if mc_s.contains(&movie_id) {
                if let (Some(title), Some(ns)) = (t_m.get(&movie_id), an_m.get(&person_id)) {
                    for name in ns {
                        if let Some((old_name, old_title)) = res.as_mut() {
                            if *name < *old_name {
                                *old_name = name;
                            }
                            if *title < *old_title {
                                *old_title = title;
                            }
                        } else {
                            res = Some((name, title));
                        }
                    }
                }
            }
        }
    }

    println!("{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 16a
// SELECT MIN(an.name) AS cool_actor_pseudonym, MIN(t.title) AS series_named_after_char
// FROM aka_name AS an,
// cast_info AS ci,
// company_name AS cn,
// keyword AS k,
// movie_companies AS mc,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE cn.country_code = '[us]'
// AND k.keyword = 'character-name-in-title'
// AND t.episode_nr >= 50
// AND t.episode_nr < 100
// AND an.person_id = n.id
// AND n.id = ci.person_id
// AND ci.movie_id = t.id
// AND t.id = mk.movie_id
// AND mk.keyword_id = k.id
// AND t.id = mc.movie_id
// AND mc.company_id = cn.id
// AND an.person_id = ci.person_id
// AND ci.movie_id = mc.movie_id
// AND ci.movie_id = mk.movie_id
// AND mc.movie_id = mk.movie_id;
#[cfg(test)]
mod test_16a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q16a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q16a(&db)?;
        assert_eq!(
            res,
            Some(("Adams, Stan", "Carol Burnett vs. Anthony Perkins"))
        );
        Ok(())
    }
}
