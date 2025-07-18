use crate::data::ImdbData;
// use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

pub fn q17d(db: &ImdbData) -> Result<Option<&str>, PolarsError> {
    let ci = &db.ci;
    let k = &db.k;
    let mk = &db.mk;
    let n = &db.n;
    let cn = &db.cn;
    let mc = &db.mc;
    // let t = &db.t;

    // FK-PK optimization: FK is ci.movie_id and PK is title.id
    // let t_s: HashSet<i32> = t
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .flatten()
    //     .collect();

    // let cn_s: HashSet<i32> = cn.column("id")?.i32()?.into_iter().flatten().collect();

    let start = Instant::now();

    let k_s: HashSet<i32> = k
        .column("keyword")?
        .str()?
        .into_iter()
        .zip(k.column("id")?.i32()?.into_iter())
        .filter_map(|(keyword, id)| {
            if let (Some(keyword), Some(id)) = (keyword, id) {
                if keyword == "character-name-in-title" {
                    Some(id)
                } else {
                    None
                }
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
        .collect();

    let mc_s: HashSet<i32> = mc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .flat_map(|movie_id| {
            if mk_s.contains(&movie_id?) {
                movie_id
            } else {
                None
            }
        })
        .collect();

    let n_m: HashMap<i32, &str> = n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?.into_iter())
        .filter_map(|(id, name)| {
            if let (Some(id), Some(name)) = (id, name) {
                if name.contains("Bert") {
                    Some((id, name))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut res: Option<&str> = None;

    for (pid, mid) in ci
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("movie_id")?.i32()?.into_iter())
    {
        if let (Some(pid), Some(mid)) = (pid, mid) {
            if let Some(name) = n_m.get(&pid) {
                if mc_s.contains(&mid) {
                    if let Some(old_name) = res.as_mut() {
                        if name < old_name {
                            *old_name = *name;
                        }
                    } else {
                        res = Some(name);
                    }
                }
            }
        }
    }

    dbg!(start.elapsed());

    Ok(res)
}

// -- JOB Query 17d
// SELECT MIN(n.name) AS member_in_charnamed_movie
// FROM cast_info AS ci,
// company_name AS cn,
// keyword AS k,
// movie_companies AS mc,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE k.keyword = 'character-name-in-title'
// AND n.name LIKE '%Bert%'
// AND n.id = ci.person_id
// AND ci.movie_id = t.id
// AND t.id = mk.movie_id
// AND mk.keyword_id = k.id
// AND t.id = mc.movie_id
// AND mc.company_id = cn.id
// AND ci.movie_id = mc.movie_id
// AND ci.movie_id = mk.movie_id
// AND mc.movie_id = mk.movie_id;
#[cfg(test)]
mod test_17d {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q17d() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q17d(&db)?;
        assert_eq!(res, Some("Abrahamsson, Bertil"));
        Ok(())
    }
}
