use crate::data::ImdbData;
use polars::prelude::*;
use std::time::Instant;

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

pub fn q17b(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    let ci = &db.ci;
    let k = &db.k;
    let mk = &db.mk;
    let n = &db.n;
    // let cn = &db.cn;
    let mc = &db.mc;
    // let t = &db.t;

    // FK-PK optimization: FK is ci.movie_id and PK is title.id
    // let t_s: HashSet<i32> = t
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .flatten()
    //     .collect();

    // let cn_s: HashSet<i32> = cn
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .flatten()
    //     .collect();

    let start = Instant::now();

    let k_s: HashSet<i32> = k
        .column("keyword")?
        .str()?
        .into_iter()
        .zip(k.column("id")?.i32()?)
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
        .zip(mk.column("movie_id")?.i32()?)
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
        .filter_map(|movie_id| {
            if let Some(movie_id) = movie_id {
                if mk_s.contains(&movie_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let n_m: HashMap<i32, &str> = n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?)
        .filter_map(|(id, name)| {
            if let (Some(id), Some(name)) = (id, name) {
                if name.starts_with('Z') {
                    Some((id, name))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut res: Option<(&str, &str)> = None;

    for (pid, mid) in ci
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("movie_id")?.i32()?)
    {
        if let (Some(pid), Some(mid)) = (pid, mid) {
            if let Some(name) = n_m.get(&pid) {
                if mc_s.contains(&mid) {
                    if let Some((old_name, old_name2)) = res.as_mut() {
                        if name < old_name {
                            *old_name = *name;
                            *old_name2 = *name;
                        }
                    } else {
                        res = Some((name, name));
                    }
                }
            }
        }
    }

    println!("17b,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 17b
// SELECT MIN(n.name) AS member_in_charnamed_movie, MIN(n.name) AS a1
// FROM cast_info AS ci,
// company_name AS cn,
// keyword AS k,
// movie_companies AS mc,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE k.keyword = 'character-name-in-title'
// AND n.name LIKE 'Z%'
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
mod test_17b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q17b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q17b(&db)?;
        assert_eq!(res, Some(("Z'Dar, Robert", "Z'Dar, Robert")));
        Ok(())
    }
}
