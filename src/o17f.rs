use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q17f(db: &ImdbData) -> Result<Option<&str>, PolarsError> {
    let ci = &db.ci;
    let k = &db.k;
    let mk = &db.mk;
    let n = &db.n;

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

    let mut n_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (id, name) in n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?.into_iter())
    {
        if let (Some(id), Some(name)) = (id, name) {
            if name.contains('B') {
                n_m.entry(id).or_default().push(name);
            }
        }
    }

    let mut res: Option<&str> = None;

    for (pid, mid) in ci
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("movie_id")?.i32()?.into_iter())
    {
        if let (Some(pid), Some(mid)) = (pid, mid) {
            if mk_s.contains(&mid) {
                if let Some(names) = n_m.get(&pid) {
                    for name in names {
                        if let Some(old_name) = res.as_mut() {
                            if name < old_name {
                                *old_name = name;
                            }
                        } else {
                            res = Some(name);
                        }
                    }
                }
            }
        }
    }

    println!("17f,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 17f
// SELECT MIN(n.name) AS member_in_charnamed_movie
// FROM cast_info AS ci,
// company_name AS cn,
// keyword AS k,
// movie_companies AS mc,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE k.keyword = 'character-name-in-title'
// AND n.name LIKE '%B%'
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
mod test_17f {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q17f() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        assert_eq!(q17f(&db)?, Some("'El Galgo PornoStar', Blanquito"));
        Ok(())
    }
}
