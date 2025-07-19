use crate::data::ImdbData;
// use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

pub fn q11a(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cn = &db.cn;
    let ct = &db.ct;
    let k = &db.k;
    let lt = &db.lt;
    let mc = &db.mc;
    let mk = &db.mk;
    let ml = &db.ml;
    let t = &db.t;

    let start = Instant::now();

    let cn_m: HashMap<i32, &str> = cn
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cn.column("name")?.str()?)
        .zip(cn.column("country_code")?.str()?)
        .filter_map(|((id, name), country_code)| {
            if let (Some(id), Some(name), Some(country_code)) = (id, name, country_code) {
                if (name.contains("Film") || name.contains("Warner")) && country_code != "[pl]" {
                    Some((id, name))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let ct_s: HashSet<i32> = ct
        .column("id")?
        .i32()?
        .into_iter()
        .zip(ct.column("kind")?.str()?)
        .filter_map(|(id, kind)| {
            if let (Some(id), Some(kind)) = (id, kind) {
                if kind == "production companies" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let k_s: HashSet<i32> = k
        .column("id")?
        .i32()?
        .into_iter()
        .zip(k.column("keyword")?.str()?)
        .filter_map(|(id, keyword)| {
            if let (Some(id), Some(keyword)) = (id, keyword) {
                if keyword == "sequel" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let mk_s: HashSet<i32> = mk
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("keyword_id")?.i32()?)
        .filter_map(|(movie_id, keyword_id)| {
            if let (Some(movie_id), Some(keyword_id)) = (movie_id, keyword_id) {
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

    let lt_m: HashMap<i32, &str> = lt
        .column("id")?
        .i32()?
        .into_iter()
        .zip(lt.column("link")?.str()?)
        .filter_map(|(id, link)| {
            if let (Some(id), Some(link)) = (id, link) {
                if link.contains("follow") {
                    Some((id, link))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let ml_m: HashMap<i32, Vec<&str>> = ml
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ml.column("link_type_id")?.i32()?)
        .filter_map(|(mid, lt_id)| {
            if let (Some(mid), Some(lt_id)) = (mid, lt_id) {
                lt_m.get(&lt_id).map(|link| (mid, link))
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (mid, link)| {
            acc.entry(mid).or_default().push(link);
            acc
        });

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?)
        .zip(t.column("production_year")?.i32()?)
        .filter_map(|((id, title), production_year)| {
            if let (Some(id), Some(title), Some(production_year)) = (id, title, production_year) {
                if mk_s.contains(&id)
                    && ml_m.contains_key(&id)
                    && (1950..=2000).contains(&production_year)
                {
                    Some((id, title))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut res: Option<(&str, &str, &str)> = None;

    for (((mid, cid), ct_id), note) in mc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_id")?.i32()?.into_iter())
        .zip(mc.column("company_type_id")?.i32()?.into_iter())
        .zip(mc.column("note")?.str()?.into_iter())
    {
        if let (Some(mid), Some(cid), Some(ct_id), None) = (mid, cid, ct_id, note) {
            if ct_s.contains(&ct_id) {
                if let (Some(links), Some(name), Some(title)) =
                    (ml_m.get(&mid), cn_m.get(&cid), t_m.get(&mid))
                {
                    for link in links {
                        if let Some((old_name, old_link, old_title)) = res.as_mut() {
                            if name < old_name {
                                *old_name = name;
                            }
                            if link < old_link {
                                *old_link = link;
                            }
                            if title < old_title {
                                *old_title = title;
                            }
                        } else {
                            res = Some((name, link, title));
                        }
                    }
                }
            }
        }
    }

    println!("11a,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 11a
// SELECT MIN(cn.name) AS from_company, MIN(lt.link) AS movie_link_type, MIN(t.title) AS non_polish_sequel_movie
// FROM company_name AS cn,
// company_type AS ct,
// keyword AS k,
// link_type AS lt,
// movie_companies AS mc,
// movie_keyword AS mk,
// movie_link AS ml,
// title AS t
// WHERE cn.country_code !='[pl]' AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%') AND ct.kind ='production companies' AND k.keyword ='sequel' AND lt.link LIKE '%follow%' AND mc.note IS NULL AND t.production_year BETWEEN 1950 AND 2000 AND lt.id = ml.link_type_id AND ml.movie_id = t.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mc.movie_id AND mc.company_type_id = ct.id AND mc.company_id = cn.id AND ml.movie_id = mk.movie_id AND ml.movie_id = mc.movie_id AND mk.movie_id = mc.movie_id;
#[cfg(test)]
mod test_11a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q11a() -> Result<(), PolarsError> {
        let db = ImdbData::new();

        assert_eq!(
            q11a(&db)?,
            Some(("Churchill Films", "followed by", "Batman Beyond"))
        );
        Ok(())
    }
}
