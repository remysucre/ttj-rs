use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q11c(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cn = &db.cn;
    let ct = &db.ct;
    let k = &db.k;
    let lt = &db.lt;
    let mc = &db.mc;
    let mk = &db.mk;
    let ml = &db.ml;
    let t = &db.t;

    let lt_s: HashSet<i32> = lt.column("id")?.i32()?.into_iter().flatten().collect();

    let start = Instant::now();

    let mut cn_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, name), country_code) in cn
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cn.column("name")?.str()?.into_iter())
        .zip(cn.column("country_code")?.str()?.into_iter())
    {
        if let (Some(id), Some(name), Some(country_code)) = (id, name, country_code) {
            if country_code != "[pl]"
                && (name.starts_with("20th Century Fox")
                    || name.starts_with("Twentieth Century Fox"))
            {
                cn_m.entry(id).or_default().push(name);
            }
        }
    }

    let ct_s: HashSet<i32> = ct
        .column("id")?
        .i32()?
        .into_iter()
        .zip(ct.column("kind")?.str()?.into_iter())
        .filter_map(|(id, kind)| {
            if let (Some(id), Some(kind)) = (id, kind) {
                if kind != "production companies" {
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
        .zip(k.column("keyword")?.str()?.into_iter())
        .filter_map(|(id, keyword)| {
            if let (Some(id), Some(keyword)) = (id, keyword) {
                if matches!(keyword, "sequel" | "revenge" | "based-on-novel") {
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

    let ml_s: HashSet<i32> = ml
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ml.column("link_type_id")?.i32()?)
        .filter_map(|(movie_id, link_type_id)| {
            if let (Some(movie_id), Some(link_type_id)) = (movie_id, link_type_id) {
                if lt_s.contains(&link_type_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut t_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, title), production_year) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .zip(t.column("production_year")?.i32()?.into_iter())
    {
        if let (Some(id), Some(title), Some(production_year)) = (id, title, production_year) {
            if mk_s.contains(&id) && ml_s.contains(&id) && production_year > 1950 {
                t_m.entry(id).or_default().push(title);
            }
        }
    }

    let mut res: Option<(&str, &str, &str)> = None;

    for (((mid, cid), ct_id), note) in mc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_id")?.i32()?.into_iter())
        .zip(mc.column("company_type_id")?.i32()?.into_iter())
        .zip(mc.column("note")?.str()?.into_iter())
    {
        if let (Some(mid), Some(cid), Some(ct_id), Some(note)) = (mid, cid, ct_id, note) {
            if ct_s.contains(&ct_id) && ml_s.contains(&mid) {
                if let (Some(names), Some(titles)) = (cn_m.get(&cid), t_m.get(&mid)) {
                    for name in names {
                        for title in titles {
                            if let Some((old_name, old_note, old_title)) = res.as_mut() {
                                if name < old_name {
                                    *old_name = name;
                                }
                                if title < old_title {
                                    *old_title = title;
                                }

                                if note < *old_note {
                                    *old_note = note;
                                }
                            } else {
                                res = Some((name, note, title));
                            }
                        }
                    }
                }
            }
        }
    }

    dbg!(start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 11c
// SELECT MIN(cn.name) AS from_company, MIN(mc.note) AS production_note, MIN(t.title) AS movie_based_on_book
// FROM company_name AS cn,
// company_type AS ct,
// keyword AS k,
// link_type AS lt,
// movie_companies AS mc,
// movie_keyword AS mk,
// movie_link AS ml,
// title AS t
// WHERE cn.country_code !='[pl]' and (cn.name like '20th Century Fox%' or cn.name like 'Twentieth Century Fox%') AND ct.kind  != 'production companies' and ct.kind is not NULL AND k.keyword  in ('sequel', 'revenge', 'based-on-novel') AND mc.note  is not NULL AND t.production_year  > 1950 AND lt.id = ml.link_type_id AND ml.movie_id = t.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mc.movie_id AND mc.company_type_id = ct.id AND mc.company_id = cn.id AND ml.movie_id = mk.movie_id AND ml.movie_id = mc.movie_id AND mk.movie_id = mc.movie_id;
#[cfg(test)]
mod test_11c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q11c() -> Result<(), PolarsError> {
        let db = ImdbData::new();

        assert_eq!(
            q11c(&db)?,
            Some((
                "20th Century Fox Home Entertainment",
                "(1997-2002) (worldwide) (all media)",
                "24"
            ))
        );
        Ok(())
    }
}
