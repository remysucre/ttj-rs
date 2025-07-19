use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q22b(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cn = &db.cn;
    let ct = &db.ct;
    let it1 = &db.it;
    let it2 = &db.it;
    let k = &db.k;
    let kt = &db.kt;
    let mc = &db.mc;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let mk = &db.mk;
    let t = &db.t;

    let ct_s: HashSet<i32> = ct.column("id")?.i32()?.into_iter().flatten().collect();

    let start = Instant::now();

    let cn_m: HashMap<i32, Vec<&str>> = cn
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cn.column("country_code")?.str()?)
        .zip(cn.column("name")?.str()?)
        .filter_map(|((id, country_code), name)| {
            if let (Some(id), Some(country_code), Some(name)) = (id, country_code, name) {
                if country_code != "[us]" {
                    Some((id, name))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (id, name)| {
            acc.entry(id).or_default().push(name);
            acc
        });

    let it1_s: HashSet<i32> = it1
        .column("info")?
        .str()?
        .into_iter()
        .zip(it1.column("id")?.i32()?)
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "countries" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let it2_s: HashSet<i32> = it2
        .column("info")?
        .str()?
        .into_iter()
        .zip(it2.column("id")?.i32()?)
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "rating" { Some(id) } else { None }
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
                if matches!(keyword, "murder" | "murder-in-title" | "blood" | "violence") {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let kt_s: HashSet<i32> = kt
        .column("kind")?
        .str()?
        .into_iter()
        .zip(kt.column("id")?.i32()?)
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if matches!(kind, "movie" | "episode") {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mi_s: HashSet<i32> = mi
        .column("info")?
        .str()?
        .into_iter()
        .zip(mi.column("movie_id")?.i32()?)
        .zip(mi.column("info_type_id")?.i32()?)
        .filter_map(|((info, movie_id), info_type_id)| {
            if let (Some(info), Some(movie_id), Some(info_type_id)) = (info, movie_id, info_type_id)
            {
                if matches!(info, "Germany" | "German" | "USA" | "American")
                    && it1_s.contains(&info_type_id)
                {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let t_m: HashMap<i32, Vec<&str>> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?)
        .zip(t.column("production_year")?.i32()?)
        .zip(t.column("kind_id")?.i32()?)
        .filter_map(|(((id, title), production_year), kind_id)| {
            if let (Some(id), Some(title), Some(production_year), Some(kind_id)) =
                (id, title, production_year, kind_id)
            {
                if production_year > 2009 && kt_s.contains(&kind_id) {
                    Some((id, title))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (id, title)| {
            acc.entry(id).or_default().push(title);
            acc
        });

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

    let mi_idx_m: HashMap<i32, Vec<&str>> = mi_idx
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi_idx.column("info")?.str()?)
        .zip(mi_idx.column("info_type_id")?.i32()?)
        .filter_map(|((movie_id, info), info_type_id)| {
            if let (Some(movie_id), Some(info), Some(info_type_id)) = (movie_id, info, info_type_id)
            {
                if info < "7.0" && it2_s.contains(&info_type_id) {
                    Some((movie_id, info))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (movie_id, info)| {
            acc.entry(movie_id).or_default().push(info);
            acc
        });

    let mut res: Option<(&str, &str, &str)> = None;

    for (((movie_id, company_id), company_type_id), note) in mc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_id")?.i32()?.into_iter())
        .zip(mc.column("company_type_id")?.i32()?.into_iter())
        .zip(mc.column("note")?.str()?.into_iter())
    {
        if let (Some(movie_id), Some(company_id), Some(company_type_id), Some(note)) =
            (movie_id, company_id, company_type_id, note)
        {
            if ct_s.contains(&company_type_id)
                && mi_s.contains(&movie_id)
                && mk_s.contains(&movie_id)
                && note.contains("(200")
                && !note.contains("(USA)")
            {
                if let (Some(titles), Some(infos), Some(names)) = (
                    t_m.get(&movie_id),
                    mi_idx_m.get(&movie_id),
                    cn_m.get(&company_id),
                ) {
                    for title in titles {
                        for info in infos {
                            for name in names {
                                if let Some((old_name, old_info, old_title)) = res.as_mut() {
                                    if *old_title > *title {
                                        *old_title = title;
                                    }
                                    if *old_info > *info {
                                        *old_info = info;
                                    }
                                    if *old_name > *name {
                                        *old_name = name;
                                    }
                                } else {
                                    res = Some((name, info, title));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    println!("22b,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 22b
// SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS western_violent_movie
// FROM company_name AS cn,
// company_type AS ct,
// info_type AS it1,
// info_type AS it2,
// keyword AS k,
// kind_type AS kt,
// movie_companies AS mc,
// movie_info AS mi,
// movie_info_idx AS mi_idx,
// movie_keyword AS mk,
// title AS t
// WHERE cn.country_code != '[us]' AND it1.info  = 'countries' AND it2.info  = 'rating' AND k.keyword  in ('murder', 'murder-in-title', 'blood', 'violence') AND kt.kind  in ('movie', 'episode') AND mc.note  not like '%(USA)%' and mc.note like '%(200%)%' AND mi.info IN ('Germany', 'German', 'USA', 'American') AND mi_idx.info  < '7.0' AND t.production_year  > 2009 AND kt.id = t.kind_id AND t.id = mi.movie_id AND t.id = mk.movie_id AND t.id = mi_idx.movie_id AND t.id = mc.movie_id AND mk.movie_id = mi.movie_id AND mk.movie_id = mi_idx.movie_id AND mk.movie_id = mc.movie_id AND mi.movie_id = mi_idx.movie_id AND mi.movie_id = mc.movie_id AND mc.movie_id = mi_idx.movie_id AND k.id = mk.keyword_id AND it1.id = mi.info_type_id AND it2.id = mi_idx.info_type_id AND ct.id = mc.company_type_id AND cn.id = mc.company_id;
#[cfg(test)]
mod test_22b {
    use crate::data::ImdbData;
    use crate::o22b::q22b;
    use polars::error::PolarsError;

    #[test]
    fn test_q22b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q22b(&db)?;
        assert_eq!(
            res,
            Some(("Boll Kino Beteiligungs GmbH & Co. KG", "3.0", "A Small Act"))
        );
        Ok(())
    }
}
