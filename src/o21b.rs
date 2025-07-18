use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q21b(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cn = &db.cn;
    let ct = &db.ct;
    let k = &db.k;
    let lt = &db.lt;
    let mc = &db.mc;
    let mi = &db.mi;
    let mk = &db.mk;
    let ml = &db.ml;
    let t = &db.t;

    let start = Instant::now();

    let mut cn_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, country_code), name) in cn
        .column("id")?
        .i32()?
        .into_iter()
        .zip(cn.column("country_code")?.str()?.into_iter())
        .zip(cn.column("name")?.str()?.into_iter())
    {
        if let (Some(id), Some(country_code), Some(name)) = (id, country_code, name) {
            if country_code != "[pl]" && (name.contains("Film") || name.contains("Warner")) {
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
        .zip(k.column("keyword")?.str()?.into_iter())
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
        .zip(mk.column("keyword_id")?.i32()?.into_iter())
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

    let mut lt_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (id, link) in lt
        .column("id")?
        .i32()?
        .into_iter()
        .zip(lt.column("link")?.str()?.into_iter())
    {
        if let (Some(id), Some(link)) = (id, link) {
            if link.contains("follow") {
                lt_m.entry(id).or_default().push(link);
            }
        }
    }

    let mut ml_m: HashMap<i32, Vec<i32>> = HashMap::default();

    for (movie_id, link_type_id) in ml
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ml.column("link_type_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(link_type_id)) = (movie_id, link_type_id) {
            ml_m.entry(movie_id).or_default().push(link_type_id);
        }
    }

    let mi_s: HashSet<i32> = mi
        .column("info")?
        .str()?
        .into_iter()
        .zip(mi.column("movie_id")?.i32()?.into_iter())
        .filter_map(|(info, movie_id)| {
            if let (Some(info), Some(movie_id)) = (info, movie_id) {
                if matches!(info, "German" | "Germany") {
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

    for ((id, production_year), title) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("production_year")?.i32()?.into_iter())
        .zip(t.column("title")?.str()?.into_iter())
    {
        if let (Some(id), Some(production_year), Some(title)) = (id, production_year, title) {
            if mk_s.contains(&id) && (2000..=2010).contains(&production_year) {
                t_m.entry(id).or_default().push(title);
            }
        }
    }

    let mut res: Option<(&str, &str, &str)> = None;

    for (((movie_id, company_id), company_type_id), note) in mc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_id")?.i32()?.into_iter())
        .zip(mc.column("company_type_id")?.i32()?.into_iter())
        .zip(mc.column("note")?.str()?.into_iter())
    {
        if let (Some(movie_id), Some(company_id), Some(company_type_id), None) =
            (movie_id, company_id, company_type_id, note)
        {
            if ct_s.contains(&company_type_id) && mi_s.contains(&movie_id) {
                if let Some(lt_ids) = ml_m.get(&movie_id) {
                    for lt_id in lt_ids {
                        if let (Some(links), Some(titles), Some(names)) =
                            (lt_m.get(lt_id), t_m.get(&movie_id), cn_m.get(&company_id))
                        {
                            for link in links {
                                for title in titles {
                                    for name in names {
                                        if let Some((old_name, old_link, old_title)) = res.as_mut()
                                        {
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
                }
            }
        }
    }

    println!("{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 21b
// SELECT MIN(cn.name) AS company_name, MIN(lt.link) AS link_type, MIN(t.title) AS german_follow_up
// FROM company_name AS cn,
// company_type AS ct,
// keyword AS k,
// link_type AS lt,
// movie_companies AS mc,
// movie_info AS mi,
// movie_keyword AS mk,
// movie_link AS ml,
// title AS t
// WHERE cn.country_code !='[pl]' AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%') AND ct.kind ='production companies' AND k.keyword ='sequel' AND lt.link LIKE '%follow%' AND mc.note IS NULL AND mi.info IN ('Germany', 'German') AND t.production_year BETWEEN 2000 AND 2010 AND lt.id = ml.link_type_id AND ml.movie_id = t.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mc.movie_id AND mc.company_type_id = ct.id AND mc.company_id = cn.id AND mi.movie_id = t.id AND ml.movie_id = mk.movie_id AND ml.movie_id = mc.movie_id AND mk.movie_id = mc.movie_id AND ml.movie_id = mi.movie_id AND mk.movie_id = mi.movie_id AND mc.movie_id = mi.movie_id;
#[cfg(test)]
mod test_21b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q21b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q21b(&db)?;
        assert_eq!(
            res,
            Some(("Filmlance International AB", "followed by", "Hämndens pris"))
        );
        Ok(())
    }
}
