use crate::data::ImdbData;
use ahash::HashMap;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q8c(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    let t = &db.t;
    let an = &db.an;
    let n = &db.n;
    let rt = &db.rt;
    let ci = &db.ci;
    let mc = &db.mc;
    let cn = &db.cn;

    let n_s: HashSet<i32> = n.column("id")?.i32()?.into_iter().flatten().collect();

    let t_m: HashMap<i32, Vec<&str>> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .filter_map(|(id, title)| {
            if let (Some(id), Some(title)) = (id, title) {
                Some((id, title))
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (id, title)| {
            acc.entry(id).or_default().push(title);
            acc
        });

    let mut ci_m: HashMap<i32, Vec<(i32, i32)>> = HashMap::default();

    for ((rid, mid), pid) in ci
        .column("role_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("movie_id")?.i32()?.into_iter())
        .zip(ci.column("person_id")?.i32()?.into_iter())
    {
        if let (Some(rid), Some(mid), Some(pid)) = (rid, mid, pid) {
            ci_m.entry(rid).or_default().push((mid, pid));
        }
    }

    let mut an_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (id, name) in an
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(an.column("name")?.str()?.into_iter())
    {
        if let (Some(id), Some(name)) = (id, name) {
            an_m.entry(id).or_default().push(name);
        }
    }

    let mut mc_m: HashMap<i32, Vec<i32>> = HashMap::default();

    for (mid, cid) in mc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_id")?.i32()?.into_iter())
    {
        if let (Some(mid), Some(cid)) = (mid, cid) {
            mc_m.entry(mid).or_default().push(cid);
        }
    }

    let start = Instant::now();

    let cn_s: HashSet<i32> = cn
        .column("country_code")?
        .str()?
        .into_iter()
        .zip(cn.column("id")?.i32()?.into_iter())
        .filter_map(|(country_code, id)| {
            if let (Some(country_code), Some(id)) = (country_code, id) {
                if country_code == "[us]" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut res: Option<(&str, &str)> = None;

    for (id, role) in rt
        .column("id")?
        .i32()?
        .into_iter()
        .zip(rt.column("role")?.str()?.into_iter())
    {
        if let (Some(id), Some(role)) = (id, role) {
            if role == "writer" {
                let mut delete_cim = false;
                if let Some(movie_persons) = ci_m.get_mut(&id) {
                    let mut i = 0;
                    'ci: while i < movie_persons.len() {
                        let (mid, pid) = movie_persons[i];
                        let mut delete_mc = false;
                        if let Some(cids) = mc_m.get_mut(&mid) {
                            let mut j = 0;
                            'mc: while j < cids.len() {
                                let cid = cids[j];
                                if cn_s.contains(&cid) {
                                    if n_s.contains(&pid) {
                                        if let Some(names) = an_m.get(&pid) {
                                            if let Some(titles) = t_m.get(&mid) {
                                                for name in names {
                                                    for title in titles {
                                                        if let Some((old_name, old_title)) =
                                                            res.as_mut()
                                                        {
                                                            if name < old_name {
                                                                *old_name = name;
                                                            }
                                                            if title < old_title {
                                                                *old_title = title;
                                                            }
                                                        } else {
                                                            res = Some((name, title));
                                                        }
                                                    }
                                                }
                                            } else {
                                                movie_persons.swap_remove(i);
                                                continue 'ci;
                                            }
                                        } else {
                                            movie_persons.swap_remove(i);
                                            continue 'ci;
                                        }
                                    } else {
                                        movie_persons.swap_remove(i);
                                        continue 'ci;
                                    }
                                } else {
                                    cids.swap_remove(j);
                                    continue 'mc;
                                }
                                j += 1;
                            }
                            if cids.is_empty() {
                                delete_mc = true;
                            }
                        } else {
                            movie_persons.swap_remove(i);
                            continue 'ci;
                        }
                        if delete_mc {
                            mc_m.remove(&mid);
                        }
                        i += 1;
                    }
                    if movie_persons.is_empty() {
                        delete_cim = true;
                    }
                }
                if delete_cim {
                    ci_m.remove(&id);
                }
            }
        }
    }

    dbg!(start.elapsed());

    Ok(res)
}

// -- JOB Query 8d
// SELECT MIN(an1.name) AS costume_designer_pseudo, MIN(t.title) AS movie_with_costumes
// FROM aka_name AS an1,
// cast_info AS ci,
// company_name AS cn,
// movie_companies AS mc,
// name AS n1,
// role_type AS rt,
// title AS t
// WHERE cn.country_code = '[us]'
// AND rt.role = 'costume designer'
// AND an1.person_id = n1.id
// AND n1.id = ci.person_id
// AND ci.movie_id = t.id
// AND t.id = mc.movie_id
// AND mc.company_id = cn.id
// AND ci.role_id = rt.id
// AND an1.person_id = ci.person_id
// AND ci.movie_id = mc.movie_id;
#[cfg(test)]
mod test_8c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q8b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q8c(&db)?;
        assert_eq!(
            res,
            Some(("\"Jenny from the Block\"", "#1 Cheerleader Camp"))
        );
        Ok(())
    }
}
