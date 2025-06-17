use ahash::HashMap;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q8c() -> Result<(), PolarsError> {
    let t = LazyFrame::scan_parquet("imdb/title.parquet", Default::default())?.collect()?;
    let an = LazyFrame::scan_parquet("imdb/aka_name.parquet", Default::default())?.collect()?;
    let n = LazyFrame::scan_parquet("imdb/name.parquet", Default::default())?.collect()?;
    let rt = LazyFrame::scan_parquet("imdb/role_type.parquet", Default::default())?.collect()?;
    let ci = LazyFrame::scan_parquet("imdb/cast_info.parquet", Default::default())?.collect()?;
    let mc =
        LazyFrame::scan_parquet("imdb/movie_companies.parquet", Default::default())?.collect()?;
    let cn = LazyFrame::scan_parquet("imdb/company_name.parquet", Default::default())?.collect()?;

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

    // collect us companies into a hash set
    let cn_s = cn
        .lazy()
        .filter(col("country_code").eq(lit("[us]")))
        .select([col("id")])
        .collect()?
        .column("id")?
        .i32()?
        .into_iter()
        .flatten()
        .collect::<HashSet<i32>>();

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

    println!("{:?}", res);

    let duration = start.elapsed();
    dbg!(duration);

    Ok(())
}

// 1a.sql
// SELECT MIN(mc.note) AS production_note,
//        MIN(t.title) AS movie_title,
//        MIN(t.production_year) AS movie_year
// FROM company_type AS ct,
//      info_type AS it,
//      movie_companies AS mc,
//      movie_info_idx AS mi_idx,
//      title AS t
// WHERE ct.kind = 'production companies'
//   AND it.info = 'top 250 rank'
//   AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
//   AND (mc.note LIKE '%(co-production)%'
//        OR mc.note LIKE '%(presents)%')
//   AND ct.id = mc.company_type_id
//   AND t.id = mc.movie_id
//   AND t.id = mi_idx.movie_id
//   AND mc.movie_id = mi_idx.movie_id
//   AND it.id = mi_idx.info_type_id;
