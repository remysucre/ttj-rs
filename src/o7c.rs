use ahash::HashMap;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q7c() -> Result<(), PolarsError> {
    let an = LazyFrame::scan_parquet("imdb/aka_name.parquet", Default::default())?.collect()?;
    let ci = LazyFrame::scan_parquet("imdb/cast_info.parquet", Default::default())?.collect()?;
    let it = LazyFrame::scan_parquet("imdb/info_type.parquet", Default::default())?.collect()?;
    let lt = LazyFrame::scan_parquet("imdb/link_type.parquet", Default::default())?.collect()?;
    let ml = LazyFrame::scan_parquet("imdb/movie_link.parquet", Default::default())?.collect()?;
    let n = LazyFrame::scan_parquet("imdb/name.parquet", Default::default())?.collect()?;
    let pi = LazyFrame::scan_parquet("imdb/person_info.parquet", Default::default())?.collect()?;
    let t = LazyFrame::scan_parquet("imdb/title.parquet", Default::default())?.collect()?;

    let start = Instant::now();

    let lt_s: HashSet<i32> = lt
        .column("id")?
        .i32()?
        .into_iter()
        .zip(lt.column("link")?.str()?.into_iter())
        .filter_map(|(id, link)| {
            if let (Some(id), Some(link)) = (id, link) {
                if matches!(
                    link,
                    "references" | "referenced in" | "features" | "featured in"
                ) {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let it_s: HashSet<i32> = it
        .column("id")?
        .i32()?
        .into_iter()
        .zip(it.column("info")?.str()?.into_iter())
        .filter_map(|(id, info)| {
            if let (Some(id), Some(info)) = (id, info) {
                if info == "mini biography" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let ml_s: HashSet<i32> = ml
        .column("linked_movie_id")?
        .i32()?
        .into_iter()
        .zip(ml.column("link_type_id")?.i32()?.into_iter())
        .filter_map(|(id, link_type_id)| {
            if let (Some(id), Some(link_type_id)) = (id, link_type_id) {
                if lt_s.contains(&link_type_id) {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let pi_m: HashMap<i32, Vec<&str>> = pi
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(pi.column("note")?.str()?.into_iter())
        .zip(pi.column("info_type_id")?.i32()?.into_iter())
        .zip(pi.column("info")?.str()?.into_iter())
        .fold(
            HashMap::default(),
            |mut acc, (((id, note), info_type_id), info)| {
                if let (Some(id), Some(note), Some(info_type_id), Some(info)) =
                    (id, note, info_type_id, info)
                {
                    if it_s.contains(&info_type_id) {
                        acc.entry(id).or_default().push(info);
                    }
                }
                acc
            },
        );

    let t_s: HashSet<i32> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("production_year")?.i32()?.into_iter())
        .filter_map(|(id, production_year)| {
            if let (Some(id), Some(production_year)) = (id, production_year) {
                if production_year >= 1980 && production_year <= 2010 && ml_s.contains(&id) {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let n_m: HashMap<i32, Vec<&str>> = n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?.into_iter())
        .zip(n.column("name_pcode_cf")?.str()?.into_iter())
        .zip(n.column("gender")?.str()?.into_iter())
        .fold(
            HashMap::default(),
            |mut acc, (((id, name), name_pcode), gender)| {
                if let (Some(id), Some(name), Some(name_pcode), Some(gender)) =
                    (id, name, name_pcode, gender)
                {
                    if pi_m.contains_key(&id) && ((name_pcode >= "A" && name_pcode <= "F") && gender == "m" || gender == "f" && name.starts_with('A'))
                    {
                        acc.entry(id).or_default().push(name);
                    }
                }
                acc
            },
        );

    let an_s: HashSet<i32> = an
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(an.column("name")?.str()?.into_iter())
        .filter_map(|(id, name)| {
            if let (Some(id), Some(name)) = (id, name) {
                if n_m.contains_key(&id) && (name.contains('a') || name.starts_with('A')){
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    // let mut all_res = HashSet::default();

    let mut res: Option<(&str, &str)> = None;

    for (pid, mid) in ci
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("movie_id")?.i32()?.into_iter())
    {
        if let (Some(pid), Some(mid)) = (pid, mid) {
            if an_s.contains(&pid) && t_s.contains(&mid) {
                if let Some(name) = n_m.get(&pid) {
                    if let Some(info) = pi_m.get(&pid) {
                        for name in name {
                            for info in info {
                                if let Some((old_name, old_info)) = res.as_mut() {
                                    if name < old_name {
                                        *old_name = name;
                                    }
                                    if info < old_info {
                                        *old_info = info;
                                    }
                                } else {
                                    res = Some((name, info));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    dbg!(res);

    let duration = start.elapsed();
    dbg!(duration);

    Ok(())
}

// -- JOB Query 7c
// SELECT MIN(n.name) AS cast_member_name, MIN(pi.info) AS cast_member_info
// FROM aka_name AS an,
// cast_info AS ci,
// info_type AS it,
// link_type AS lt,
// movie_link AS ml,
// name AS n,
// person_info AS pi,
// title AS t
// WHERE an.name is not NULL
// and (an.name LIKE '%a%' or an.name LIKE 'A%')
// AND it.info = 'mini biography'
// AND lt.link in ('references', 'referenced in', 'features', 'featured in')
// AND n.name_pcode_cf BETWEEN 'A' AND 'F'
// AND (n.gender = 'm' OR (n.gender = 'f' AND n.name LIKE 'A%'))
// AND pi.note is not NULL
// AND t.production_year BETWEEN 1980 AND 2010
// AND n.id = an.person_id
// AND n.id = pi.person_id
// AND ci.person_id = n.id
// AND t.id = ci.movie_id
// AND ml.linked_movie_id = t.id
// AND lt.id = ml.link_type_id
// AND it.id = pi.info_type_id
// AND pi.person_id = an.person_id
// AND pi.person_id = ci.person_id
// AND an.person_id = ci.person_id
// AND ci.movie_id = ml.linked_movie_id;
