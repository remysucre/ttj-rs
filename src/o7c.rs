use ahash::HashMap;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;
use crate::data::ImdbData;

pub fn q7c(db: &ImdbData) -> Result<(), PolarsError> {
    let an = &db.an;
    let ci = &db.ci;
    let it = &db.it;
    let lt = &db.lt;
    let ml = &db.ml;
    let n = &db.n;
    let pi = &db.pi;
    let t = &db.t;

    let start = Instant::now();

    let lt_s: Vec<i32> = lt
        .column("id")?
        .i32()?
        .into_iter()
        .zip(lt.column("link")?.str()?)
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

    let it_s: Vec<i32> = it
        .column("id")?
        .i32()?
        .into_iter()
        .zip(it.column("info")?.str()?)
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
        .zip(ml.column("link_type_id")?.i32()?)
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

    let mut pi_m: HashMap<i32, Vec<&str>> = HashMap::default();

    let pi_id_col = pi.column("person_id")?.i32()?;
    let pi_note_col = pi.column("note")?.str()?;
    let pi_info_type_id_col = pi.column("info_type_id")?.i32()?;
    let pi_info_col = pi.column("info")?.str()?;

    for (((id, note), info_type_id), info) in pi_id_col
        .into_iter()
        .zip(pi_note_col.into_iter())
        .zip(pi_info_type_id_col.into_iter())
        .zip(pi_info_col.into_iter())
    {
        if let (Some(id), Some(_note), Some(info_type_id), Some(info)) =
            (id, note, info_type_id, info)
        {
            if it_s.contains(&info_type_id) {
                pi_m.entry(id).or_default().push(info);
            }
        }
    }

    let mut t_s: HashSet<i32> = HashSet::default();

    let t_id_col = t.column("id")?.i32()?;
    let t_year_col = t.column("production_year")?.i32()?;

    for (id, production_year) in t_id_col.into_iter().zip(t_year_col.into_iter()) {
        if let (Some(id), Some(production_year)) = (id, production_year) {
            if (1980..=2010).contains(&production_year) && ml_s.contains(&id) {
                t_s.insert(id);
            }
        }
    }

    let mut n_m: HashMap<i32, Vec<&str>> = HashMap::default();

    let id_col = n.column("id")?.i32()?;
    let name_col = n.column("name")?.str()?;
    let name_pcode_col = n.column("name_pcode_cf")?.str()?;
    let gender_col = n.column("gender")?.str()?;

    for ((id, name), (name_pcode, gender)) in id_col
        .into_iter()
        .zip(name_col.into_iter())
        .zip(name_pcode_col.into_iter().zip(gender_col.into_iter()))
    {
        if let (Some(id), Some(name), Some(name_pcode), Some(gender)) =
            (id, name, name_pcode, gender)
        {
            if pi_m.contains_key(&id)
                && (("A"..="F").contains(&name_pcode) && gender == "m"
                    || gender == "f" && name.starts_with('A'))
            {
                n_m.entry(id).or_default().push(name);
            }
        }
    }

    let mut an_s: HashSet<i32> = HashSet::default();

    let an_id_col = an.column("person_id")?.i32()?;
    let an_name_col = an.column("name")?.str()?;

    for (id, name) in an_id_col.into_iter().zip(an_name_col.into_iter()) {
        if let (Some(id), Some(name)) = (id, name) {
            if n_m.contains_key(&id) && (name.contains('a') || name.starts_with('A')) {
                an_s.insert(id);
            }
        }
    }

    let mut res: Option<(&str, &str)> = None;

    for (pid, mid) in ci
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("movie_id")?.i32()?.into_iter())
    {
        if let (Some(pid), Some(mid)) = (pid, mid) {
                if let Some(info) = pi_m.get(&pid) {
                        if let Some(name) = n_m.get(&pid) {
                            if t_s.contains(&mid) && an_s.contains(&pid)
                            {
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

    // println!("{:}", res);

    let duration = start.elapsed().as_secs_f32();
    println!("{duration:}");

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
