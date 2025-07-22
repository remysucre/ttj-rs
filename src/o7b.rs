use crate::data::ImdbData;
use ahash::HashMap;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q7b(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    // let an = &db.an;
    let ci = &db.ci;
    let it = &db.it;
    let lt = &db.lt;
    let ml = &db.ml;
    let n = &db.n;
    let pi = &db.pi;
    let t = &db.t;

    let start = Instant::now();

    let lt_id = lt
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(lt.column("link")?.str()?.into_no_null_iter())
        .find(|(_, link)| *link == "features")
        .map(|(id, _)| id)
        .unwrap();

    let it_id = it
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(it.column("info")?.str()?.into_no_null_iter())
        .find(|(_, info)| *info == "mini biography")
        .map(|(id, _)| id)
        .unwrap();

    let ml_s: HashSet<i32> = ml
        .column("linked_movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(ml.column("link_type_id")?.i32()?.into_no_null_iter())
        .filter_map(|(id, link_type_id)| (lt_id == link_type_id).then_some(id))
        .collect();

    let pi_s: HashSet<i32> = pi
        .column("person_id")?
        .i32()?
        .into_no_null_iter()
        .zip(pi.column("info_type_id")?.i32()?.into_no_null_iter())
        .zip(pi.column("note")?.str()?)
        .filter_map(|((person_id, info_type_id), note)| {
            (it_id == info_type_id && note? == "Volker Boehm").then_some(person_id)
        })
        .collect();

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(t.column("production_year")?.i32()?)
        .zip(t.column("title")?.str()?.into_no_null_iter())
        .filter_map(|((id, production_year), title)| {
            (ml_s.contains(&id) && (1980..=1984).contains(&production_year?)).then_some((id, title))
        })
        .fold(HashMap::default(), |mut acc, (id, title)| {
            acc.entry(id)
                .and_modify(|existing| {
                    if title < *existing {
                        *existing = title;
                    }
                })
                .or_insert(title);
            acc
        });

    let n_m: HashMap<i32, &str> = n
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(n.column("name")?.str()?.into_no_null_iter())
        .zip(n.column("name_pcode_cf")?.str()?)
        .zip(n.column("gender")?.str()?)
        .filter_map(|(((id, name), name_pcode), gender)| {
            (pi_s.contains(&id) && name_pcode?.starts_with('D') && gender? == "m")
                .then_some((id, name))
        })
        .fold(HashMap::default(), |mut acc, (id, name)| {
            acc.entry(id)
                .and_modify(|existing| {
                    if name < *existing {
                        *existing = name;
                    }
                })
                .or_insert(name);
            acc
        });

    // let an_s: HashSet<i32> = an
    //     .column("person_id")?
    //     .i32()?
    //     .into_iter()
    //     .zip(an.column("name")?.str()?)
    //     .filter_map(|(id, name)| {
    //         if let (Some(id), Some(name)) = (id, name) {
    //             if n_m.contains_key(&id) && name.contains('a') {
    //                 Some(id)
    //             } else {
    //                 None
    //             }
    //         } else {
    //             None
    //         }
    //     })
    //     .collect();

    let mut res: Option<(&str, &str)> = None;

    for (pid, mid) in ci
        .column("person_id")?
        .i32()?
        .into_no_null_iter()
        .zip(ci.column("movie_id")?.i32()?.into_no_null_iter())
    {
        if let Some(name) = n_m.get(&pid)
            && let Some(title) = t_m.get(&mid)
        {
            res = match res {
                Some((old_name, old_title)) => Some((name.min(&old_name), title.min(&old_title))),
                None => Some((name, title)),
            };
        }
    }

    println!("7b,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 7b
// SELECT MIN(n.name) AS of_person, MIN(t.title) AS biography_movie
// FROM aka_name AS an,
// cast_info AS ci,
// info_type AS it,
// link_type AS lt,
// movie_link AS ml,
// name AS n,
// person_info AS pi,
// title AS t
// WHERE an.name LIKE '%a%'
// AND it.info = 'mini biography'
// AND lt.link = 'features'
// AND n.name_pcode_cf LIKE 'D%'
// AND n.gender = 'm'
// AND pi.note = 'Volker Boehm'
// AND t.production_year BETWEEN 1980 AND 1984
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
#[cfg(test)]
mod test_7b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q7b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q7b(&db)?;
        assert_eq!(res, Some(("De Palma, Brian", "Dressed to Kill")));
        Ok(())
    }
}
