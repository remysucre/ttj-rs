use crate::data::ImdbData;
use ahash::HashMap;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q7a(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    // let an = &db.an;
    let ci = &db.ci;
    let it = &db.it;
    let lt = &db.lt;
    let ml = &db.ml;
    let n = &db.n;
    let pi = &db.pi;
    let t = &db.t;

    let start = Instant::now();

    let lt_s: HashSet<i32> = lt
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(lt.column("link")?.str()?.into_no_null_iter())
        .filter_map(|(id, link)| if link == "features" { Some(id) } else { None })
        .collect();

    let it_s: HashSet<i32> = it
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(it.column("info")?.str()?.into_no_null_iter())
        .filter_map(|(id, info)| {
            if info == "mini biography" {
                Some(id)
            } else {
                None
            }
        })
        .collect();

    let ml_s: HashSet<i32> = ml
        .column("linked_movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(ml.column("link_type_id")?.i32()?.into_no_null_iter())
        .filter_map(|(id, link_type_id)| {
            if lt_s.contains(&link_type_id) {
                Some(id)
            } else {
                None
            }
        })
        .collect();

    let pi_s: HashSet<i32> = pi
        .column("person_id")?
        .i32()?
        .into_no_null_iter()
        .zip(pi.column("info_type_id")?.i32()?.into_no_null_iter())
        .zip(pi.column("note")?.str()?)
        .filter_map(
            |((person_id, info_type_id), note)| match (person_id, info_type_id, note) {
                (person_id, info_type_id, Some(note)) => {
                    if note == "Volker Boehm" && it_s.contains(&info_type_id) {
                        Some(person_id)
                    } else {
                        None
                    }
                }
                _ => None,
            },
        )
        .collect();

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(t.column("production_year")?.i32()?)
        .zip(t.column("title")?.str()?.into_no_null_iter())
        .filter_map(|((id, production_year), title)| {
            if let (id, Some(production_year), title) = (id, production_year, title) {
                if (1980..=1995).contains(&production_year) && ml_s.contains(&id) {
                    Some((id, title))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let n_m: HashMap<i32, &str> = n
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(n.column("name")?.str()?.into_no_null_iter())
        .zip(n.column("name_pcode_cf")?.str()?)
        .zip(n.column("gender")?.str()?)
        .filter_map(|(((id, name), name_pcode), gender)| {
            if let (id, name, Some(name_pcode), Some(gender)) = (id, name, name_pcode, gender) {
                if (("A"..="F").contains(&name_pcode) && gender == "m"
                    || gender == "f" && name.starts_with('B'))
                    && pi_s.contains(&id)
                {
                    Some((id, name))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

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

    let res: Option<(&str, &str)> = ci
        .column("person_id")?
        .i32()?
        .into_no_null_iter()
        .zip(ci.column("movie_id")?.i32()?.into_no_null_iter())
        .filter_map(|(person_id, movie_id)| match (person_id, movie_id) {
            (p_id, m_id) => {
                if let Some(&name) = n_m.get(&p_id)
                    && let Some(&title) = t_m.get(&m_id)
                {
                    Some((name, title))
                } else {
                    None
                }
            }
        })
        .fold(None, |acc: Option<(&str, &str)>, (name, title)| match acc {
            None => Some((name, title)),
            Some((min_name, min_title)) => Some((
                if name < min_name { name } else { min_name },
                if title < min_title { title } else { min_title },
            )),
        });

    println!("7a,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 7a
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
// AND n.name_pcode_cf BETWEEN 'A' AND 'F'
// AND (n.gender = 'm' OR (n.gender = 'f' AND n.name LIKE 'B%'))
// AND pi.note = 'Volker Boehm'
// AND t.production_year BETWEEN 1980 AND 1995
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
mod test_7a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q7a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q7a(&db)?;
        assert_eq!(res, Some(("Antonioni, Michelangelo", "Dressed to Kill")));
        Ok(())
    }
}
