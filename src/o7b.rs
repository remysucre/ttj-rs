use crate::data::Data;
use ahash::HashMap;
use ahash::HashSet;
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q7b(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
    let an = &db.an;
    let ci = &db.ci;
    let it = &db.it;
    let lt = &db.lt;
    let ml = &db.ml;
    let n = &db.n;
    let pi = &db.pi;
    let t = &db.t;

    let d = Finder::new("D");
    let a = Finder::new("a");

    let start = Instant::now();

    let lt_id = lt
        .id
        .iter()
        .zip(lt.link.iter())
        .find(|(_, link)| *link == "features")
        .map(|(id, _)| id)
        .unwrap();

    let it_id = it
        .id
        .iter()
        .zip(it.info.iter())
        .find(|(_, info)| *info == "mini biography")
        .map(|(id, _)| id)
        .unwrap();

    let ml_s: HashSet<i32> = ml
        .linked_movie_id
        .iter()
        .zip(ml.link_type_id.iter())
        .filter_map(|(id, link_type_id)| (lt_id == link_type_id).then_some(*id))
        .collect();

    let pi_s: HashSet<i32> = pi
        .person_id
        .iter()
        .zip(pi.info_type_id.iter())
        .zip(pi.note.iter())
        .filter_map(|((person_id, info_type_id), note)| {
            note.as_ref()
                .filter(|note| it_id == info_type_id && *note == "Volker Boehm")
                .map(|_| *person_id)
        })
        .collect();

    let t_m: HashMap<i32, &str> =
        t.id.iter()
            .zip(t.production_year.iter())
            .zip(t.title.iter())
            .filter_map(|((id, production_year), title)| {
                production_year
                    .filter(|production_year| {
                        ml_s.contains(&id) && (1980..=1984).contains(production_year)
                    })
                    .map(|_| (*id, title.as_str()))
            })
            .collect();

    let an_s: HashSet<i32> = an
        .person_id
        .iter()
        .zip(an.name.iter())
        .filter_map(|(id, name)| a.find(name.as_bytes()).is_some().then_some(*id))
        .collect();

    let n_m: HashMap<i32, &str> =
        n.id.iter()
            .zip(n.name.iter())
            .zip(n.name_pcode_cf.iter())
            .zip(n.gender.iter())
            .filter_map(|(((id, name), name_pcode), gender)| {
                if let Some(name_pcode) = name_pcode
                    && let Some(gender) = gender
                    && pi_s.contains(&id)
                    && d.find(name_pcode.as_bytes()) == Some(0)
                    && gender == "m"
                    && an_s.contains(&id)
                {
                    Some((*id, name.as_str()))
                } else {
                    None
                }
            })
            .collect();

    let mut res: Option<(&str, &str)> = None;

    for (pid, mid) in ci.person_id.iter().zip(ci.movie_id.iter()) {
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
mod test_q7b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q7b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q7b(&data)?;
        assert_eq!(res, Some(("De Palma, Brian", "Dressed to Kill")));
        Ok(())
    }
}
