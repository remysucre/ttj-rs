use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q10c(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
    let chn = &db.chn;
    let ci = &db.ci;
    let cn = &db.cn;
    // let ct = &db.ct;
    let mc = &db.mc;
    // let rt = &db.rt;
    let t = &db.t;

    let producer = Finder::new("(producer)");

    let chn_m: HashMap<&i32, Vec<&str>> =
        chn.id
            .iter()
            .zip(chn.name.iter())
            .fold(HashMap::default(), |mut acc, (chn_id, name)| {
                acc.entry(chn_id).or_default().push(name);
                acc
            });

    // Due to ct and mc form a PK-FK join and there are no selection predicates on ct, we can drop
    // ct from the join.
    // let ct_s: Vec<i32> = ct.column("id")?.i32()?.into_iter().flatten().collect();

    let start = Instant::now();

    let cn_s: HashSet<&i32> = cn
        .country_code
        .iter()
        .zip(cn.id.iter())
        .filter_map(|(country_code, id)| {
            country_code
                .as_ref()
                .filter(|code| code.as_str() == "[us]")
                .map(|_| id)
        })
        .collect();

    let mc_s: HashSet<&i32> = mc
        .movie_id
        .iter()
        .zip(mc.company_id.iter())
        .filter_map(|(movie_id, company_id)| cn_s.contains(&company_id).then_some(movie_id))
        .collect();

    // PK (rt) - FK (ci)
    // let rt_s: HashSet<i32> = rt
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .filter_map(|id| id)
    //     .collect();

    let t_m: HashMap<&i32, Vec<&str>> =
        t.id.iter()
            .zip(t.production_year.iter())
            .zip(t.title.iter())
            .filter_map(|((movie_id, production_year), title)| {
                if let Some(production_year) = production_year
                    && mc_s.contains(&movie_id)
                    && *production_year > 1990
                {
                    Some((movie_id, title))
                } else {
                    None
                }
            })
            .fold(HashMap::default(), |mut acc, (movie_id, title)| {
                acc.entry(movie_id).or_default().push(title);
                acc
            });

    let mut res: Option<(&str, &str)> = None;

    for ((mid, person_role_id), note) in ci
        .movie_id
        .iter()
        .zip(ci.person_role_id.iter())
        .zip(ci.note.iter())
    {
        if let Some(person_role_id) = person_role_id
            && let Some(note) = note
            && producer.find(note.as_bytes()).is_some()
            && let Some(character_names) = chn_m.get(&person_role_id)
            && let Some(titles) = t_m.get(&mid)
        {
            res = match res {
                Some((old_character_name, old_title)) => Some((
                    character_names
                        .iter()
                        .min()
                        .unwrap()
                        .min(&old_character_name),
                    titles.iter().min().unwrap().min(&old_title),
                )),
                None => Some((
                    character_names.iter().min().unwrap(),
                    titles.iter().min().unwrap(),
                )),
            };
        }
    }

    println!("10c,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 10c
// SELECT MIN(chn.name) AS character, MIN(t.title) AS movie_with_american_producer
// FROM char_name AS chn, cast_info AS ci, company_name AS cn, company_type AS ct, movie_companies AS mc, role_type AS rt, title AS t
// WHERE ci.note like '%(producer)%'
// AND cn.country_code = '[us]'
// AND t.production_year > 1990
// AND t.id = mc.movie_id
// AND t.id = ci.movie_id
// AND ci.movie_id = mc.movie_id
// AND chn.id = ci.person_role_id
// AND rt.id = ci.role_id
// AND cn.id = mc.company_id
// AND ct.id = mc.company_type_id;
#[cfg(test)]
mod test_q10c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q10c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);

        assert_eq!(
            q10c(&data)?,
            Some(("Himself", "Evil Eyes: Behind the Scenes"))
        );
        Ok(())
    }
}
