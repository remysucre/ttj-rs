use crate::data::Data;
use ahash::HashMap;
use ahash::HashSet;
use memchr::memmem;
use polars::prelude::*;
use std::time::Instant;

// * imdb.q8a_cast_info(person_id,movie_id,person_role_id,role_id)
// * |imdb.q8a_name(person_id)
// * |imdb.q8a_role_type(role_id)
// * |imdb.q8a_movie_companies(movie_id,company_id,company_type_id)
// * ||imdb.q8a_company_name(company_id)
// * |imdb_int.aka_name(person_id)
// * |imdb_int.title(movie_id,kind_id)
pub fn q8a(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
    let t = &db.t;
    let an = &db.an;
    let n = &db.n;
    let rt = &db.rt;
    let ci = &db.ci;
    let mc = &db.mc;
    let cn = &db.cn;

    let yo_finder = memmem::Finder::new("Yo");
    let yu_finder = memmem::Finder::new("Yu");
    let japan_finder = memmem::Finder::new("(Japan)");
    let usa_finder = memmem::Finder::new("(USA)");

    let start = Instant::now();

    let t_m: HashMap<i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .map(|(id, title)| (*id, title.as_str()))
            .collect();

    let an_m: HashMap<i32, Vec<&str>> =
        an.person_id
            .iter()
            .zip(an.name.iter())
            .fold(HashMap::default(), |mut acc, (id, name)| {
                acc.entry(*id).or_default().push(name);
                acc
            });

    let n_s: HashSet<i32> = n
        .id
        .iter()
        .zip(n.name.iter())
        .filter_map(|(id, name)| {
            (yo_finder.find(name.as_bytes()).is_some() && yu_finder.find(name.as_bytes()).is_none())
                .then_some(*id)
        })
        .collect();

    let rt_s: HashSet<i32> = rt
        .role
        .iter()
        .zip(rt.id.iter())
        .filter_map(|(role, id)| (role == "actress").then_some(*id))
        .collect();

    let cn_s: HashSet<i32> = cn
        .country_code
        .iter()
        .zip(cn.id.iter())
        .filter_map(|(country_code, id)| {
            country_code
                .as_ref()
                .filter(|&country_code| country_code == "[jp]")
                .map(|_| *id)
        })
        .collect();

    let mc_s: HashSet<i32> = mc
        .movie_id
        .iter()
        .zip(mc.note.iter())
        .zip(mc.company_id.iter())
        .filter_map(|((movie_id, note), company_id)| {
            note.as_ref()
                .filter(|note| {
                    japan_finder.find(note.as_bytes()).is_some()
                        && usa_finder.find(note.as_bytes()).is_none()
                        && cn_s.contains(&company_id)
                })
                .map(|_| *movie_id)
        })
        .collect();

    let mut res: Option<(&str, &str)> = None;

    for (((movie_id, person_id), role_id), note) in ci
        .movie_id
        .iter()
        .zip(ci.person_id.iter())
        .zip(ci.role_id.iter())
        .zip(ci.note.iter())
    {
        if let Some(note) = note
            && rt_s.contains(&role_id)
            && note == "(voice: English version)"
            && mc_s.contains(&movie_id)
            && n_s.contains(&person_id)
            && let Some(name) = an_m.get(&person_id)
            && let Some(title) = t_m.get(&movie_id)
        {
            res = match res {
                Some((old_name, old_title)) => Some((
                    name.iter().min().unwrap().min(&old_name),
                    title.min(&old_title),
                )),
                None => Some((name.iter().min().unwrap(), title)),
            };
        }
    }

    println!("8a,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 8a
// SELECT MIN(an1.name) AS actress_pseudonym, MIN(t.title) AS japanese_movie_dubbed
// FROM aka_name AS an1,
// cast_info AS ci,
// company_name AS cn,
// movie_companies AS mc,
// name AS n1,
// role_type AS rt,
// title AS t
// WHERE ci.note = '(voice: English version)'
// AND cn.country_code = '[jp]'
// AND mc.note like '%(Japan)%'
// and mc.note not like '%(USA)%'
// AND n1.name like '%Yo%'
// and n1.name not like '%Yu%'
// AND rt.role = 'actress'
// AND an1.person_id = n1.id
// AND n1.id = ci.person_id
// AND ci.movie_id = t.id
// AND t.id = mc.movie_id
// AND mc.company_id = cn.id
// AND ci.role_id = rt.id
// AND an1.person_id = ci.person_id
// AND ci.movie_id = mc.movie_id;
#[cfg(test)]
mod test_q8a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q8a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q8a(&data)?;
        assert_eq!(res, Some(("Chambers, Linda", ".hack//Quantum")));
        Ok(())
    }
}
