use crate::data::Data;
use ahash::HashMap;
use ahash::HashSet;
use memchr::memmem;
use polars::prelude::*;
use std::time::Instant;

pub fn q8b(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
    let t = &db.t;
    let an = &db.an;
    let n = &db.n;
    let rt = &db.rt;
    let ci = &db.ci;
    let mc = &db.mc;
    let cn = &db.cn;

    let op_finder = memmem::Finder::new("One Piece");
    let dbz_finder = memmem::Finder::new("Dragon Ball Z");
    let yo_finder = memmem::Finder::new("Yo");
    let yu_finder = memmem::Finder::new("Yu");
    let japan_finder = memmem::Finder::new("(Japan)");
    let usa_finder = memmem::Finder::new("(USA)");
    let y2006_finder = memmem::Finder::new("(2006)");
    let y2007_finder = memmem::Finder::new("(2007)");

    let start = Instant::now();

    let an_m: HashMap<i32, Vec<&str>> =
        an.person_id
            .iter()
            .zip(an.name.iter())
            .fold(HashMap::default(), |mut acc, (id, name)| {
                acc.entry(*id).or_default().push(name);
                acc
            });

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|((id, title), production_year)| {
                let title_bytes = title.as_bytes();
                production_year
                    .filter(|&year| {
                        (2006..=2007).contains(&year)
                            && (op_finder.find(title_bytes) == Some(0)
                                || dbz_finder.find(title_bytes) == Some(0))
                    })
                    .map(|_| (id, title.as_str()))
            })
            .collect();

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
                .filter(|&code| code == "[jp]") // Check if the inner value is "[jp]"
                .map(|_| *id) // If it is, map the result to the id
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
                    let note_bytes = note.as_bytes();
                    japan_finder.find(note_bytes).is_some()
                        && usa_finder.find(note_bytes).is_none()
                        && (y2006_finder.find(note_bytes).is_some()
                            || y2007_finder.find(note_bytes).is_some())
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
            && note == "(voice: English version)"
            && rt_s.contains(&role_id)
            && n_s.contains(&person_id)
            && mc_s.contains(&movie_id)
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

    println!("8b,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 8b
// SELECT MIN(an.name) AS acress_pseudonym, MIN(t.title) AS japanese_anime_movie
// FROM aka_name AS an,
// cast_info AS ci,
// company_name AS cn,
// movie_companies AS mc,
// name AS n,
// role_type AS rt,
// title AS t
// WHERE ci.note = '(voice: English version)'
// AND cn.country_code = '[jp]'
// AND mc.note like '%(Japan)%'
// and mc.note not like '%(USA)%'
// and (mc.note like '%(2006)%' or mc.note like '%(2007)%')
// AND n.name like '%Yo%'
// and n.name not like '%Yu%'
// AND rt.role = 'actress'
// AND t.production_year between 2006 and 2007
// and (t.title like 'One Piece%' or t.title like 'Dragon Ball Z%')
// AND an.person_id = n.id
// AND n.id = ci.person_id
// AND ci.movie_id = t.id
// AND t.id = mc.movie_id
// AND mc.company_id = cn.id
// AND ci.role_id = rt.id
// AND an.person_id = ci.person_id
// AND ci.movie_id = mc.movie_id;
#[cfg(test)]
mod test_8b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q8b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q8b(&data)?;
        assert_eq!(
            res,
            Some(("Chambers, Linda", "Dragon Ball Z: Shin Budokai"))
        );
        Ok(())
    }
}
