use crate::data::ImdbData;
use ahash::{HashMap, HashMapExt, HashSet};
use polars::prelude::*;
use std::time::Instant;

pub fn q16d(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    let an = &db.an;
    let ci = &db.ci;
    let cn = &db.cn;
    let k = &db.k;
    let mk = &db.mk;
    let t = &db.t;
    let mc = &db.mc;

    let an_m: HashMap<i32, Vec<&str>> = an
        .column("person_id")?
        .i32()?
        .into_no_null_iter()
        .zip(an.column("name")?.str()?.into_no_null_iter())
        .fold(HashMap::new(), |mut acc, (person_id, name)| {
            acc.entry(person_id).or_default().push(name);
            acc
        });

    let start = Instant::now();

    let k_id = k
        .column("keyword")?
        .str()?
        .into_no_null_iter()
        .zip(k.column("id")?.i32()?.into_no_null_iter())
        .find(|(keyword, _)| *keyword == "character-name-in-title")
        .map(|(_, id)| id)
        .unwrap();

    let mk_s: HashSet<i32> = mk
        .column("keyword_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mk.column("movie_id")?.i32()?.into_no_null_iter())
        .filter_map(|(keyword_id, movie_id)| (k_id == keyword_id).then_some(movie_id))
        .collect();

    let cn_s: HashSet<i32> = cn
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(cn.column("country_code")?.str()?)
        .filter_map(|(id, country_code)| country_code.filter(|&cc| cc == "[us]").map(|_| id))
        .collect();

    let mc_s: HashSet<i32> = mc
        .column("movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mc.column("company_id")?.i32()?.into_no_null_iter())
        .filter_map(|(movie_id, company_id)| {
            (cn_s.contains(&company_id) && mk_s.contains(&movie_id)).then_some(movie_id)
        })
        .collect();

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(t.column("title")?.str()?.into_no_null_iter())
        .zip(t.column("episode_nr")?.i32()?)
        .filter_map(|((id, title), episode_nr)| {
            if mk_s.contains(&id)
                && mc_s.contains(&id)
                && let Some(episode_nr) = episode_nr
                && (5..100).contains(&episode_nr)
            {
                Some((id, title))
            } else {
                None
            }
        })
        .collect();

    let mut res: Option<(&str, &str)> = None;

    for (person_id, movie_id) in ci
        .column("person_id")?
        .i32()?
        .into_no_null_iter()
        .zip(ci.column("movie_id")?.i32()?.into_no_null_iter())
    {
        if let Some(title) = t_m.get(&movie_id)
            && let Some(names) = an_m.get(&person_id)
        {
            res = match res {
                Some((old_name, old_title)) => Some((
                    names.iter().min().unwrap().min(&old_name),
                    title.min(&old_title),
                )),
                None => Some((names.iter().min().unwrap(), title)),
            };
        }
    }

    println!("16d,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 16d
// SELECT MIN(an.name) AS cool_actor_pseudonym, MIN(t.title) AS series_named_after_char
// FROM aka_name AS an,
// cast_info AS ci,
// company_name AS cn,
// keyword AS k,
// movie_companies AS mc,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE cn.country_code = '[us]'
// AND k.keyword = 'character-name-in-title'
// AND t.episode_nr >= 5
// AND t.episode_nr < 100
// AND an.person_id = n.id
// AND n.id = ci.person_id
// AND ci.movie_id = t.id
// AND t.id = mk.movie_id
// AND mk.keyword_id = k.id
// AND t.id = mc.movie_id
// AND mc.company_id = cn.id
// AND an.person_id = ci.person_id
// AND ci.movie_id = mc.movie_id
// AND ci.movie_id = mk.movie_id
// AND mc.movie_id = mk.movie_id;
#[cfg(test)]
mod test_16d {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q16d() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q16d(&db)?;
        assert_eq!(res, Some(("\"Brooklyn\" Tony Danza", "(#1.5)")));
        Ok(())
    }
}
