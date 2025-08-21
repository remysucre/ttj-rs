use crate::data::Data;
use ahash::HashMap;
use ahash::HashSet;
use memchr::memmem::Finder;
use polars::error::PolarsError;
use std::time::Instant;

// imdb.q10b_cast_info(person_id,movie_id,person_role_id,role_id)
// |imdb_int.char_name(person_role_id)
// |imdb.q10b_title(movie_id,kind_id)
// ||imdb_int.movie_companies(movie_id,company_id,company_type_id)
// |||imdb.q10b_company_name(company_id)
// |||imdb_int.company_type(company_type_id)
// |imdb.q10b_role_type(role_id)
pub fn q10b(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
    let chn = &db.chn;
    let ci = &db.ci;
    let cn = &db.cn;
    // FK-PK optimization
    // let ct = &db.ct;
    let mc = &db.mc;
    let rt = &db.rt;
    let t = &db.t;

    let producer = Finder::new("(producer)");

    let start = Instant::now();

    let chn_m: HashMap<i32, &str> = chn
        .id
        .iter()
        .zip(chn.name.iter())
        .map(|(id, name)| (*id, name.as_str()))
        .collect();

    let cn_s: HashSet<i32> = cn
        .country_code
        .iter()
        .zip(cn.id.iter())
        .filter_map(|(country_code, id)| {
            country_code
                .as_ref()
                .filter(|code| code.as_str() == "[ru]")
                .map(|_| *id)
        })
        .collect();

    let mc_s: HashSet<i32> = mc
        .movie_id
        .iter()
        .zip(mc.company_id.iter())
        .filter_map(|(movie_id, company_id)| cn_s.contains(&company_id).then_some(*movie_id))
        .collect();

    let rt_id = rt
        .role
        .iter()
        .zip(rt.id.iter())
        .find(|(role, _)| *role == "actor")
        .map(|(_, id)| id)
        .unwrap();

    let t_m: HashMap<i32, &str> =
        t.id.iter()
            .zip(t.production_year.iter())
            .zip(t.title.iter())
            .filter_map(|((movie_id, production_year), title)| {
                if let Some(production_year) = production_year
                    && mc_s.contains(&movie_id)
                    && *production_year > 2010
                {
                    Some((*movie_id, title.as_str()))
                } else {
                    None
                }
            })
            .collect();

    let mut res: Option<(&str, &str)> = None;

    for (((mid, person_role_id), role_id), note) in ci
        .movie_id
        .iter()
        .zip(ci.person_role_id.iter())
        .zip(ci.role_id.iter())
        .zip(ci.note.iter())
    {
        if let Some(person_role_id) = person_role_id
            && let Some(note) = note
            && producer.find(note.as_bytes()).is_some()
            && let Some(character_name) = chn_m.get(&person_role_id)
            && let Some(title) = t_m.get(&mid)
            && rt_id == role_id
        {
            res = match res {
                Some((old_character_name, old_title)) => Some((
                    character_name.min(&old_character_name),
                    title.min(&old_title),
                )),
                None => Some((character_name, title)),
            };
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("10b,{duration:}");

    Ok(res)
}

// -- JOB Query 10b
// SELECT MIN(chn.name) AS character,
//        MIN(t.title) AS russian_mov_with_actor_producer
// FROM char_name AS chn,
//      cast_info AS ci,
//      company_name AS cn,
//      company_type AS ct,
//      movie_companies AS mc,
//      role_type AS rt,
//      title AS t
// WHERE ci.note LIKE '%(producer)%'
//   AND cn.country_code = '[ru]'
//   AND rt.role = 'actor'
//   AND t.production_year > 2010
//   AND t.id = mc.movie_id
//   AND t.id = ci.movie_id
//   AND ci.movie_id = mc.movie_id
//   AND chn.id = ci.person_role_id
//   AND rt.id = ci.role_id
//   AND cn.id = mc.company_id
//   AND ct.id = mc.company_type_id;
#[cfg(test)]
mod test_10b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q10b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q10b(&data)?;
        assert_eq!(res, None);
        Ok(())
    }
}
