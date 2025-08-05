use crate::data::Data;
use ahash::HashSet;
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q5a(db: &Data) -> Result<Option<&str>, PolarsError> {
    let ct = &db.ct;
    // let it = &db.it;
    let mc = &db.mc;
    let mi = &db.mi;
    let t = &db.t;

    let theatrical = Finder::new("(theatrical)");
    let france = Finder::new("(France)");

    let start = Instant::now();

    let ct_id = ct
        .kind
        .iter()
        .zip(ct.id.iter())
        .find(|(kind, _)| *kind == "production companies")
        .map(|(_, id)| id)
        .unwrap();

    let mc_s: HashSet<i32> = mc
        .note
        .iter()
        .zip(mc.movie_id.iter())
        .zip(mc.company_type_id.iter())
        .filter_map(|((note, movie_id), company_type_id)| {
            if let Some(note) = note
                && theatrical.find(note.as_bytes()).is_some()
                && france.find(note.as_bytes()).is_some()
                && ct_id == company_type_id
            {
                Some(*movie_id)
            } else {
                None
            }
        })
        .collect();

    let target_info: HashSet<&str> = [
        "Sweden",
        "Norway",
        "Germany",
        "Denmark",
        "Swedish",
        "Denish",
        "Norwegian",
        "German",
    ]
    .into_iter()
    .collect();

    let mi_s: HashSet<i32> = mi
        .info
        .iter()
        .zip(mi.movie_id.iter())
        .filter_map(|(info, movie_id)| target_info.contains(info.as_str()).then_some(*movie_id))
        .collect();

    let mut res: Option<&str> = None;

    for ((movie_id, production_year), title) in
        t.id.iter()
            .zip(t.production_year.iter())
            .zip(t.title.iter())
    {
        if let Some(production_year) = production_year
            && production_year > &2005
            && mc_s.contains(&movie_id)
            && mi_s.contains(&movie_id)
        {
            res = match res {
                Some(old_title) => Some(title.as_str().min(&old_title)),
                None => Some(title),
            };
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("5a,{duration:}");

    Ok(res)
}

// SELECT MIN(t.title) AS typical_european_movie
// FROM company_type AS ct,
//      info_type AS it,
//      movie_companies AS mc,
//      movie_info AS mi,
//      title AS t
// WHERE ct.kind = 'production companies'
//   AND mc.note LIKE '%(theatrical)%'
//   AND mc.note LIKE '%(France)%'
//   AND mi.info IN ('Sweden',
//                   'Norway',
//                   'Germany',
//                   'Denmark',
//                   'Swedish',
//                   'Denish',
//                   'Norwegian',
//                   'German')
//   AND t.production_year > 2005
//   AND t.id = mi.movie_id
//   AND t.id = mc.movie_id
//   AND mc.movie_id = mi.movie_id
//   AND ct.id = mc.company_type_id
//   AND it.id = mi.info_type_id;

#[cfg(test)]
mod test_q5a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q5a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q5a(&data)?;
        assert_eq!(res, None);
        Ok(())
    }
}
