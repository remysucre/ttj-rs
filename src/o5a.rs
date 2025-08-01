use crate::data::ImdbData;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q5a(db: &ImdbData) -> Result<Option<&str>, PolarsError> {
    let ct = &db.ct;
    // let it = &db.it;
    let mc = &db.mc;
    let mi = &db.mi;
    let t = &db.t;

    let start = Instant::now();

    let ct_s: HashSet<i32> = ct
        .column("kind")?
        .str()?
        .into_iter()
        .zip(ct.column("id")?.i32()?)
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if kind == "production companies" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mc_s: HashSet<i32> = mc
        .column("note")?
        .str()?
        .into_iter()
        .zip(mc.column("movie_id")?.i32()?)
        .zip(mc.column("company_type_id")?.i32()?)
        .filter_map(|((note, movie_id), company_type_id)| {
            if let (Some(note), Some(movie_id), Some(company_type_id)) =
                (note, movie_id, company_type_id)
            {
                if note.contains("(theatrical)")
                    && note.contains("(France)")
                    && ct_s.contains(&company_type_id)
                {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mi_s: HashSet<i32> = mi
        .column("info")?
        .str()?
        .into_iter()
        .zip(mi.column("movie_id")?.i32()?)
        .filter_map(|(info, movie_id)| {
            if let (Some(info), Some(movie_id)) = (info, movie_id) {
                if matches!(
                    info,
                    "Sweden"
                        | "Norway"
                        | "Germany"
                        | "Denmark"
                        | "Swedish"
                        | "Denish"
                        | "Norwegian"
                        | "German"
                ) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut res: Option<&str> = None;

    for ((id, production_year), title) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("production_year")?.i32()?.into_iter())
        .zip(t.column("title")?.str()?.into_iter())
    {
        if let (Some(id), Some(production_year), Some(title)) = (id, production_year, title) {
            if production_year > 2005 && mc_s.contains(&id) && mi_s.contains(&id) {
                if let Some(old_title) = res.as_mut() {
                    if title < *old_title {
                        *old_title = title;
                    }
                } else {
                    res = Some(title);
                }
            }
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
        let res = q5a(&db)?;
        assert_eq!(res, None);
        Ok(())
    }
}
