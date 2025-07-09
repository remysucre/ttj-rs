use crate::data::ImdbData;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q5b(db: &ImdbData) -> Result<Option<&str>, PolarsError> {
    let ct = &db.ct;
    let it = &db.it;
    let mc = &db.mc;
    let mi = &db.mi;
    let t = &db.t;

    let start = Instant::now();

    let ct_s: HashSet<i32> = ct
        .column("kind")?
        .str()?
        .into_iter()
        .zip(ct.column("id")?.i32()?.into_iter())
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
        .zip(mc.column("movie_id")?.i32()?.into_iter())
        .zip(mc.column("company_type_id")?.i32()?.into_iter())
        .filter_map(|((note, movie_id), company_type_id)| {
            if let (Some(note), Some(movie_id), Some(company_type_id)) =
                (note, movie_id, company_type_id)
            {
                if note.contains("(VHS)")
                    && note.contains("(USA)")
                    && note.contains("(1994)")
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
        .zip(mi.column("movie_id")?.i32()?.into_iter())
        .filter_map(|(info, movie_id)| {
            if let (Some(info), Some(movie_id)) = (info, movie_id) {
                if matches!(info, "USA" | "America") {
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
            if production_year > 2010 && mc_s.contains(&id) && mi_s.contains(&id) {
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

    dbg!(start.elapsed());

    Ok(res)
}

// -- JOB Query 5b
// SELECT MIN(t.title) AS american_vhs_movie
// FROM company_type AS ct,
// info_type AS it,
// movie_companies AS mc,
// movie_info AS mi,
// title AS t
// WHERE ct.kind = 'production companies'
// AND mc.note like '%(VHS)%'
// and mc.note like '%(USA)%'
// and mc.note like '%(1994)%'
// AND mi.info IN ('USA', 'America')
// AND t.production_year > 2010
// AND t.id = mi.movie_id
// AND t.id = mc.movie_id
// AND mc.movie_id = mi.movie_id
// AND ct.id = mc.company_type_id
// AND it.id = mi.info_type_id;
#[cfg(test)]
mod test_5b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q5b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q5b(&db)?;
        assert_eq!(res, None);
        Ok(())
    }
}
