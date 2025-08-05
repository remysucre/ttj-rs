use crate::data::Data;
use ahash::HashSet;
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q5c(db: &Data) -> Result<Option<&str>, PolarsError> {
    let ct = &db.ct;
    // let it = &db.it;
    let mc = &db.mc;
    let mi = &db.mi;
    let t = &db.t;

    let tv = Finder::new("(TV)");
    let usa = Finder::new("(USA)");

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
                && usa.find(note.as_bytes()).is_some()
                && tv.find(note.as_bytes()).is_none()
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
        "USA",
        "American",
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
            && production_year > &1990
            && mc_s.contains(&movie_id)
            && mi_s.contains(&movie_id)
        {
            res = match res {
                Some(old_title) => Some(title.as_str().min(&old_title)),
                None => Some(title),
            };
        }
    }

    println!("5c,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 5c
// SELECT MIN(t.title) AS american_movie
// FROM company_type AS ct,
// info_type AS it,
// movie_companies AS mc,
// movie_info AS mi,
// title AS t
// WHERE ct.kind = 'production companies'
// AND mc.note not like '%(TV)%'
// and mc.note like '%(USA)%'
// AND mi.info IN
// ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German', 'USA', 'American')
// AND t.production_year > 1990
// AND t.id = mi.movie_id
// AND t.id = mc.movie_id
// AND mc.movie_id = mi.movie_id
// AND ct.id = mc.company_type_id
// AND it.id = mi.info_type_id;
#[cfg(test)]
mod test_q5c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q5c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q5c(&data)?;
        assert_eq!(res, Some("11,830,420"));
        Ok(())
    }
}
