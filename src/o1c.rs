use crate::data::Data;
use ahash::HashMap;
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

// imdb.q1c_movie_companies(movie_id,company_id,company_type_id)
// |imdb.q1c_title(movie_id,kind_id)
// |imdb.q1c_company_type(company_type_id)
// |imdb_int.movie_info_idx(movie_id,info_type_id)
// ||imdb.q1c_info_type(info_type_id)
pub fn q1c(db: &Data) -> Result<Option<(&str, &str, &i32)>, PolarsError> {
    let ct = &db.ct;
    let it = &db.it;
    let mc = &db.mc;
    let mi_idx = &db.mi_idx;
    let t = &db.t;

    let pictures = Finder::new("(as Metro-Goldwyn-Mayer Pictures)");
    let productions = Finder::new("(co-production)");

    let start = Instant::now();

    let it_id: &i32 = it
        .info
        .iter()
        .zip(it.id.iter())
        .find(|(info, _)| *info == "top 250 rank")
        .map(|(_, id)| id)
        .unwrap();

    let mi_idx_s: ahash::HashSet<i32> = mi_idx
        .movie_id
        .iter()
        .zip(mi_idx.info_type_id.iter())
        .filter_map(|(movie_id, info_type_id)| (it_id == info_type_id).then_some(*movie_id))
        .collect();

    let ct_id = ct
        .kind
        .iter()
        .zip(ct.id.iter())
        .find(|(kind, _)| *kind == "production companies")
        .map(|(_, id)| id)
        .unwrap();

    let mc_m: HashMap<i32, Vec<&str>> = mc
        .movie_id
        .iter()
        .zip(mc.note.iter())
        .zip(mc.company_type_id.iter())
        .filter_map(|((movie_id, note), company_type_id)| {
            if let Some(note) = note {
                if company_type_id == ct_id
                    && pictures.find(note.as_bytes()).is_none()
                    && productions.find(note.as_bytes()).is_some()
                    && mi_idx_s.contains(&movie_id)
                {
                    Some((*movie_id, note.as_str()))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (movie_id, note)| {
            acc.entry(movie_id).or_default().push(note);
            acc
        });

    let mut res: Option<(&str, &str, &i32)> = None;

    for ((id, title), production_year) in
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
    {
        if let Some(note) = mc_m.get(&id)
            && let Some(production_year) = production_year
            && production_year > &2010
        {
            res = match res {
                Some((old_note, old_title, old_production_year)) => Some((
                    note.iter().min().unwrap().min(&old_note),
                    title.as_str().min(&old_title),
                    production_year.min(&old_production_year),
                )),
                None => Some((note.iter().min().unwrap(), title, production_year)),
            };
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("1c,{duration:}");

    Ok(res)
}

// 1c.sql
// SELECT MIN(mc.note) AS production_note,
// MIN(t.title) AS movie_title,
// MIN(t.production_year) AS movie_year
// FROM company_type AS ct,
// info_type AS it,
// movie_companies AS mc,
// movie_info_idx AS mi_idx,
// title AS t
// WHERE ct.kind = 'production companies'
// AND it.info = 'top 250 rank'
// AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
// AND (mc.note LIKE '%(co-production)%')
// AND t.production_year >2010
// AND ct.id = mc.company_type_id
// AND t.id = mc.movie_id
// AND t.id = mi_idx.movie_id
// AND mc.movie_id = mi_idx.movie_id
// AND it.id = mi_idx.info_type_id;
#[cfg(test)]
mod test_q1c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q1c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        assert_eq!(
            q1c(&data)?,
            Some(("(co-production)", "Intouchables", &2011))
        );
        Ok(())
    }
}
