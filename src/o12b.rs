use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem::Finder;
use polars::error::PolarsError;
use std::time::Instant;

pub fn q12b(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
    let cn = &db.cn;
    let ct = &db.ct;
    let it = &db.it;
    let mc = &db.mc;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let t = &db.t;

    let birdemic = Finder::new("Birdemic");
    let movie = Finder::new("Movie");

    let start = Instant::now();

    let mut it1_id: &i32 = &0;
    let mut it2_id: &i32 = &0;

    for (id, info) in it.id.iter().zip(it.info.iter()) {
        if info == "budget" {
            it1_id = id;
        } else if info == "bottom 10 rank" {
            it2_id = id;
        }
    }

    let mi_idx_s: HashSet<&i32> = mi_idx
        .movie_id
        .iter()
        .zip(mi_idx.info_type_id.iter())
        .filter_map(|(movie_id, info_type_id)| (it2_id == info_type_id).then_some(movie_id))
        .collect();

    let mi_m: HashMap<i32, Vec<&str>> = mi
        .movie_id
        .iter()
        .zip(mi.info.iter())
        .zip(mi.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            (it1_id == info_type_id).then_some((*movie_id, info.as_str()))
        })
        .fold(HashMap::default(), |mut acc, (movie_id, info)| {
            acc.entry(movie_id).or_default().push(info);
            acc
        });

    let ct_s: HashSet<&i32> = ct
        .kind
        .iter()
        .zip(ct.id.iter())
        .filter_map(|(kind, id)| {
            (kind == "production companies" || kind == "distributors").then_some(id)
        })
        .collect();

    let cn_s: HashSet<i32> = cn
        .country_code
        .iter()
        .zip(cn.id.iter())
        .filter_map(|(country_code, id)| {
            country_code
                .as_ref()
                .filter(|&code| code == "[us]")
                .map(|_| *id)
        })
        .collect();

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|((id, title), production_year)| {
                if let Some(production_year) = production_year
                    && *production_year > 2000
                    && (birdemic.find(title.as_bytes()) == Some(0)
                        || movie.find(title.as_bytes()).is_some())
                {
                    Some((id, title.as_str()))
                } else {
                    None
                }
            })
            .collect();

    let mut res: Option<(&str, &str)> = None;

    for ((movie_id, company_id), company_type_id) in mc
        .movie_id
        .iter()
        .zip(mc.company_id.iter())
        .zip(mc.company_type_id.iter())
    {
        if let Some(title) = t_m.get(&movie_id)
            && ct_s.contains(&company_type_id)
            && cn_s.contains(&company_id)
            && mi_idx_s.contains(&movie_id)
            && let Some(info) = mi_m.get(&movie_id)
        {
            res = match res {
                Some((old_info, old_title)) => Some((
                    info.iter().min().unwrap().min(&old_info),
                    title.min(&old_title),
                )),
                None => Some((info.iter().min().unwrap(), title)),
            };
        }
    }

    println!("12b,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 12b
// SELECT MIN(mi.info) AS budget, MIN(t.title) AS unsuccsessful_movie
// FROM company_name AS cn,
// company_type AS ct,
// info_type AS it1,
// info_type AS it2,
// movie_companies AS mc,
// movie_info AS mi,
// movie_info_idx AS mi_idx,
// title AS t
// WHERE cn.country_code = '[us]'
// AND ct.kind is not NULL
// and (ct.kind = 'production companies' or ct.kind = 'distributors')
// AND it1.info = 'budget'
// AND it2.info = 'bottom 10 rank'
// AND t.production_year > 2000
// AND (t.title LIKE 'Birdemic%' OR t.title LIKE '%Movie%')
// AND t.id = mi.movie_id
// AND t.id = mi_idx.movie_id
// AND mi.info_type_id = it1.id
// AND mi_idx.info_type_id = it2.id
// AND t.id = mc.movie_id
// AND ct.id = mc.company_type_id
// AND cn.id = mc.company_id
// AND mc.movie_id = mi.movie_id
// AND mc.movie_id = mi_idx.movie_id
// AND mi.movie_id = mi_idx.movie_id;
#[cfg(test)]
mod test_q12b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q12b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q12b(&data)?;
        assert_eq!(res, Some(("$10,000", "Birdemic: Shock and Terror")));
        Ok(())
    }
}
