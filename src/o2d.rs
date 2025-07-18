use crate::data::ImdbData;
use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;

// * imdb_int.movie_companies(movie_id,company_id,company_type_id)
// * |imdb.q2c_company_name(company_id)
// * |imdb_int.title(movie_id,kind_id)
// * |imdb_int.movie_keyword(movie_id,keyword_id)
// * ||imdb.q2c_keyword(keyword_id)
pub fn q2d(db: &ImdbData) -> Result<Option<&str>, PolarsError> {
    let cn = &db.cn;
    let k = &db.k;
    let mc = &db.mc;
    let mk = &db.mk;
    let t = &db.t;

    let t_m: HashMap<i32, Vec<&str>> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .fold(HashMap::default(), |mut acc, (id, title)| {
            if let (Some(id), Some(title)) = (id, title) {
                acc.entry(id).or_default().push(title);
            }
            acc
        });

    let start = Instant::now();

    let cn_s: HashSet<i32> = cn
        .column("country_code")?
        .str()?
        .into_iter()
        .zip(cn.column("id")?.i32()?.into_iter())
        .filter_map(|(country_code, id)| {
            if let (Some(country_code), Some(id)) = (country_code, id) {
                if country_code == "[us]" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let k_s: HashSet<i32> = k
        .column("keyword")?
        .str()?
        .into_iter()
        .zip(k.column("id")?.i32()?.into_iter())
        .filter_map(|(keyword, id)| {
            if let (Some(keyword), Some(id)) = (keyword, id) {
                if keyword == "character-name-in-title" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mk_s = mk
        .column("keyword_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("movie_id")?.i32()?.into_iter())
        .filter_map(|(keyword_id, movie_id)| {
            if let (Some(keyword_id), Some(movie_id)) = (keyword_id, movie_id) {
                if k_s.contains(&keyword_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let mut res: Option<&str> = None;

    for (movie_id, company_id) in mc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("company_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(company_id)) = (movie_id, company_id) {
            if cn_s.contains(&company_id) && mk_s.contains(&movie_id) {
                if let Some(titles) = t_m.get(&movie_id) {
                    for title in titles {
                        if let Some(old_title) = res.as_mut() {
                            if *title < *old_title {
                                *old_title = title;
                            }
                        } else {
                            res = Some(title);
                        }
                    }
                }
            }
        }
    }

    dbg!(start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 2d
// SELECT MIN(t.title) AS movie_title
// FROM company_name AS cn,
// keyword AS k,
// movie_companies AS mc,
// movie_keyword AS mk,
// title AS t
// WHERE cn.country_code = '[us]'
// AND k.keyword = 'character-name-in-title'
// AND cn.id = mc.company_id
// AND mc.movie_id = t.id
// AND t.id = mk.movie_id
// AND mk.keyword_id = k.id
// AND mc.movie_id = mk.movie_id;
#[cfg(test)]
mod test_2d {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q2d() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q2d(&db)?;
        assert_eq!(res, Some("& Teller"));
        Ok(())
    }
}
