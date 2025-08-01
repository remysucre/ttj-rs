use crate::data::ImdbData;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

// * imdb_int.movie_keyword(movie_id,keyword_id)
// * |imdb.q3c_title(movie_id,kind_id)
// * |imdb.q3c_keyword(keyword_id)
// * |imdb.q3c_movie_info(movie_id,info_type_id)
pub fn q3c(db: &ImdbData) -> Result<Option<&str>, PolarsError> {
    let k = &db.k;
    let mi = &db.mi;
    let mk = &db.mk;
    let t = &db.t;

    let start = Instant::now();

    let k_s: HashSet<i32> = k
        .column("keyword")?
        .str()?
        .into_iter()
        .zip(k.column("id")?.i32()?)
        .filter_map(|(keyword, id)| {
            if let (Some(keyword), Some(id)) = (keyword, id) {
                if keyword.contains("sequel") {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mk_s: HashSet<i32> = mk
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("keyword_id")?.i32()?)
        .filter_map(|(movie_id, keyword_id)| {
            if let (Some(movie_id), Some(keyword_id)) = (movie_id, keyword_id) {
                if k_s.contains(&keyword_id) {
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
                        | "USA"
                        | "American"
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

    for ((movie_id, production_year), title) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("production_year")?.i32()?.into_iter())
        .zip(t.column("title")?.str()?.into_iter())
    {
        if let (Some(movie_id), Some(production_year), Some(title)) =
            (movie_id, production_year, title)
        {
            if production_year > 1990 && mk_s.contains(&movie_id) && mi_s.contains(&movie_id) {
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
    println!("3c,{duration:}");

    Ok(res)
}

// -- JOB Query 3c
// SELECT MIN(t.title) AS movie_title
// FROM keyword AS k,
// movie_info AS mi,
// movie_keyword AS mk,
// title AS t
// WHERE k.keyword like '%sequel%'
// AND mi.info IN
// ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German', 'USA', 'American')
// AND t.production_year > 1990
// AND t.id = mi.movie_id
// AND t.id = mk.movie_id
// AND mk.movie_id = mi.movie_id
// AND k.id = mk.keyword_id;
#[cfg(test)]
mod test_3c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q3c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q3c(&db)?;
        assert_eq!(res, Some("& Teller 2"));
        Ok(())
    }
}
