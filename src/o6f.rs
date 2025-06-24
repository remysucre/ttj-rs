use ahash::HashMap;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q6f() -> Result<(), PolarsError> {
    let ci = LazyFrame::scan_parquet("imdb/cast_info.parquet", Default::default())?.collect()?;
    let k = LazyFrame::scan_parquet("imdb/keyword.parquet", Default::default())?.collect()?;
    let mk =
        LazyFrame::scan_parquet("imdb/movie_keyword.parquet", Default::default())?.collect()?;
    let n = LazyFrame::scan_parquet("imdb/name.parquet", Default::default())?.collect()?;
    let t = LazyFrame::scan_parquet("imdb/title.parquet", Default::default())?.collect()?;

    let start = Instant::now();

    let mut k_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (id, keyword) in k
        .column("id")?
        .i32()?
        .into_iter()
        .zip(k.column("keyword")?.str()?.into_iter())
    {
        if let (Some(id), Some(keyword)) = (id, keyword) {
            if matches!(
                keyword,
                "superhero"
                    | "sequel"
                    | "second-part"
                    | "marvel-comics"
                    | "based-on-comic"
                    | "tv-special"
                    | "fight"
                    | "violence"
            ) {
                k_m.entry(id).or_default().push(keyword);
            }
        }
    }

    let mut mk_m: HashMap<i32, Vec<i32>> = HashMap::default();

    for (movie_id, keyword_id) in mk
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("keyword_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(keyword_id)) = (movie_id, keyword_id) {
            if k_m.contains_key(&keyword_id) {
                mk_m.entry(movie_id).or_default().push(keyword_id);
            }
        }
    }

    let mut t_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, title), production_year) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .zip(t.column("production_year")?.i32()?.into_iter())
    {
        if let (Some(id), Some(title), Some(production_year)) = (id, title, production_year) {
            if mk_m.contains_key(&id) && production_year > 2000 {
                t_m.entry(id).or_default().push(title);
            }
        }
    }

    let mut n_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (id, name) in n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?.into_iter())
    {
        if let (Some(id), Some(name)) = (id, name) {
            n_m.entry(id).or_default().push(name);
        }
    }

    let mut res: Option<(&str, &str, &str)> = None;

    for (pid, mid) in ci
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("movie_id")?.i32()?.into_iter())
    {
        if let (Some(pid), Some(mid)) = (pid, mid) {
            if let Some(titles) = t_m.get(&mid) {
                if let Some(kids) = mk_m.get(&mid) {
                    if let Some(names) = n_m.get(&pid) {
                        for kid in kids {
                            if let Some(keywords) = k_m.get(kid) {
                                for title in titles {
                                    for keyword in keywords {
                                        for name in names {
                                            if let Some((old_name, old_keyword, old_title)) =
                                                res.as_mut()
                                            {
                                                if name < old_name {
                                                    *old_name = name;
                                                }
                                                if keyword < old_keyword {
                                                    *old_keyword = keyword;
                                                }
                                                if title < old_title {
                                                    *old_title = title;
                                                }
                                            } else {
                                                res = Some((name, keyword, title));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    dbg!(res);

    let duration = start.elapsed();
    dbg!("total elapsed");
    dbg!(duration);

    Ok(())
}

// -- JOB Query 6f
// SELECT MIN(k.keyword) AS movie_keyword, MIN(n.name) AS actor_name, MIN(t.title) AS hero_movie
// FROM cast_info AS ci,
// keyword AS k,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE k.keyword in
// ('superhero', 'sequel', 'second-part', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence')
// AND t.production_year > 2000
// AND k.id = mk.keyword_id
// AND t.id = mk.movie_id
// AND t.id = ci.movie_id
// AND ci.movie_id = mk.movie_id
// AND n.id = ci.person_id;
