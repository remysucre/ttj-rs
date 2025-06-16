use polars::prelude::*;
use std::time::Instant;

pub fn q16bo() {
    let an = LazyFrame::scan_parquet("imdb/aka_name.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let ci = LazyFrame::scan_parquet("imdb/cast_info.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let cn = LazyFrame::scan_parquet("imdb/company_name.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let k = LazyFrame::scan_parquet("imdb/keyword.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let mc = LazyFrame::scan_parquet("imdb/movie_companies.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let mk = LazyFrame::scan_parquet("imdb/movie_keyword.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let n = LazyFrame::scan_parquet("imdb/name.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let t = LazyFrame::scan_parquet("imdb/title.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();

    let start = Instant::now();

    let cn = cn
        .lazy()
        .filter(col("country_code").eq(lit("[us]")))
        .collect()
        .unwrap();
    let k = k
        .lazy()
        .filter(col("keyword").eq(lit("character-name-in-title")))
        .collect()
        .unwrap();

    let res = an
        .lazy()
        .join_builder()
        .with(n.lazy())
        .left_on([col("person_id")])
        .right_on([col("id")])
        .suffix("_n")
        .finish()
        .join_builder()
        .with(ci.lazy())
        .left_on([col("person_id")])
        .right_on([col("person_id")])
        .suffix("_ci")
        .finish()
        .join_builder()
        .with(t.lazy())
        .left_on([col("movie_id")])
        .right_on([col("id")])
        .suffix("_t")
        .finish()
        .join_builder()
        .with(mk.lazy())
        .left_on([col("movie_id")])
        .right_on([col("movie_id")])
        .suffix("_mk")
        .finish()
        .join_builder()
        .with(k.lazy())
        .left_on([col("keyword_id")])
        .right_on([col("id")])
        .suffix("_k")
        .finish()
        .join_builder()
        .with(mc.lazy())
        .left_on([col("movie_id")])
        .right_on([col("movie_id")])
        .suffix("_mc")
        .finish()
        .join_builder()
        .with(cn.lazy())
        .left_on([col("company_id")])
        .right_on([col("id")])
        .suffix("_cn")
        .finish()
        .select([
            col("name").min().alias("cool_actor_pseudonym"),
            col("title").min().alias("series_named_after_char"),
        ])
        .collect()
        .unwrap();

    println!("{:?}", res);
    let duration = start.elapsed();
    dbg!(duration);
}

// 16b.sql
// SELECT MIN(an.name) AS cool_actor_pseudonym,
//        MIN(t.title) AS series_named_after_char
// FROM aka_name AS an,
//      cast_info AS ci,
//      company_name AS cn,
//      keyword AS k,
//      movie_companies AS mc,
//      movie_keyword AS mk,
//      name AS n,
//      title AS t
// WHERE cn.country_code ='[us]'
//   AND k.keyword ='character-name-in-title'
//   AND an.person_id = n.id
//   AND n.id = ci.person_id
//   AND ci.movie_id = t.id
//   AND t.id = mk.movie_id
//   AND mk.keyword_id = k.id
//   AND t.id = mc.movie_id
//   AND mc.company_id = cn.id
//   AND an.person_id = ci.person_id
//   AND ci.movie_id = mc.movie_id
//   AND ci.movie_id = mk.movie_id
//   AND mc.movie_id = mk.movie_id; 