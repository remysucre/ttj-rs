use polars::prelude::*;
use std::time::Instant;

pub fn q19d() {
    let an = LazyFrame::scan_parquet("imdb/aka_name.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let chn = LazyFrame::scan_parquet("imdb/char_name.parquet", Default::default())
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
    let it = LazyFrame::scan_parquet("imdb/info_type.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let mc = LazyFrame::scan_parquet("imdb/movie_companies.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let mi = LazyFrame::scan_parquet("imdb/movie_info.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let n = LazyFrame::scan_parquet("imdb/name.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let rt = LazyFrame::scan_parquet("imdb/role_type.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let t = LazyFrame::scan_parquet("imdb/title.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();

    let start = Instant::now();

    let s = Series::new(
        "x".into(),
        [
            "(voice)",
            "(voice: Japanese version)",
            "(voice) (uncredited)",
            "(voice: English version)",
        ],
    );

    let ci = ci
        .lazy()
        .filter(col("note").is_in(
            lit(s).implode(),
            false,
        ))
        .collect()
        .unwrap();

    let cn = cn
        .lazy()
        .filter(col("country_code").eq(lit("[us]")))
        .collect()
        .unwrap();

    let it = it
        .lazy()
        .filter(col("info").eq(lit("release dates")))
        .collect()
        .unwrap();

    let n = n
        .lazy()
        .filter(col("gender").eq(lit("f")))
        .collect()
        .unwrap();

    let rt = rt
        .lazy()
        .filter(col("role").eq(lit("actress")))
        .collect()
        .unwrap();

    let t = t
        .lazy()
        .filter(col("production_year").gt(lit(2000)))
        .collect()
        .unwrap();

    let res = t
        .lazy()
        .join_builder()
        .with(mi.lazy())
        .left_on([col("id")])
        .right_on([col("movie_id")])
        .suffix("_mi")
        .finish()
        .join_builder()
        .with(mc.lazy())
        .left_on([col("id")])
        .right_on([col("movie_id")])
        .suffix("_mc")
        .finish()
        .join_builder()
        .with(ci.lazy())
        .left_on([col("id")])
        .right_on([col("movie_id")])
        .suffix("_ci")
        .finish()
        .join_builder()
        .with(cn.lazy())
        .left_on([col("company_id")])
        .right_on([col("id")])
        .suffix("_cn")
        .finish()
        .join_builder()
        .with(it.lazy())
        .left_on([col("info_type_id")])
        .right_on([col("id")])
        .suffix("_it")
        .finish()
        .join_builder()
        .with(n.lazy())
        .left_on([col("person_id")])
        .right_on([col("id")])
        .suffix("_n")
        .finish()
        .join_builder()
        .with(an.lazy())
        .left_on([col("person_id")])
        .right_on([col("person_id")])
        .suffix("_an")
        .finish()
        .join_builder()
        .with(rt.lazy())
        .left_on([col("role_id")])
        .right_on([col("id")])
        .suffix("_rt")
        .finish()
        .join_builder()
        .with(chn.lazy())
        .left_on([col("person_role_id")])
        .right_on([col("id")])
        .suffix("_chn")
        .finish()
        .select([
            col("name").min().alias("voicing_actress"),
            col("title").min().alias("jap_engl_voiced_movie"),
        ])
        .collect()
        .unwrap();

    println!("{:?}", res);
    let duration = start.elapsed();
    dbg!(duration);
}

// 19d.sql
// SELECT MIN(n.name) AS voicing_actress,
//        MIN(t.title) AS jap_engl_voiced_movie
// FROM aka_name AS an,
//      char_name AS chn,
//      cast_info AS ci,
//      company_name AS cn,
//      info_type AS it,
//      movie_companies AS mc,
//      movie_info AS mi,
//      name AS n,
//      role_type AS rt,
//      title AS t
// WHERE ci.note IN ('(voice)',
//                   '(voice: Japanese version)',
//                   '(voice) (uncredited)',
//                   '(voice: English version)')
//   AND cn.country_code ='[us]'
//   AND it.info = 'release dates'
//   AND n.gender ='f'
//   AND rt.role ='actress'
//   AND t.production_year > 2000
//   AND t.id = mi.movie_id
//   AND t.id = mc.movie_id
//   AND t.id = ci.movie_id
//   AND mc.movie_id = ci.movie_id
//   AND mc.movie_id = mi.movie_id
//   AND mi.movie_id = ci.movie_id
//   AND cn.id = mc.company_id
//   AND it.id = mi.info_type_id
//   AND n.id = ci.person_id
//   AND rt.id = ci.role_id
//   AND n.id = an.person_id
//   AND ci.person_id = an.person_id
//   AND chn.id = ci.person_role_id; 