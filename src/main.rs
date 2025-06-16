use polars::prelude::*;
use std::time::Instant;

fn main() {
    // q1a();
    // q16b();
    q19d();
}

fn q1a() {
    let ct = LazyFrame::scan_parquet("imdb/company_type.parquet", Default::default())
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
    let mi_idx = LazyFrame::scan_parquet("imdb/movie_info_idx.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();
    let t = LazyFrame::scan_parquet("imdb/title.parquet", Default::default())
        .unwrap()
        .collect()
        .unwrap();

    let start = Instant::now();

    let ct = ct
        .lazy()
        .filter(col("kind").eq(lit("production companies")))
        .collect()
        .unwrap();
    let it = it
        .lazy()
        .filter(col("info").eq(lit("top 250 rank")))
        .collect()
        .unwrap();
    let mc = mc
        .lazy()
        .filter(
            col("note")
                .str()
                .contains(lit("(as Metro-Goldwyn-Mayer Pictures)"), false)
                .not(),
        )
        .filter(
            col("note")
                .str()
                .contains(lit("(co-production)"), false)
                .or(col("note").str().contains(lit("(presents)"), false)),
        )
        .collect()
        .unwrap();

    let res = ct
        .lazy()
        .join_builder()
        .with(mc.lazy())
        .left_on([col("id")])
        .right_on([col("company_type_id")])
        .suffix("_mc")
        .finish()
        .join_builder()
        .with(t.lazy())
        .left_on([col("movie_id")])
        .right_on([col("id")])
        .suffix("_t")
        .finish()
        .join_builder()
        .with(mi_idx.lazy())
        .left_on([col("movie_id")])
        .right_on([col("movie_id")])
        .suffix("_mi_idx")
        .finish()
        .join_builder()
        .with(it.lazy())
        .left_on([col("info_type_id")])
        .right_on([col("id")])
        .suffix("_it")
        .finish()
        .select([
            col("note").min().alias("production_note"),
            col("title").min().alias("movie_title"),
            col("production_year").min().alias("movie_year"),
        ])
        .collect()
        .unwrap();

    println!("{:?}", res);

    let duration = start.elapsed();
    dbg!(duration);
}

// 1a.sql
// SELECT MIN(mc.note) AS production_note,
//        MIN(t.title) AS movie_title,
//        MIN(t.production_year) AS movie_year
// FROM company_type AS ct,
//      info_type AS it,
//      movie_companies AS mc,
//      movie_info_idx AS mi_idx,
//      title AS t
// WHERE ct.kind = 'production companies'
//   AND it.info = 'top 250 rank'
//   AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
//   AND (mc.note LIKE '%(co-production)%'
//        OR mc.note LIKE '%(presents)%')
//   AND ct.id = mc.company_type_id
//   AND t.id = mc.movie_id
//   AND t.id = mi_idx.movie_id
//   AND mc.movie_id = mi_idx.movie_id
//   AND it.id = mi_idx.info_type_id;

fn q16b() {
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

fn q19d() {
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
            col("name_n").min().alias("voicing_actress"),
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
