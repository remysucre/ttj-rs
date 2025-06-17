use ahash::HashMap;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q19d() -> Result<(), PolarsError> {
    let an = LazyFrame::scan_parquet("imdb/aka_name.parquet", Default::default())?.collect()?;
    let chn = LazyFrame::scan_parquet("imdb/char_name.parquet", Default::default())?.collect()?;
    let ci = LazyFrame::scan_parquet("imdb/cast_info.parquet", Default::default())?.collect()?;
    let cn = LazyFrame::scan_parquet("imdb/company_name.parquet", Default::default())?.collect()?;
    let it = LazyFrame::scan_parquet("imdb/info_type.parquet", Default::default())?.collect()?;
    let mc =
        LazyFrame::scan_parquet("imdb/movie_companies.parquet", Default::default())?.collect()?;
    let mi = LazyFrame::scan_parquet("imdb/movie_info.parquet", Default::default())?.collect()?;
    let n = LazyFrame::scan_parquet("imdb/name.parquet", Default::default())?.collect()?;
    let rt = LazyFrame::scan_parquet("imdb/role_type.parquet", Default::default())?.collect()?;
    let t = LazyFrame::scan_parquet("imdb/title.parquet", Default::default())?.collect()?;

    let chn_s = chn
        .column("id")?
        .i32()?
        .into_iter()
        .flatten()
        .collect::<HashSet<_>>();

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
        .filter(col("note").is_in(lit(s).implode(), false))
        .collect()?;

    let cn = cn
        .lazy()
        .filter(col("country_code").eq(lit("[us]")))
        .collect()?;

    let it = it
        .lazy()
        .filter(col("info").eq(lit("release dates")))
        .collect()?;

    let n = n.lazy().filter(col("gender").eq(lit("f"))).collect()?;

    let rt = rt.lazy().filter(col("role").eq(lit("actress"))).collect()?;

    let t = t
        .lazy()
        .filter(col("production_year").gt(lit(2000)))
        .collect()?;

    let cn_s = cn
        .column("id")?
        .i32()?
        .into_iter()
        .flatten()
        .collect::<HashSet<_>>();

    let it_s = it
        .column("id")?
        .i32()?
        .into_iter()
        .flatten()
        .collect::<HashSet<_>>();

    let an_s = an
        .column("person_id")?
        .i32()?
        .into_iter()
        .flatten()
        .collect::<HashSet<_>>();

    let rt_s = rt
        .column("id")?
        .i32()?
        .into_iter()
        .flatten()
        .collect::<HashSet<_>>();

    let mut n_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (id, name) in n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?.into_iter())
    {
        if let (Some(id), Some(name)) = (id, name) {
            if an_s.contains(&id) {
                n_m.entry(id).or_default().push(name);
            }
        }
    }

    let mut mc_s: HashSet<i32> = HashSet::default();

    for (cid, mid) in mc
        .column("company_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("movie_id")?.i32()?.into_iter())
    {
        if let (Some(company_id), Some(movie_id)) = (cid, mid) {
            if cn_s.contains(&company_id) {
                mc_s.insert(movie_id);
            }
        }
    }

    let mut mi_s = HashSet::default();

    for (mid, it_id) in mi
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi.column("info_type_id")?.i32()?.into_iter())
    {
        if let (Some(mid), Some(it_id)) = (mid, it_id) {
            if it_s.contains(&it_id) {
                mi_s.insert(mid);
            }
        }
    }

    let mut t_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for (id, title) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
    {
        if let (Some(id), Some(title)) = (id, title) {
            if mi_s.contains(&id) && mc_s.contains(&id) {
                t_m.entry(id).or_default().push(title);
            }
        }
    }

    let mut res = None;

    for (((mid, pid), rid), prid) in ci
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("person_id")?.i32()?.into_iter())
        .zip(ci.column("role_id")?.i32()?.into_iter())
        .zip(ci.column("person_role_id")?.i32()?.into_iter())
    {
        if let (Some(mid), Some(pid), Some(rid), Some(prid)) = (mid, pid, rid, prid) {
            if rt_s.contains(&rid) && chn_s.contains(&prid) {
                if let Some(titles) = t_m.get(&mid) {
                    if let Some(names) = n_m.get(&pid) {
                        for title in titles {
                            for name in names {
                                if let Some((old_name, old_title)) = res.as_mut() {
                                    if name < *old_name {
                                        *old_name = name;
                                    }
                                    if title < *old_title {
                                        *old_title = title;
                                    }
                                } else {
                                    res = Some((name, title));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    println!("{:?}", res);
    let duration = start.elapsed();
    dbg!(duration);

    Ok(())
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
