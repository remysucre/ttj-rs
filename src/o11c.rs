use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q11c(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cn = &db.cn;
    let ct = &db.ct;
    let k = &db.k;
    let lt = &db.lt;
    let mc = &db.mc;
    let mk = &db.mk;
    let ml = &db.ml;
    let t = &db.t;

    let lt_s: HashSet<i32> = lt.id.iter().copied().collect();

    let fox_num = Finder::new("20th Century Fox");
    let fox_word = Finder::new("Twentieth Century Fox");

    let start = Instant::now();

    let cn_m: HashMap<&i32, &str> = cn
        .id
        .iter()
        .zip(cn.name.iter())
        .zip(cn.country_code.iter())
        .filter_map(|((id, name), country_code)| {
            country_code
                .as_deref()
                .filter(|&code| {
                    (fox_num.find(name.as_bytes()) == Some(0)
                        || fox_word.find(name.as_bytes()) == Some(0))
                        && code != "[pl]"
                })
                .map(|_| (id, name.as_str()))
        })
        .collect();

    let ct_s: HashSet<i32> = ct
        .kind
        .iter()
        .zip(ct.id.iter().copied())
        .filter_map(|(kind_opt, id_opt)| (kind_opt != "production companies").then_some(id_opt))
        .collect();

    let target_keywords: HashSet<&str> = ["revenge", "sequel", "based-on-novel"]
        .into_iter()
        .collect();

    let k_s: HashSet<&i32> =
        k.id.iter()
            .zip(k.keyword.iter())
            .filter_map(|(id, keyword)| target_keywords.contains(keyword.as_str()).then_some(id))
            .collect();

    let mk_s: HashSet<&i32> = mk
        .movie_id
        .iter()
        .zip(mk.keyword_id.iter())
        .filter_map(|(movie_id, keyword_id)| (k_s.contains(keyword_id)).then_some(movie_id))
        .collect();

    let ml_s: HashSet<&i32> = ml
        .movie_id
        .iter()
        .zip(ml.link_type_id.iter())
        .filter_map(|(mid, lt_id)| lt_s.contains(&lt_id).then_some(mid))
        .collect();

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|((id, title), production_year)| {
                if let Some(production_year) = production_year
                    && mk_s.contains(&id)
                    && ml_s.contains(&id)
                    && *production_year > 1950
                {
                    Some((id, title.as_str()))
                } else {
                    None
                }
            })
            .collect();

    let mut res: Option<(&str, &str, &str)> = None;

    for (((mid, cid), company_type_id), note) in mc
        .movie_id
        .iter()
        .zip(mc.company_id.iter())
        .zip(mc.company_type_id.iter())
        .zip(mc.note.iter())
    {
        if let Some(note) = note
            && ct_s.contains(&company_type_id)
            && ml_s.contains(&mid)
            && let Some(name) = cn_m.get(&cid)
            && let Some(title) = t_m.get(&mid)
        {
            res = match res {
                Some((old_name, old_note, old_title)) => Some((
                    name.min(&old_name),
                    note.as_str().min(&old_note),
                    title.min(&old_title),
                )),
                None => Some((name, note.as_str(), title)),
            };
        }
    }

    println!("11c,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 11c
// SELECT MIN(cn.name) AS from_company, MIN(mc.note) AS production_note, MIN(t.title) AS movie_based_on_book
// FROM company_name AS cn,
// company_type AS ct,
// keyword AS k,
// link_type AS lt,
// movie_companies AS mc,
// movie_keyword AS mk,
// movie_link AS ml,
// title AS t
// WHERE cn.country_code !='[pl]' and (cn.name like '20th Century Fox%' or cn.name like 'Twentieth Century Fox%') AND ct.kind  != 'production companies' and ct.kind is not NULL AND k.keyword  in ('sequel', 'revenge', 'based-on-novel') AND mc.note  is not NULL AND t.production_year  > 1950 AND lt.id = ml.link_type_id AND ml.movie_id = t.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mc.movie_id AND mc.company_type_id = ct.id AND mc.company_id = cn.id AND ml.movie_id = mk.movie_id AND ml.movie_id = mc.movie_id AND mk.movie_id = mc.movie_id;
#[cfg(test)]
mod test_11c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q11c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);

        assert_eq!(
            q11c(&data)?,
            Some((
                "20th Century Fox Home Entertainment",
                "(1997-2002) (worldwide) (all media)",
                "24"
            ))
        );
        Ok(())
    }
}
