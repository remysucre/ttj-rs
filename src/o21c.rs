use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q21c(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cn = &db.cn;
    let ct = &db.ct;
    let k = &db.k;
    let lt = &db.lt;
    let mc = &db.mc;
    let mi = &db.mi;
    let mk = &db.mk;
    let ml = &db.ml;
    let t = &db.t;

    let film = Finder::new("Film");
    let warner = Finder::new("Warner");
    let follow = Finder::new("follow");

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
                    (film.find(name.as_bytes()).is_some() || warner.find(name.as_bytes()).is_some())
                        && code != "[pl]"
                })
                .map(|_| (id, name.as_str()))
        })
        .collect();

    let ct_id = ct
        .kind
        .iter()
        .zip(ct.id.iter())
        .find(|(kind, _)| *kind == "production companies")
        .map(|(_, id)| id)
        .unwrap();

    let k_id = k
        .keyword
        .iter()
        .zip(k.id.iter())
        .find(|(keyword, _)| *keyword == "sequel")
        .map(|(_, id)| id)
        .unwrap();

    let mk_s: HashSet<&i32> = mk
        .movie_id
        .iter()
        .zip(mk.keyword_id.iter())
        .filter_map(|(movie_id, keyword_id)| (k_id == keyword_id).then_some(movie_id))
        .collect();

    let lt_m: HashMap<&i32, &str> = lt
        .id
        .iter()
        .zip(lt.link.iter())
        .filter_map(|(id, link)| {
            follow
                .find(link.as_bytes())
                .is_some()
                .then_some((id, link.as_str()))
        })
        .collect();

    let ml_m: HashMap<&i32, Vec<&str>> = ml
        .movie_id
        .iter()
        .zip(ml.link_type_id.iter())
        .filter_map(|(mid, lt_id)| lt_m.get(&lt_id).map(|link| (mid, link)))
        .fold(HashMap::default(), |mut acc, (mid, link)| {
            acc.entry(mid).or_default().push(link);
            acc
        });

    let target_info: HashSet<&str> = [
        "German",
        "Germany",
        "Sweden",
        "Norway",
        "Denmark",
        "Swedish",
        "Denish",
        "Norwegian",
        "English",
    ]
    .into_iter()
    .collect();

    let mi_s: HashSet<&i32> = mi
        .movie_id
        .iter()
        .zip(mi.info.iter())
        .filter_map(|(movie_id, info)| target_info.contains(info.as_str()).then_some(movie_id))
        .collect();

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|((id, title), production_year)| {
                if let Some(production_year) = production_year
                    && mk_s.contains(&id)
                    && ml_m.contains_key(&id)
                    && mi_s.contains(&id)
                    && (1950..=2010).contains(production_year)
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
        if ct_id == company_type_id
            && let Some(links) = ml_m.get(&mid)
            && let Some(name) = cn_m.get(&cid)
            && let Some(title) = t_m.get(&mid)
            && note.is_none()
        {
            res = match res {
                Some((old_name, old_link, old_title)) => Some((
                    name.min(&old_name),
                    links.iter().min().unwrap().min(&old_link),
                    title.min(&old_title),
                )),
                None => Some((name, links.iter().min().unwrap(), title)),
            };
        }
    }

    println!("21c,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 21c
// SELECT MIN(cn.name) AS company_name, MIN(lt.link) AS link_type, MIN(t.title) AS western_follow_up
// FROM company_name AS cn,
// company_type AS ct,
// keyword AS k,
// link_type AS lt,
// movie_companies AS mc,
// movie_info AS mi,
// movie_keyword AS mk,
// movie_link AS ml,
// title AS t
// WHERE cn.country_code !='[pl]' AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%') AND ct.kind ='production companies' AND k.keyword ='sequel' AND lt.link LIKE '%follow%' AND mc.note IS NULL AND mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German', 'English') AND t.production_year BETWEEN 1950 AND 2010 AND lt.id = ml.link_type_id AND ml.movie_id = t.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mc.movie_id AND mc.company_type_id = ct.id AND mc.company_id = cn.id AND mi.movie_id = t.id AND ml.movie_id = mk.movie_id AND ml.movie_id = mc.movie_id AND mk.movie_id = mc.movie_id AND ml.movie_id = mi.movie_id AND mk.movie_id = mi.movie_id AND mc.movie_id = mi.movie_id;
#[cfg(test)]
mod test_q21c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q21c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q21c(&data)?;
        assert_eq!(
            res,
            Some(("Churchill Films", "followed by", "Batman Beyond"))
        );
        Ok(())
    }
}
