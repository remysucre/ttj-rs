use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q27b(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let cc = &db.cc;
    let cct = &db.cct;
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

    let mut cct2_id: i32 = 0;
    let mut cct1_s = HashSet::default();

    for (id, kind) in cct.id.iter().zip(cct.kind.iter()) {
        if kind == "complete" {
            cct2_id = *id;
        } else if kind == "cast" || kind == "crew" {
            cct1_s.insert(id);
        }
    }

    let cc_s: HashSet<i32> = cc
        .subject_id
        .iter()
        .zip(cc.status_id.iter())
        .zip(cc.movie_id.iter())
        .filter_map(|((subject_id, status_id), movie_id)| {
            (cct1_s.contains(subject_id) && cct2_id == *status_id).then_some(*movie_id)
        })
        .collect();

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

    let mk_s: HashSet<i32> = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| {
            (k_id == keyword_id && cc_s.contains(&movie_id)).then_some(*movie_id)
        })
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
        .filter_map(|(mid, lt_id)| {
            if mk_s.contains(&mid)
                && let Some(link) = lt_m.get(&lt_id)
            {
                Some((mid, link))
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (mid, link)| {
            acc.entry(mid).or_default().push(link);
            acc
        });

    let target_info: ahash::HashSet<&str> = ["Sweden", "Germany", "Swedish", "German"]
        .into_iter()
        .collect();

    let mi_s: HashSet<&i32> = mi
        .info
        .iter()
        .zip(mi.movie_id.iter())
        .filter_map(|(info, movie_id)| {
            (target_info.contains(info.as_str()) && ml_m.contains_key(&movie_id))
                .then_some(movie_id)
        })
        .collect();

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|((id, title), production_year)| {
                if let Some(production_year) = production_year
                    && mi_s.contains(&id)
                    && *production_year == 1998
                {
                    Some((id, title.as_str()))
                } else {
                    None
                }
            })
            .collect();

    let mut res: Option<(&str, &str, &str)> = None;

    for (((company_id, company_type_id), movie_id), note) in mc
        .company_id
        .iter()
        .zip(mc.company_type_id.iter())
        .zip(mc.movie_id.iter())
        .zip(mc.note.iter())
    {
        if ct_id == company_type_id
            && let Some(title) = t_m.get(&movie_id)
            && let Some(company) = cn_m.get(&company_id)
            && let Some(links) = ml_m.get(&movie_id)
            && note.is_none()
        {
            res = match res {
                Some((old_company, old_link, old_title)) => Some((
                    company.min(&old_company),
                    links.iter().min().unwrap().min(&old_link),
                    title.min(&old_title),
                )),
                None => Some((company, links.iter().min().unwrap(), title)),
            };
        }
    }

    println!("27b,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 27b
// SELECT MIN(cn.name) AS producing_company, MIN(lt.link) AS link_type, MIN(t.title) AS complete_western_sequel
// FROM complete_cast AS cc,
// comp_cast_type AS cct1,
// comp_cast_type AS cct2,
// company_name AS cn,
// company_type AS ct,
// keyword AS k,
// link_type AS lt,
// movie_companies AS mc,
// movie_info AS mi,
// movie_keyword AS mk,
// movie_link AS ml,
// title AS t
// WHERE cct1.kind in ('cast', 'crew')
// AND cct2.kind = 'complete'
// AND cn.country_code !='[pl]' AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%') AND ct.kind ='production companies' AND k.keyword ='sequel' AND lt.link LIKE '%follow%' AND mc.note IS NULL AND mi.info IN ('Sweden', 'Germany','Swedish', 'German') AND t.production_year  = 1998 AND lt.id = ml.link_type_id AND ml.movie_id = t.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mc.movie_id AND mc.company_type_id = ct.id AND mc.company_id = cn.id AND mi.movie_id = t.id AND t.id = cc.movie_id AND cct1.id = cc.subject_id AND cct2.id = cc.status_id AND ml.movie_id = mk.movie_id AND ml.movie_id = mc.movie_id AND mk.movie_id = mc.movie_id AND ml.movie_id = mi.movie_id AND mk.movie_id = mi.movie_id AND mc.movie_id = mi.movie_id AND ml.movie_id = cc.movie_id AND mk.movie_id = cc.movie_id AND mc.movie_id = cc.movie_id AND mi.movie_id = cc.movie_id;
#[cfg(test)]
mod test_q27b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q27b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q27b(&data)?;
        assert_eq!(
            res,
            Some(("Filmlance International AB", "followed by", "Vita nätter"))
        );
        Ok(())
    }
}
