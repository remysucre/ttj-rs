use crate::data::Data;
use ahash::{HashMap, HashSet};
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q31c(db: &Data) -> Result<Option<(&str, &str, &str, &str)>, PolarsError> {
    let ci = &db.ci;
    let cn = &db.cn;
    let it = &db.it;
    let k = &db.k;
    let mc = &db.mc;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let mk = &db.mk;
    let n = &db.n;
    let t = &db.t;

    let n_m: HashMap<&i32, Vec<&str>> =
        n.id.iter()
            .zip(n.name.iter())
            .fold(HashMap::default(), |mut acc, (id, name)| {
                acc.entry(id).or_default().push(name);
                acc
            });

    let lionsgate = Finder::new("Lionsgate");

    let elapsed = Instant::now();

    let cn_s: HashSet<&i32> = cn
        .id
        .iter()
        .zip(cn.name.iter())
        .filter_map(|(id, name)| (lionsgate.find(name.as_bytes()) == Some(0)).then_some(id))
        .collect();

    let mc_s: HashSet<&i32> = mc
        .movie_id
        .iter()
        .zip(mc.company_id.iter())
        .filter_map(|(movie_id, company_id)| cn_s.contains(company_id).then_some(movie_id))
        .collect();

    let mut it_id: &i32 = &0;
    let mut it2_id: &i32 = &0;
    for (info, id) in it.info.iter().zip(it.id.iter()) {
        if info == &"genres" {
            it_id = id;
        } else if info == "votes" {
            it2_id = id;
        }
    }

    let target_info: HashSet<&str> = ["Horror", "Action", "Sci-Fi", "Thriller", "Crime", "War"]
        .into_iter()
        .collect();

    let mi_m: HashMap<i32, Vec<&str>> = mi
        .movie_id
        .iter()
        .zip(mi.info.iter())
        .zip(mi.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            (it_id == info_type_id
                && target_info.contains(info.as_str())
                && mc_s.contains(&movie_id))
            .then_some((*movie_id, info.as_str()))
        })
        .fold(HashMap::default(), |mut acc, (movie_id, info)| {
            acc.entry(movie_id).or_default().push(info);
            acc
        });

    let mi_idx_m: HashMap<&i32, Vec<&str>> = mi_idx
        .movie_id
        .iter()
        .zip(mi_idx.info.iter())
        .zip(mi_idx.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            (it2_id == info_type_id && mi_m.contains_key(&movie_id)).then_some((movie_id, info))
        })
        .fold(HashMap::default(), |mut acc, (idx, info)| {
            acc.entry(idx).or_insert_with(Vec::new).push(info);
            acc
        });

    let target_keywords: HashSet<&str> = [
        "murder",
        "violence",
        "blood",
        "gore",
        "death",
        "female-nudity",
        "hospital",
    ]
    .into_iter()
    .collect();

    let k_s: HashSet<&i32> =
        k.id.iter()
            .zip(k.keyword.iter())
            .filter_map(|(id, keyword)| target_keywords.contains(keyword.as_str()).then_some(id))
            .collect();

    let mk_s: HashSet<&i32> = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| {
            (k_s.contains(keyword_id) && mi_idx_m.contains_key(&movie_id)).then_some(movie_id)
        })
        .collect();

    let t_m: HashMap<&i32, &str> =
        t.id.iter()
            .zip(t.title.iter())
            .filter_map(|(id, title)| mk_s.contains(&id).then_some((id, title.as_str())))
            .collect();

    let target_note: ahash::HashSet<&str> = [
        "(writer)",
        "(head writer)",
        "(written by)",
        "(story)",
        "(story editor)",
    ]
    .into_iter()
    .collect();

    let mut res: Option<(&str, &str, &str, &str)> = None;

    for ((mid, pid), note) in ci
        .movie_id
        .iter()
        .zip(ci.person_id.iter())
        .zip(ci.note.iter())
    {
        if let Some(note) = note
            && let Some(title) = t_m.get(&mid)
            && target_note.contains(note.as_str())
            && let Some(names) = n_m.get(&pid)
            && let Some(info) = mi_m.get(&mid)
            && let Some(info_idx) = mi_idx_m.get(&mid)
        {
            res = match res {
                Some((old_info, old_info_idx, old_names, old_title)) => Some((
                    info.iter().min().unwrap().min(&old_info),
                    info_idx.iter().min().unwrap().min(&old_info_idx),
                    names.iter().min().unwrap().min(&old_names),
                    title.min(&old_title),
                )),
                None => Some((
                    info.iter().min().unwrap(),
                    info_idx.iter().min().unwrap(),
                    names.iter().min().unwrap(),
                    title,
                )),
            };
        }
    }

    println!("31c,{:}", elapsed.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 31c
// SELECT MIN(mi.info)     AS movie_budget,
// MIN(mi_idx.info) AS movie_votes,
// MIN(n.name)      AS writer,
// MIN(t.title)     AS violent_liongate_movie
// FROM cast_info AS ci,
// company_name AS cn,
// info_type AS it1,
// info_type AS it2,
// keyword AS k,
// movie_companies AS mc,
// movie_info AS mi,
// movie_info_idx AS mi_idx,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE ci.note in ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)')
// AND cn.name like 'Lionsgate%'
// AND it1.info = 'genres'
// AND it2.info = 'votes'
// AND k.keyword in ('murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital')
// AND mi.info in ('Horror', 'Action', 'Sci-Fi', 'Thriller', 'Crime', 'War')
// AND t.id = mi.movie_id
// AND t.id = mi_idx.movie_id
// AND t.id = ci.movie_id
// AND t.id = mk.movie_id
// AND t.id = mc.movie_id
// AND ci.movie_id = mi.movie_id
// AND ci.movie_id = mi_idx.movie_id
// AND ci.movie_id = mk.movie_id
// AND ci.movie_id = mc.movie_id
// AND mi.movie_id = mi_idx.movie_id
// AND mi.movie_id = mk.movie_id
// AND mi.movie_id = mc.movie_id
// AND mi_idx.movie_id = mk.movie_id
// AND mi_idx.movie_id = mc.movie_id
// AND mk.movie_id = mc.movie_id
// AND n.id = ci.person_id
// AND it1.id = mi.info_type_id
// AND it2.id = mi_idx.info_type_id
// AND k.id = mk.keyword_id
// AND cn.id = mc.company_id;
#[cfg(test)]
mod test_31c {
    use crate::data::{Data, ImdbData};
    use crate::o31c::q31c;
    use polars::error::PolarsError;

    #[test]
    fn test_q31c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q31c(&data)?;
        assert_eq!(res, Some(("Action", "1008", "Abraham, Brad", "11:14")));
        Ok(())
    }
}
