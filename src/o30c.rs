use ahash::{HashMap, HashSet};
use polars::prelude::*;
use std::time::Instant;
use crate::data::ImdbData;

pub fn q30c(db: &ImdbData) -> Result<(), PolarsError> {
    let cc = &db.cc;
    let cct1 = &db.cct;
    let cct2 = &db.cct;
    let ci = &db.ci;
    let it1 = &db.it;
    let it2 = &db.it;
    let k = &db.k;
    let mi = &db.mi;
    let mi_idx = &db.mi_idx;
    let mk = &db.mk;
    let n = &db.n;
    let t = &db.t;

    let elapsed = Instant::now();

    let cct1_s: HashSet<i32> = cct1
        .column("kind")?
        .str()?
        .into_iter()
        .zip(cct1.column("id")?.i32()?.into_iter())
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if kind == "cast" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let cct2_s: HashSet<i32> = cct2
        .column("kind")?
        .str()?
        .into_iter()
        .zip(cct2.column("id")?.i32()?.into_iter())
        .filter_map(|(kind, id)| {
            if let (Some(kind), Some(id)) = (kind, id) {
                if kind == "complete+verified" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let cc_s: HashSet<i32> = cc
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(cc.column("subject_id")?.i32()?.into_iter())
        .zip(cc.column("status_id")?.i32()?.into_iter())
        .filter_map(|((movie_id, subject_id), status_id)| {
            if let (Some(movie_id), Some(subject_id), Some(status_id)) =
                (movie_id, subject_id, status_id)
            {
                if cct1_s.contains(&subject_id) && cct2_s.contains(&status_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let k_s: HashSet<i32> = k
        .column("keyword")?
        .str()?
        .into_iter()
        .zip(k.column("id")?.i32()?.into_iter())
        .filter_map(|(keyword, id)| {
            if let (Some(keyword), Some(id)) = (keyword, id) {
                if matches!(
                    keyword,
                    "murder"
                        | "violence"
                        | "blood"
                        | "gore"
                        | "death"
                        | "female-nudity"
                        | "hospital"
                ) {
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
        .zip(mk.column("keyword_id")?.i32()?.into_iter())
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

    let it1_s: HashSet<i32> = it1
        .column("info")?
        .str()?
        .into_iter()
        .zip(it1.column("id")?.i32()?.into_iter())
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "genres" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let mut mi_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((movie_id, info), info_type_id) in mi
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi.column("info")?.str()?.into_iter())
        .zip(mi.column("info_type_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(info), Some(info_type_id)) = (movie_id, info, info_type_id) {
            if matches!(
                info,
                "Horror" | "Action" | "Sci-Fi" | "Thriller" | "Crime" | "War"
            ) && cc_s.contains(&movie_id)
                && mk_s.contains(&movie_id)
                && it1_s.contains(&info_type_id)
            {
                mi_m.entry(movie_id).or_default().push(info);
            }
        }
    }

    let it2_s: HashSet<i32> = it2
        .column("info")?
        .str()?
        .into_iter()
        .zip(it2.column("id")?.i32()?.into_iter())
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "votes" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let mut mi_idx_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((movie_id, info), info_type_id) in mi_idx
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi_idx.column("info")?.str()?.into_iter())
        .zip(mi_idx.column("info_type_id")?.i32()?.into_iter())
    {
        if let (Some(movie_id), Some(info), Some(info_type_id)) = (movie_id, info, info_type_id) {
            if it2_s.contains(&info_type_id) {
                mi_idx_m.entry(movie_id).or_default().push(info);
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
            if mi_m.contains_key(&id)
                && mi_idx_m.contains_key(&id)
                && mk_s.contains(&id)
                && cc_s.contains(&id)
            {
                t_m.entry(id).or_default().push(title);
            }
        }
    }

    let mut n_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, name), gender) in n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?.into_iter())
        .zip(n.column("gender")?.str()?.into_iter())
    {
        if let (Some(id), Some(name), Some(gender)) = (id, name, gender) {
            if gender == "m" {
                n_m.entry(id).or_default().push(name);
            }
        }
    }

    let mut res: Option<(&str, &str, &str, &str)> = None;

    for ((mid, pid), note) in ci
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("person_id")?.i32()?.into_iter())
        .zip(ci.column("note")?.str()?.into_iter())
    {
        if let (Some(mid), Some(pid), Some(note)) = (mid, pid, note) {
            if matches!(
                note,
                "(writer)" | "(head writer)" | "(written by)" | "(story)" | "(story editor)"
            ) {
                if let Some(titles) = t_m.get(&mid) {
                    if let Some(names) = n_m.get(&pid) {
                        if let Some(info) = mi_m.get(&mid) {
                            if let Some(info_idx) = mi_idx_m.get(&mid) {
                                for title in titles {
                                    for name in names {
                                        for movie_budget in info {
                                            for movie_votes in info_idx {
                                                if let Some((
                                                    old_name,
                                                    old_budget,
                                                    old_votes,
                                                    old_title,
                                                )) = res.as_mut()
                                                {
                                                    if name < old_name {
                                                        *old_name = name;
                                                    }
                                                    if movie_budget < old_budget {
                                                        *old_budget = movie_budget;
                                                    }
                                                    if movie_votes < old_votes {
                                                        *old_votes = movie_votes;
                                                    }
                                                    if title < old_title {
                                                        *old_title = title;
                                                    }
                                                } else {
                                                    res = Some((
                                                        name,
                                                        movie_budget,
                                                        movie_votes,
                                                        title,
                                                    ));
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
    }

    // println!("{:}", res);
    println!("{:}", elapsed.elapsed().as_secs_f32());

    Ok(())
}

// -- JOB Query 30c
// SELECT MIN(mi.info)     AS movie_budget,
// MIN(mi_idx.info) AS movie_votes,
// MIN(n.name)      AS writer,
// MIN(t.title)     AS complete_violent_movie
// FROM complete_cast AS cc,
// comp_cast_type AS cct1,
// comp_cast_type AS cct2,
// cast_info AS ci,
// info_type AS it1,
// info_type AS it2,
// keyword AS k,
// movie_info AS mi,
// movie_info_idx AS mi_idx,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE cct1.kind = 'cast'
// AND cct2.kind = 'complete+verified'
// AND ci.note in ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)')
// AND it1.info = 'genres'
// AND it2.info = 'votes'
// AND k.keyword in ('murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital')
// AND mi.info in ('Horror', 'Action', 'Sci-Fi', 'Thriller', 'Crime', 'War')
// AND n.gender = 'm'
// AND t.id = mi.movie_id
// AND t.id = mi_idx.movie_id
// AND t.id = ci.movie_id
// AND t.id = mk.movie_id
// AND t.id = cc.movie_id
// AND ci.movie_id = mi.movie_id
// AND ci.movie_id = mi_idx.movie_id
// AND ci.movie_id = mk.movie_id
// AND ci.movie_id = cc.movie_id
// AND mi.movie_id = mi_idx.movie_id
// AND mi.movie_id = mk.movie_id
// AND mi.movie_id = cc.movie_id
// AND mi_idx.movie_id = mk.movie_id
// AND mi_idx.movie_id = cc.movie_id
// AND mk.movie_id = cc.movie_id
// AND n.id = ci.person_id
// AND it1.id = mi.info_type_id
// AND it2.id = mi_idx.info_type_id
// AND k.id = mk.keyword_id
// AND cct1.id = cc.subject_id
// AND cct2.id = cc.status_id;
