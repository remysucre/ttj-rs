use crate::data::ImdbData;
use ahash::HashMap;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q24b(db: &ImdbData) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let ci = &db.ci;
    let t = &db.t;
    let n: &DataFrame = &db.n;
    let rt = &db.rt;
    let chn = &db.chn;
    let mi = &db.mi;
    let it = &db.it;
    let mc = &db.mc;
    let cn = &db.cn;
    let mk = &db.mk;
    let k = &db.k;
    let an = &db.an;

    let start = Instant::now();

    let an_s = an
        .column("person_id")?
        .i32()?
        .into_iter()
        .flatten()
        .collect::<HashSet<_>>();

    let chn_m: HashMap<i32, Vec<&str>> = chn
        .column("id")?
        .i32()?
        .into_iter()
        .zip(chn.column("name")?.str()?)
        .filter_map(|(id, name)| {
            if let (Some(id), Some(name)) = (id, name) {
                Some((id, name))
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (id, name)| {
            acc.entry(id).or_default().push(name);
            acc
        });

    let cn_s: HashSet<i32> = cn
        .column("country_code")?
        .str()?
        .into_iter()
        .zip(cn.column("id")?.i32()?)
        .zip(cn.column("name")?.str()?)
        .filter_map(|((country_code, id), name)| {
            if let (Some(country_code), Some(id), Some(name)) = (country_code, id, name) {
                if country_code == "[us]" && name == "DreamWorks Animation" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let it_s: HashSet<i32> = it
        .column("info")?
        .str()?
        .into_iter()
        .zip(it.column("id")?.i32()?)
        .filter_map(|(info, id)| {
            if let (Some(info), Some(id)) = (info, id) {
                if info == "release dates" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let k_s = k
        .column("id")?
        .i32()?
        .into_iter()
        .zip(k.column("keyword")?.str()?)
        .filter_map(|(id, keyword)| {
            if let (Some(id), Some(keyword)) = (id, keyword) {
                if matches!(
                    keyword,
                    "hero" | "martial-arts" | "hand-to-hand-combat" | "computer-animated-movie"
                ) {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let mi_s: HashSet<i32> = mi
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi.column("info")?.str()?)
        .zip(mi.column("info_type_id")?.i32()?)
        .filter_map(|((movie_id, info), info_type_id)| {
            if let (Some(movie_id), Some(info), Some(info_type_id)) = (movie_id, info, info_type_id)
            {
                if it_s.contains(&info_type_id)
                    && ((info.starts_with("Japan:") && info.contains("201"))
                        || (info.starts_with("USA:") && info.contains("201")))
                {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let n_m: HashMap<i32, Vec<&str>> = n
        .column("gender")?
        .str()?
        .into_iter()
        .zip(n.column("id")?.i32()?)
        .zip(n.column("name")?.str()?)
        .filter_map(|((gender, id), name)| {
            if let (Some(gender), Some(id), Some(name)) = (gender, id, name) {
                if an_s.contains(&id) && gender == "f" && name.contains("An") {
                    Some((id, name))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (id, name)| {
            acc.entry(id).or_default().push(name);
            acc
        });

    let rt_s: HashSet<i32> = rt
        .column("id")?
        .i32()?
        .into_iter()
        .zip(rt.column("role")?.str()?)
        .filter_map(|(id, role)| {
            if let (Some(id), Some(role)) = (id, role) {
                if role == "actress" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect();

    let t_m: HashMap<i32, Vec<&str>> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?)
        .zip(t.column("production_year")?.i32()?)
        .filter_map(|((id, title), production_year)| {
            if let (Some(id), Some(title), Some(production_year)) = (id, title, production_year) {
                if production_year > 2010 && title.starts_with("Kung Fu Panda") {
                    Some((id, title))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (id, title)| {
            acc.entry(id).or_default().push(title);
            acc
        });

    let mk_s = mk
        .column("keyword_id")?
        .i32()?
        .into_iter()
        .zip(mk.column("movie_id")?.i32()?)
        .filter_map(|(keyword_id, movie_id)| {
            if let (Some(keyword_id), Some(movie_id)) = (keyword_id, movie_id) {
                if k_s.contains(&keyword_id) && mi_s.contains(&movie_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let mc_s: HashSet<i32> = mc
        .column("company_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("movie_id")?.i32()?)
        .filter_map(|(company_id, movie_id)| {
            if let (Some(company_id), Some(movie_id)) = (company_id, movie_id) {
                if cn_s.contains(&company_id) && mk_s.contains(&movie_id) {
                    Some(movie_id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut res = None;

    for ((((movie_id, person_id), person_role_id), role_id), note) in ci
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("person_id")?.i32()?.into_iter())
        .zip(ci.column("person_role_id")?.i32()?.into_iter())
        .zip(ci.column("role_id")?.i32()?.into_iter())
        .zip(ci.column("note")?.str()?.into_iter())
    {
        if let (Some(role_id), Some(movie_id), Some(person_id), Some(person_role_id), Some(note)) =
            (role_id, movie_id, person_id, person_role_id, note)
        {
            if rt_s.contains(&role_id)
                && matches!(
                    note,
                    "(voice)"
                        | "(voice: Japanese version)"
                        | "(voice) (uncredited)"
                        | "(voice: English version)"
                )
                && mc_s.contains(&movie_id)
            {
                if let (Some(titles), Some(names), Some(char_names)) = (
                    t_m.get(&movie_id),
                    n_m.get(&person_id),
                    chn_m.get(&person_role_id),
                ) {
                    for title in titles {
                        for name in names {
                            for char_name in char_names {
                                if let Some((old_char_name, old_name, old_title)) = res.as_mut() {
                                    if name < old_name {
                                        *old_name = *name;
                                    }
                                    if title < old_title {
                                        *old_title = *title;
                                    }
                                    if char_name < old_char_name {
                                        *old_char_name = *char_name;
                                    }
                                } else {
                                    res = Some((char_name, name, title));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    println!("{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 24b
// SELECT MIN(chn.name) AS voiced_char_name, MIN(n.name) AS voicing_actress_name, MIN(t.title) AS kung_fu_panda
// FROM aka_name AS an,
// char_name AS chn,
// cast_info AS ci,
// company_name AS cn,
// info_type AS it,
// keyword AS k,
// movie_companies AS mc,
// movie_info AS mi,
// movie_keyword AS mk,
// name AS n,
// role_type AS rt,
// title AS t
// WHERE ci.note in ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')
// AND cn.country_code = '[us]'
// AND cn.name = 'DreamWorks Animation'
// AND it.info = 'release dates'
// AND k.keyword in ('hero', 'martial-arts', 'hand-to-hand-combat', 'computer-animated-movie')
// AND mi.info is not null
// and (mi.info like 'Japan:%201%' or mi.info like 'USA:%201%')
// AND n.gender = 'f'
// and n.name like '%An%'
// AND rt.role = 'actress'
// AND t.production_year > 2010
// AND t.title like 'Kung Fu Panda%'
// AND t.id = mi.movie_id
// AND t.id = mc.movie_id
// AND t.id = ci.movie_id
// AND t.id = mk.movie_id
// AND mc.movie_id = ci.movie_id
// AND mc.movie_id = mi.movie_id
// AND mc.movie_id = mk.movie_id
// AND mi.movie_id = ci.movie_id
// AND mi.movie_id = mk.movie_id
// AND ci.movie_id = mk.movie_id
// AND cn.id = mc.company_id
// AND it.id = mi.info_type_id
// AND n.id = ci.person_id
// AND rt.id = ci.role_id
// AND n.id = an.person_id
// AND ci.person_id = an.person_id
// AND chn.id = ci.person_role_id
// AND k.id = mk.keyword_id;
#[cfg(test)]
mod test_24b {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q24b() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q24b(&db)?;
        assert_eq!(res, Some(("Tigress", "Jolie, Angelina", "Kung Fu Panda 2")));
        Ok(())
    }
}
