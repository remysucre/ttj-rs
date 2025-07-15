use crate::data::ImdbData;
use ahash::HashMap;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q19a(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    let an = &db.an;
    let chn = &db.chn;
    let ci = &db.ci;
    let cn = &db.cn;
    let it = &db.it;
    let mc = &db.mc;
    let mi = &db.mi;
    let n = &db.n;
    let rt = &db.rt;
    let t = &db.t;

    let chn_s = chn
        .column("id")?
        .i32()?
        .into_iter()
        .flatten()
        .collect::<HashSet<_>>();

    let start = Instant::now();

    let cn_s = cn
        .column("country_code")?
        .str()?
        .into_iter()
        .zip(cn.column("id")?.i32()?.into_iter())
        .filter_map(|(country_code, id)| {
            if let (Some(country_code), Some(id)) = (country_code, id) {
                if country_code == "[us]" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let it_s = it
        .column("info")?
        .str()?
        .into_iter()
        .zip(it.column("id")?.i32()?.into_iter())
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
        .collect::<HashSet<_>>();

    let an_s = an
        .column("person_id")?
        .i32()?
        .into_iter()
        .flatten()
        .collect::<HashSet<_>>();

    let rt_s = rt
        .column("role")?
        .str()?
        .into_iter()
        .zip(rt.column("id")?.i32()?.into_iter())
        .filter_map(|(role, id)| {
            if let (Some(role), Some(id)) = (role, id) {
                if role == "actress" { Some(id) } else { None }
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let mut n_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, name), gender) in n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?.into_iter())
        .zip(n.column("gender")?.str()?.into_iter())
    {
        if let (Some(id), Some(name), Some(gender)) = (id, name, gender) {
            if an_s.contains(&id) && gender == "f" && name.contains("Ang") {
                n_m.entry(id).or_default().push(name);
            }
        }
    }

    let mut mc_s: HashSet<i32> = HashSet::default();

    for ((cid, mid), note) in mc
        .column("company_id")?
        .i32()?
        .into_iter()
        .zip(mc.column("movie_id")?.i32()?.into_iter())
        .zip(mc.column("note")?.str()?.into_iter())
    {
        if let (Some(company_id), Some(movie_id), Some(note)) = (cid, mid, note) {
            if (note.contains("(USA)") || note.contains("(worldwide)"))
                && cn_s.contains(&company_id)
            {
                mc_s.insert(movie_id);
            }
        }
    }

    let mut mi_s = HashSet::default();

    for ((mid, it_id), info) in mi
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(mi.column("info_type_id")?.i32()?.into_iter())
        .zip(mi.column("info")?.str()?.into_iter())
    {
        if let (Some(mid), Some(it_id), Some(info)) = (mid, it_id, info) {
            if (info.starts_with("Japan:") || info.starts_with("USA:"))
                && info.contains("200")
                && it_s.contains(&it_id)
            {
                mi_s.insert(mid);
            }
        }
    }

    let mut t_m: HashMap<i32, Vec<&str>> = HashMap::default();

    for ((id, title), production_year) in t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("title")?.str()?.into_iter())
        .zip(t.column("production_year")?.i32()?.into_iter())
    {
        if let (Some(id), Some(title), Some(production_year)) = (id, title, production_year) {
            if (2005..=2009).contains(&production_year) && mi_s.contains(&id) && mc_s.contains(&id)
            {
                t_m.entry(id).or_default().push(title);
            }
        }
    }

    let mut res: Option<(&str, &str)> = None;

    for ((((mid, pid), rid), prid), note) in ci
        .column("movie_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("person_id")?.i32()?.into_iter())
        .zip(ci.column("role_id")?.i32()?.into_iter())
        .zip(ci.column("person_role_id")?.i32()?.into_iter())
        .zip(ci.column("note")?.str()?.into_iter())
    {
        if let (Some(mid), Some(pid), Some(rid), Some(prid), Some(note)) =
            (mid, pid, rid, prid, note)
        {
            if matches!(
                note,
                "(voice)"
                    | "(voice: Japanese version)"
                    | "(voice) (uncredited)"
                    | "(voice: English version)"
            ) {
                if rt_s.contains(&rid) && chn_s.contains(&prid) {
                    if let Some(titles) = t_m.get(&mid) {
                        if let Some(names) = n_m.get(&pid) {
                            for title in titles {
                                for name in names {
                                    if let Some((old_name, old_title)) = res.as_mut() {
                                        if *name < *old_name {
                                            *old_name = name;
                                        }
                                        if *title < *old_title {
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
    }

    println!("{:?}", res);
    let duration = start.elapsed();
    dbg!(duration);

    Ok(res)
}

// -- JOB Query 19a
// SELECT MIN(n.name) AS voicing_actress, MIN(t.title) AS voiced_movie
// FROM aka_name AS an,
// char_name AS chn,
// cast_info AS ci,
// company_name AS cn,
// info_type AS it,
// movie_companies AS mc,
// movie_info AS mi,
// name AS n,
// role_type AS rt,
// title AS t
// WHERE ci.note in ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')
// AND cn.country_code = '[us]'
// AND it.info = 'release dates'
// AND mc.note is not NULL
// and (mc.note like '%(USA)%' or mc.note like '%(worldwide)%')
// AND mi.info is not null
// and (mi.info like 'Japan:%200%' or mi.info like 'USA:%200%')
// AND n.gender = 'f'
// and n.name like '%Ang%'
// AND rt.role = 'actress'
// AND t.production_year between 2005 and 2009
// AND t.id = mi.movie_id
// AND t.id = mc.movie_id
// AND t.id = ci.movie_id
// AND mc.movie_id = ci.movie_id
// AND mc.movie_id = mi.movie_id
// AND mi.movie_id = ci.movie_id
// AND cn.id = mc.company_id
// AND it.id = mi.info_type_id
// AND n.id = ci.person_id
// AND rt.id = ci.role_id
// AND n.id = an.person_id
// AND ci.person_id = an.person_id
// AND chn.id = ci.person_role_id;
#[cfg(test)]
mod test_19a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q19a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q19a(&db)?;
        assert_eq!(res, Some(("Angeline, Moriah", "Blue Harvest")));
        Ok(())
    }
}
