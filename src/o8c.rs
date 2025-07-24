use crate::data::ImdbData;
use ahash::HashMap;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q8c(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    let t = &db.t;
    let an = &db.an;
    let n = &db.n;
    let rt = &db.rt;
    let ci = &db.ci;
    let mc = &db.mc;
    let cn = &db.cn;

    let an_m: HashMap<i32, Vec<&str>> = an
        .column("person_id")?
        .i32()?
        .into_no_null_iter()
        .zip(an.column("name")?.str()?.into_no_null_iter())
        .fold(HashMap::default(), |mut acc, (person_id, name)| {
            acc.entry(person_id).or_default().push(name);
            acc
        });

    let t_m: HashMap<i32, &str> = t
        .column("id")?
        .i32()?
        .into_no_null_iter()
        .zip(t.column("title")?.str()?.into_no_null_iter())
        .collect();

    let n_s: HashSet<i32> = n.column("id")?.i32()?.into_no_null_iter().collect();

    let start = Instant::now();

    let rt_s: HashSet<i32> = rt
        .column("role")?
        .str()?
        .into_no_null_iter()
        .zip(rt.column("id")?.i32()?.into_no_null_iter())
        .filter_map(|(role, id)| (role == "writer").then_some(id))
        .collect();

    let cn_s: HashSet<i32> = cn
        .column("country_code")?
        .str()?
        .into_iter()
        .zip(cn.column("id")?.i32()?.into_no_null_iter())
        .filter_map(|(country_code, id)| country_code.filter(|&code| code == "[us]").map(|_| id))
        .collect();

    let mc_s: HashSet<i32> = mc
        .column("movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(mc.column("company_id")?.i32()?.into_no_null_iter())
        .filter_map(|(movie_id, company_id)| cn_s.contains(&company_id).then_some(movie_id))
        .collect();

    let mut res: Option<(&str, &str)> = None;

    for ((movie_id, person_id), role_id) in ci
        .column("movie_id")?
        .i32()?
        .into_no_null_iter()
        .zip(ci.column("person_id")?.i32()?.into_no_null_iter())
        .zip(ci.column("role_id")?.i32()?.into_no_null_iter())
    {
        if rt_s.contains(&role_id)
            && n_s.contains(&person_id)
            && mc_s.contains(&movie_id)
            && let Some(name) = an_m.get(&person_id)
            && let Some(title) = t_m.get(&movie_id)
        {
            res = match res {
                Some((old_name, old_title)) => Some((
                    name.iter().min().unwrap().min(&old_name),
                    title.min(&old_title),
                )),
                None => Some((name.iter().min().unwrap(), title)),
            };
        }
    }

    println!("8c,{:}", start.elapsed().as_secs_f32());

    Ok(res)
}

// -- JOB Query 8c
// SELECT MIN(an1.name) AS costume_designer_pseudo, MIN(t.title) AS movie_with_costumes
// FROM aka_name AS an1,
// cast_info AS ci,
// company_name AS cn,
// movie_companies AS mc,
// name AS n1,
// role_type AS rt,
// title AS t
// WHERE cn.country_code = '[us]'
// AND rt.role = 'writer'
// AND an1.person_id = n1.id
// AND n1.id = ci.person_id
// AND ci.movie_id = t.id
// AND t.id = mc.movie_id
// AND mc.company_id = cn.id
// AND ci.role_id = rt.id
// AND an1.person_id = ci.person_id
// AND ci.movie_id = mc.movie_id;
#[cfg(test)]
mod test_8c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q8c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q8c(&db)?;
        assert_eq!(res, Some(("\"A.J.\"", "#1 Cheerleader Camp")));
        Ok(())
    }
}
