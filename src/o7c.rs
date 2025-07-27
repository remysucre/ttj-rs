use crate::data::ImdbData;
use ahash::HashMap;
use ahash::HashSet;
use polars::prelude::*;
use std::time::Instant;

pub fn q7c(db: &ImdbData) -> Result<Option<(&str, &str)>, PolarsError> {
    // let an = &db.an;
    let ci = &db.ci;
    let it = &db.it;
    let lt = &db.lt;
    let ml = &db.ml;
    let n = &db.n;
    let pi = &db.pi;
    let t = &db.t;

    let start = Instant::now();

    let lt_s: Vec<i32> = lt
        .column("id")?
        .i32()?
        .into_iter()
        .zip(lt.column("link")?.str()?)
        .filter_map(|(id, link)| {
            if let (Some(id), Some(link)) = (id, link) {
                if matches!(
                    link,
                    "references" | "referenced in" | "features" | "featured in"
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

    let it_s: Vec<i32> = it
        .column("id")?
        .i32()?
        .into_iter()
        .zip(it.column("info")?.str()?)
        .filter_map(|(id, info)| {
            if let (Some(id), Some(info)) = (id, info) {
                if info == "mini biography" {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let ml_s: HashSet<i32> = ml
        .column("linked_movie_id")?
        .i32()?
        .into_iter()
        .zip(ml.column("link_type_id")?.i32()?)
        .filter_map(|(id, link_type_id)| {
            if let (Some(id), Some(link_type_id)) = (id, link_type_id) {
                if lt_s.contains(&link_type_id) {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let pi_m: HashMap<i32, Vec<&str>> = pi
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(pi.column("info_type_id")?.i32()?)
        .zip(pi.column("info")?.str()?)
        .filter_map(|((id, info_type_id), info)| {
            if let (Some(id), Some(info_type_id), Some(info)) = (id, info_type_id, info) {
                if it_s.contains(&info_type_id) {
                    Some((id, info))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .fold(HashMap::default(), |mut acc, (id, info)| {
            acc.entry(id).or_default().push(info);
            acc
        });

    let t_s: HashSet<i32> = t
        .column("id")?
        .i32()?
        .into_iter()
        .zip(t.column("production_year")?.i32()?)
        .filter_map(|(id, production_year)| {
            if let (Some(id), Some(production_year)) = (id, production_year) {
                if (1980..=2010).contains(&production_year) && ml_s.contains(&id) {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let n_m: HashMap<i32, &str> = n
        .column("id")?
        .i32()?
        .into_iter()
        .zip(n.column("name")?.str()?)
        .zip(n.column("name_pcode_cf")?.str()?)
        .zip(n.column("gender")?.str()?)
        .fold(
            HashMap::default(),
            |mut acc, (((id, name), name_pcode), gender)| {
                if let (Some(id), Some(name), Some(name_pcode), Some(gender)) =
                    (id, name, name_pcode, gender)
                {
                    if pi_m.contains_key(&id)
                        && (("A"..="F").contains(&name_pcode) && gender == "m"
                            || gender == "f" && name.starts_with('A'))
                    {
                        acc.insert(id, name);
                        acc
                    } else {
                        acc
                    }
                } else {
                    acc
                }
            },
        );

    // let mut an_s: HashSet<i32> = HashSet::default();
    //
    // let an_id_col = an.column("person_id")?.i32()?;
    // let an_name_col = an.column("name")?.str()?;
    //
    // for (id, name) in an_id_col.into_iter().zip(an_name_col.into_iter()) {
    //     if let (Some(id), Some(name)) = (id, name) {
    //         if n_m.contains_key(&id) && (name.contains('a') || name.starts_with('A')) {
    //             an_s.insert(id);
    //         }
    //     }
    // }

    let mut res: Option<(&str, &str)> = None;

    for (pid, mid) in ci
        .column("person_id")?
        .i32()?
        .into_iter()
        .zip(ci.column("movie_id")?.i32()?.into_iter())
    {
        if let (Some(pid), Some(mid)) = (pid, mid) {
            if let Some(info) = pi_m.get(&pid)
                && let Some(name) = n_m.get(&pid)
                && t_s.contains(&mid)
            {
                for info in info {
                    if let Some((old_name, old_info)) = res.as_mut() {
                        if name < old_name {
                            *old_name = name;
                        }
                        if info < old_info {
                            *old_info = info;
                        }
                    } else {
                        res = Some((name, info));
                    }
                }
            }
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("7c,{duration:}");

    Ok(res)
}

// -- JOB Query 7c
// SELECT MIN(n.name) AS cast_member_name, MIN(pi.info) AS cast_member_info
// FROM aka_name AS an,
// cast_info AS ci,
// info_type AS it,
// link_type AS lt,
// movie_link AS ml,
// name AS n,
// person_info AS pi,
// title AS t
// WHERE an.name is not NULL
// and (an.name LIKE '%a%' or an.name LIKE 'A%')
// AND it.info = 'mini biography'
// AND lt.link in ('references', 'referenced in', 'features', 'featured in')
// AND n.name_pcode_cf BETWEEN 'A' AND 'F'
// AND (n.gender = 'm' OR (n.gender = 'f' AND n.name LIKE 'A%'))
// AND pi.note is not NULL
// AND t.production_year BETWEEN 1980 AND 2010
// AND n.id = an.person_id
// AND n.id = pi.person_id
// AND ci.person_id = n.id
// AND t.id = ci.movie_id
// AND ml.linked_movie_id = t.id
// AND lt.id = ml.link_type_id
// AND it.id = pi.info_type_id
// AND pi.person_id = an.person_id
// AND pi.person_id = ci.person_id
// AND an.person_id = ci.person_id
// AND ci.movie_id = ml.linked_movie_id;

#[cfg(test)]
mod test_q7c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q7c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let res = q7c(&db)?;
        assert_eq!(
            res,
            Some((
                "50 Cent",
                "\"Boo\" Arnold was born Earl Arnold in Hattiesburg, Mississippi in 1966. His father gave him the nickname 'Boo' early in life and it stuck through grade school, high school, and college. He is still known as \"Boo\" to family and friends.  Raised in central Texas, Arnold played baseball at Texas Tech University where he graduated with a BA in Advertising and Marketing. While at Texas Tech he was also a member of the Texas Epsilon chapter of Phi Delta Theta fraternity. After college he worked with Young Life, an outreach to high school students, in San Antonio, Texas.  While with Young Life Arnold began taking extension courses through Fuller Theological Seminary and ultimately went full-time to Gordon-Conwell Theological Seminary in Boston, Massachusetts. At Gordon-Conwell he completed a Master's Degree in Divinity studying Theology, Philosophy, Church History, Biblical Languages (Hebrew & Greek), and Exegetical Methods. Following seminary he was involved with reconciliation efforts in the former Yugoslavia shortly after the war ended there in1995.  Arnold started acting in his early thirties in Texas. After an encouraging visit to Los Angeles where he spent time with childhood friend George Eads (of CSI Las Vegas) he decided to move to Los Angeles in 2001 to pursue acting full-time. While in Los Angeles he has studied acting with Judith Weston at Judith Weston Studio for Actors and Directors.  Arnold's acting career has been one of steady development, booking co-star and guest-star roles in nighttime television. He guest-starred opposite of Jane Seymour on the night time television drama Justice. He played the lead, Michael Hollister, in the film The Seer, written and directed by Patrick Masset (Friday Night Lights).  He was nominated Best Actor in the168 Film Festival for the role of Phil Stevens in the short-film Useless. In Useless he played a US Marshal who must choose between mercy and justice as he confronts the man who murdered his father. Arnold's performance in Useless confirmed his ability to carry lead roles, and he continues to work toward solidifying himself as a male lead in film and television.  Arnold married fellow Texan Stacy Rudd of San Antonio in 2003 and they are now raising their three children in the Los Angeles area.",
            ))
        );
        Ok(())
    }
}
