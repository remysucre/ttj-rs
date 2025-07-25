use crate::data::Data;
use ahash::HashMap;
use ahash::HashSet;
use memchr::memmem;
use polars::prelude::*;
use std::time::Instant;

//  imdb.q24a_cast_info(person_id,movie_id,person_role_id,role_id)
//  |imdb.q24a_title(movie_id,kind_id)
//  |imdb.q24a_name(person_id)
//  ||imdb_int.aka_name(person_id)
//  |imdb.q24a_role_type(role_id)
//  |imdb_int.char_name(person_role_id)
//  |imdb.q24a_movie_info(movie_id,info_type_id)
//  ||imdb.q24a_info_type(info_type_id)
//  ||imdb_int.movie_keyword(movie_id,keyword_id)
//  |||imdb.q24a_keyword(keyword_id)
//  ||imdb_int.movie_companies(movie_id,company_id,company_type_id)
//  |||imdb.q24a_company_name(company_id)
pub fn q24a(db: &Data) -> Result<Option<(&str, &str, &str)>, PolarsError> {
    let ci = &db.ci;
    let t = &db.t;
    let n = &db.n;
    let rt = &db.rt;
    let chn = &db.chn;
    let mi = &db.mi;
    let it = &db.it;
    let mc = &db.mc;
    let cn = &db.cn;
    let mk = &db.mk;
    let k = &db.k;
    let an = &db.an;

    let an_s: HashSet<i32> = an.person_id.iter().map(|id| *id).collect();

    let japan = memmem::Finder::new("Japan:");
    let usa = memmem::Finder::new("USA:");
    let two_o_one = memmem::Finder::new("201");
    let an_finder = memmem::Finder::new("An");

    let start = Instant::now();

    let chn_m: HashMap<i32, Vec<&str>> =
        chn.id
            .iter()
            .zip(chn.name.iter())
            .fold(HashMap::default(), |mut acc, (id, name)| {
                acc.entry(*id).or_default().push(name);
                acc
            });

    let cn_s: HashSet<i32> = cn
        .country_code
        .iter()
        .zip(cn.id.iter())
        .filter_map(|(country_code, id)| {
            country_code
                .as_ref()
                .filter(|country_code| country_code == &"[us]")
                .map(|_| *id)
        })
        .collect();

    let it_id = it
        .info
        .iter()
        .zip(it.id.iter())
        .find(|(info, _)| *info == "release dates")
        .map(|(_, id)| *id)
        .unwrap();

    let target_keywords: HashSet<&str> = ["hero", "martial-arts", "hand-to-hand-combat"]
        .into_iter()
        .collect();

    let k_s: HashSet<i32> =
        k.id.iter()
            .zip(k.keyword.iter())
            .filter_map(|(id, keyword)| target_keywords.contains(keyword.as_str()).then_some(*id))
            .collect();

    let t_m: HashMap<i32, Vec<&str>> =
        t.id.iter()
            .zip(t.title.iter())
            .zip(t.production_year.iter())
            .filter_map(|((id, title), production_year)| {
                production_year
                    .filter(|production_year| production_year > &2010)
                    .map(|_| (*id, title))
            })
            .fold(HashMap::default(), |mut acc, (id, title)| {
                acc.entry(id).or_default().push(title);
                acc
            });

    let mi_s: HashSet<i32> = mi
        .movie_id
        .iter()
        .zip(mi.info.iter())
        .zip(mi.info_type_id.iter())
        .filter_map(|((movie_id, info), info_type_id)| {
            let info_bytes = info.as_bytes();
            (it_id == *info_type_id
                && t_m.contains_key(movie_id)
                && (japan.find(info_bytes) == Some(0) || usa.find(info_bytes) == Some(0))
                && two_o_one.find(info_bytes).is_some())
            .then_some(*movie_id)
        })
        .collect::<HashSet<_>>();

    let n_m: HashMap<i32, Vec<&str>> = n
        .gender
        .iter()
        .zip(n.id.iter())
        .zip(n.name.iter())
        .filter_map(|((gender, id), name)| {
            gender
                .as_ref()
                .filter(|gender| {
                    gender == &"f"
                        && an_s.contains(&id)
                        && an_finder.find(name.as_bytes()).is_some()
                })
                .map(|_| (*id, name))
        })
        .fold(HashMap::default(), |mut acc, (id, name)| {
            acc.entry(id).or_default().push(name);
            acc
        });

    let rt_s: HashSet<i32> = rt
        .id
        .iter()
        .zip(rt.role.iter())
        .filter_map(|(id, role)| (role == "actress").then_some(*id))
        .collect();

    let mk_s: HashSet<i32> = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| {
            (k_s.contains(&keyword_id) && mi_s.contains(&movie_id)).then_some(*movie_id)
        })
        .collect();

    let mc_s: HashSet<i32> = mc
        .company_id
        .iter()
        .zip(mc.movie_id.iter())
        .filter_map(|(company_id, movie_id)| {
            (cn_s.contains(&company_id) && mk_s.contains(&movie_id)).then_some(*movie_id)
        })
        .collect();

    let mut res: Option<(&str, &str, &str)> = None;

    let target_note: HashSet<&str> = [
        "(voice)",
        "(voice: Japanese version)",
        "(voice) (uncredited)",
        "(voice: English version)",
    ]
    .into_iter()
    .collect();

    for ((((movie_id, person_id), person_role_id), role_id), note) in ci
        .movie_id
        .iter()
        .zip(ci.person_id.iter())
        .zip(ci.person_role_id.iter())
        .zip(ci.role_id.iter())
        .zip(ci.note.iter())
    {
        if let Some(person_role_id) = person_role_id
            && let Some(note) = note
            && rt_s.contains(&role_id)
            && target_note.contains(note.as_str())
            && mc_s.contains(&movie_id)
            && let Some(titles) = t_m.get(&movie_id)
            && let Some(names) = n_m.get(&person_id)
            && let Some(char_names) = chn_m.get(&person_role_id)
        {
            res = match res {
                Some((old_char_name, old_name, old_title)) => Some((
                    char_names.iter().min().unwrap().min(&old_char_name),
                    names.iter().min().unwrap().min(&old_name),
                    titles.iter().min().unwrap().min(&old_title),
                )),
                None => Some((
                    char_names.iter().min().unwrap(),
                    names.iter().min().unwrap(),
                    titles.iter().min().unwrap(),
                )),
            };
        }
    }

    let duration = start.elapsed().as_secs_f32();
    println!("24a,{duration:}");

    Ok(res)
}

// -- JOB Query 24a
// SELECT MIN(chn.name) AS voiced_char_name,
//        MIN(n.name) AS voicing_actress_name,
//        MIN(t.title) AS voiced_action_movie_jap_eng
// FROM aka_name AS an,
//      char_name AS chn,
//      cast_info AS ci,
//      company_name AS cn,
//      info_type AS it,
//      keyword AS k,
//      movie_companies AS mc,
//      movie_info AS mi,
//      movie_keyword AS mk,
//      name AS n,
//      role_type AS rt,
//      title AS t
// WHERE ci.note IN ('(voice)',
//                   '(voice: Japanese version)',
//                   '(voice) (uncredited)',
//                   '(voice: English version)')
//   AND cn.country_code ='[us]'
//   AND it.info = 'release dates'
//   AND k.keyword IN ('hero',
//                     'martial-arts',
//                     'hand-to-hand-combat')
//   AND mi.info IS NOT NULL
//   AND (mi.info LIKE 'Japan:%201%'
//        OR mi.info LIKE 'USA:%201%')
//   AND n.gender ='f'
//   AND n.name LIKE '%An%'
//   AND rt.role ='actress'
//   AND t.production_year > 2010
//   AND t.id = mi.movie_id
//   AND t.id = mc.movie_id
//   AND t.id = ci.movie_id
//   AND t.id = mk.movie_id
//   AND mc.movie_id = ci.movie_id
//   AND mc.movie_id = mi.movie_id
//   AND mc.movie_id = mk.movie_id
//   AND mi.movie_id = ci.movie_id
//   AND mi.movie_id = mk.movie_id
//   AND ci.movie_id = mk.movie_id
//   AND cn.id = mc.company_id
//   AND it.id = mi.info_type_id
//   AND n.id = ci.person_id
//   AND rt.id = ci.role_id
//   AND n.id = an.person_id
//   AND ci.person_id = an.person_id
//   AND chn.id = ci.person_role_id
//   AND k.id = mk.keyword_id;
#[cfg(test)]
mod test_24a {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q24a() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q24a(&data)?;
        assert_eq!(
            res,
            Some(("Additional Voices", "Baker, Andrea", "Baiohazâdo 6"))
        );
        Ok(())
    }
}
