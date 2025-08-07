use crate::data::Data;
use memchr::memmem::Finder;
use polars::prelude::*;
use std::time::Instant;

pub fn q17c(db: &Data) -> Result<Option<(&str, &str)>, PolarsError> {
    let ci = &db.ci;
    let k = &db.k;
    let mk = &db.mk;
    let n = &db.n;
    // let cn = &db.cn;
    let mc = &db.mc;
    // let t = &db.t;

    let x = Finder::new("X");
    // FK-PK optimization: FK is ci.movie_id and PK is title.id
    // let t_s: HashSet<i32> = t
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .flatten()
    //     .collect();

    // let cn_s: HashSet<i32> = cn
    //     .column("id")?
    //     .i32()?
    //     .into_iter()
    //     .flatten()
    //     .collect();

    let start = Instant::now();

    let k_id = k
        .keyword
        .iter()
        .zip(k.id.iter())
        .find(|(keyword, _)| *keyword == "character-name-in-title")
        .map(|(_, id)| id)
        .unwrap();

    let mk_s: ahash::HashSet<i32> = mk
        .keyword_id
        .iter()
        .zip(mk.movie_id.iter())
        .filter_map(|(keyword_id, movie_id)| (keyword_id == k_id).then_some(*movie_id))
        .collect();

    let mc_s: ahash::HashSet<i32> = mc
        .movie_id
        .iter()
        .filter_map(|movie_id| mk_s.contains(&movie_id).then_some(*movie_id))
        .collect();

    let n_m: ahash::HashMap<&i32, &str> =
        n.id.iter()
            .zip(n.name.iter())
            .filter_map(|(id, name)| {
                (x.find(name.as_bytes()) == Some(0)).then_some((id, name.as_str()))
            })
            .collect();

    let res = ci
        .person_id
        .iter()
        .zip(ci.movie_id.iter())
        .filter_map(|(person_id, movie_id)| {
            // Check if this person has a "Bert" name and the movie is valid
            n_m.get(&person_id)
                .filter(|_| mc_s.contains(&movie_id))
                .copied()
        })
        .min();

    println!("17c,{:}", start.elapsed().as_secs_f32());

    Ok(res.map(|name| (name, name)))
}

// -- JOB Query 17c
// SELECT MIN(n.name) AS member_in_charnamed_movie, MIN(n.name) AS a1
// FROM cast_info AS ci,
// company_name AS cn,
// keyword AS k,
// movie_companies AS mc,
// movie_keyword AS mk,
// name AS n,
// title AS t
// WHERE k.keyword = 'character-name-in-title'
// AND n.name LIKE 'X%'
// AND n.id = ci.person_id
// AND ci.movie_id = t.id
// AND t.id = mk.movie_id
// AND mk.keyword_id = k.id
// AND t.id = mc.movie_id
// AND mc.company_id = cn.id
// AND ci.movie_id = mc.movie_id
// AND ci.movie_id = mk.movie_id
// AND mc.movie_id = mk.movie_id;
#[cfg(test)]
mod test_q17c {
    use super::*;
    use crate::data::ImdbData;

    #[test]
    fn test_q17c() -> Result<(), PolarsError> {
        let db = ImdbData::new();
        let data = Data::new(&db);
        let res = q17c(&data)?;
        assert_eq!(res, Some(("X'Volaitis, John", "X'Volaitis, John")));
        Ok(())
    }
}
