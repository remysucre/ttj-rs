use polars::prelude::*;

pub struct ImdbData {
    pub an: DataFrame,
    pub at: DataFrame,
    pub ci: DataFrame,
    pub chn: DataFrame,
    pub cct: DataFrame,
    pub cn: DataFrame,
    pub ct: DataFrame,
    pub cc: DataFrame,
    pub it: DataFrame,
    pub k: DataFrame,
    pub kt: DataFrame,
    pub lt: DataFrame,
    pub mc: DataFrame,
    pub mi_idx: DataFrame,
    pub mi: DataFrame,
    pub mk: DataFrame,
    pub ml: DataFrame,
    pub n: DataFrame,
    pub pi: DataFrame,
    pub rt: DataFrame,
    pub t: DataFrame,
}

// CREATE TABLE cast_info (
//     id integer NOT NULL PRIMARY KEY,
//     person_id integer NOT NULL,
//     movie_id integer NOT NULL,
//     person_role_id integer,
//     note text,
//     nr_order integer,
//     role_id integer NOT NULL
// );

pub struct CI {
    pub id: Vec<i32>,
    pub person_id: Vec<i32>,
    pub movie_id: Vec<i32>,
    pub person_role_id: Vec<Option<i32>>,
    pub note: Vec<Option<String>>,
    pub nr_order: Vec<Option<i32>>,
    pub role_id: Vec<i32>,
}

// CREATE TABLE char_name (
//     id integer NOT NULL PRIMARY KEY,
//     name text NOT NULL,
//     imdb_index character varying(12),
//     imdb_id integer,
//     name_pcode_nf character varying(5),
//     surname_pcode character varying(5),
//     md5sum character varying(32)
// );

pub struct CHN {
    pub id: Vec<i32>,
    pub name: Vec<String>,
    pub imdb_index: Vec<Option<String>>,
    pub imdb_id: Vec<Option<i32>>,
    pub name_pcode_nf: Vec<Option<String>>,
    pub surname_pcode: Vec<Option<String>>,
    pub md5sum: Vec<Option<String>>,
}

// CREATE TABLE title (
//     id integer NOT NULL PRIMARY KEY,
//     title text NOT NULL,
//     imdb_index character varying(12),
//     kind_id integer NOT NULL,
//     production_year integer,
//     imdb_id integer,
//     phonetic_code character varying(5),
//     episode_of_id integer,
//     season_nr integer,
//     episode_nr integer,
//     series_years character varying(49),
//     md5sum character varying(32)
// );

pub struct T {
    pub id: Vec<i32>,
    pub title: Vec<String>,
    pub imdb_index: Vec<Option<String>>,
    pub kind_id: Vec<i32>,
    pub production_year: Vec<Option<i32>>,
    pub imdb_id: Vec<Option<i32>>,
    pub phonetic_code: Vec<Option<String>>,
    pub episode_of_id: Vec<Option<i32>>,
    pub season_nr: Vec<Option<i32>>,
    pub episode_nr: Vec<Option<i32>>,
    pub series_years: Vec<Option<String>>,
    pub md5sum: Vec<Option<String>>,
}

// CREATE TABLE kind_type (
//     id integer NOT NULL PRIMARY KEY,
//     kind character varying(15) NOT NULL
// );

pub struct KT {
    pub id: Vec<i32>,
    pub kind: Vec<String>,
}

// CREATE TABLE name (
//     id integer NOT NULL PRIMARY KEY,
//     name text NOT NULL,
//     imdb_index character varying(12),
//     imdb_id integer,
//     gender character varying(1),
//     name_pcode_cf character varying(5),
//     name_pcode_nf character varying(5),
//     surname_pcode character varying(5),
//     md5sum character varying(32)
// );

pub struct N {
    pub id: Vec<i32>,
    pub name: Vec<String>,
    pub imdb_index: Vec<Option<String>>,
    pub imdb_id: Vec<Option<i32>>,
    pub gender: Vec<Option<String>>,
    pub name_pcode_cf: Vec<Option<String>>,
    pub name_pcode_nf: Vec<Option<String>>,
    pub surname_pcode: Vec<Option<String>>,
    pub md5sum: Vec<Option<String>>,
}

// CREATE TABLE complete_cast (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer,
//     subject_id integer NOT NULL,
//     status_id integer NOT NULL
// );

pub struct CC {
    pub id: Vec<i32>,
    pub movie_id: Vec<Option<i32>>,
    pub subject_id: Vec<i32>,
    pub status_id: Vec<i32>,
}

// CREATE TABLE comp_cast_type (
//     id integer NOT NULL PRIMARY KEY,
//     kind character varying(32) NOT NULL
// );

pub struct CCT {
    pub id: Vec<i32>,
    pub kind: Vec<String>,
}

// CREATE TABLE movie_keyword (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer NOT NULL,
//     keyword_id integer NOT NULL
// );

pub struct MK {
    pub id: Vec<i32>,
    pub movie_id: Vec<i32>,
    pub keyword_id: Vec<i32>,
}

// CREATE TABLE keyword (
//     id integer NOT NULL PRIMARY KEY,
//     keyword text NOT NULL,
//     phonetic_code character varying(5)
// );

pub struct K {
    pub id: Vec<i32>,
    pub keyword: Vec<String>,
    pub phonetic_code: Vec<Option<String>>,
}

// CREATE TABLE aka_name (
//      id integer NOT NULL,
//      person_id integer NOT NULL,
//      name text NOT NULL,
//      imdb_index character varying(12),
//      name_pcode_cf character varying(5),
//      name_pcode_nf character varying(5),
//      surname_pcode character varying(5),
//      md5sum character varying(32)
//      --FOREIGN KEY (person_id) REFERENCES name (id)
// );

pub struct AN {
    pub id: Vec<i32>,
    pub person_id: Vec<i32>,
    pub name: Vec<String>,
    pub imdb_index: Vec<Option<String>>,
    pub name_pcode_cf: Vec<Option<String>>,
    pub name_pcode_nf: Vec<Option<String>>,
    pub surname_pcode: Vec<Option<String>>,
    pub md5sum: Vec<Option<String>>,
}

// CREATE TABLE role_type (
//      id integer primary key ,
//      role character varying(32) NOT NULL
// );

pub struct RT {
    pub id: Vec<i32>,
    pub role: Vec<String>,
}

// CREATE TABLE movie_companies (
//      id integer NOT NULL,
//      movie_id integer NOT NULL,
//      company_id integer NOT NULL,
//      company_type_id integer NOT NULL,
//      note text
// --   FOREIGN KEY (company_id) REFERENCES company_name(id),
// --   FOREIGN KEY (movie_id) REFERENCES title (id),
// --   FOREIGN KEY (company_type_id) REFERENCES company_type(id)
// );
pub struct MC {
    pub id: Vec<i32>,
    pub movie_id: Vec<i32>,
    pub company_id: Vec<i32>,
    pub company_type_id: Vec<i32>,
    pub note: Vec<Option<String>>,
}

// CREATE TABLE company_name (
//      id integer primary key,
//      name text NOT NULL,
//      country_code character varying(255),
//      imdb_id integer,
//      name_pcode_nf character varying(5),
//      name_pcode_sf character varying(5),
//      md5sum character varying(32)
// );
pub struct CN {
    pub id: Vec<i32>,
    pub name: Vec<String>,
    pub country_code: Vec<Option<String>>,
    pub imdb_id: Vec<Option<i32>>,
    pub name_pcode_nf: Vec<Option<String>>,
    pub name_pcode_sf: Vec<Option<String>>,
    pub md5sum: Vec<Option<String>>,
}

pub struct Data {
    pub ci: CI,
    pub chn: CHN,
    pub t: T,
    pub kt: KT,
    pub n: N,
    pub cc: CC,
    pub cct: CCT,
    pub mk: MK,
    pub k: K,
    pub an: AN,
    pub rt: RT,
    pub mc: MC,
    pub cn: CN,
}

impl Data {
    pub fn new(imdb: &ImdbData) -> Self {
        Data {
            ci: CI {
                id: imdb
                    .ci
                    .column("id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                person_id: imdb
                    .ci
                    .column("person_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                movie_id: imdb
                    .ci
                    .column("movie_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                person_role_id: imdb
                    .ci
                    .column("person_role_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_iter()
                    .collect(),
                note: imdb
                    .ci
                    .column("note")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                nr_order: imdb
                    .ci
                    .column("nr_order")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_iter()
                    .collect(),
                role_id: imdb
                    .ci
                    .column("role_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
            },
            chn: CHN {
                id: imdb
                    .chn
                    .column("id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                name: imdb
                    .chn
                    .column("name")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|s| s.to_string())
                    .collect(),
                imdb_index: imdb
                    .chn
                    .column("imdb_index")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                imdb_id: imdb
                    .chn
                    .column("imdb_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_iter()
                    .collect(),
                name_pcode_nf: imdb
                    .chn
                    .column("name_pcode_nf")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                surname_pcode: imdb
                    .chn
                    .column("surname_pcode")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                md5sum: imdb
                    .chn
                    .column("md5sum")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
            },
            t: T {
                id: imdb
                    .t
                    .column("id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                title: imdb
                    .t
                    .column("title")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|s| s.to_string())
                    .collect(),
                imdb_index: imdb
                    .t
                    .column("imdb_index")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                kind_id: imdb
                    .t
                    .column("kind_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                production_year: imdb
                    .t
                    .column("production_year")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_iter()
                    .collect(),
                imdb_id: imdb
                    .t
                    .column("imdb_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_iter()
                    .collect(),
                phonetic_code: imdb
                    .t
                    .column("phonetic_code")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                episode_of_id: imdb
                    .t
                    .column("episode_of_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_iter()
                    .collect(),
                season_nr: imdb
                    .t
                    .column("season_nr")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_iter()
                    .collect(),
                episode_nr: imdb
                    .t
                    .column("episode_nr")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_iter()
                    .collect(),
                series_years: imdb
                    .t
                    .column("series_years")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                md5sum: imdb
                    .t
                    .column("md5sum")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
            },
            kt: KT {
                id: imdb
                    .kt
                    .column("id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                kind: imdb
                    .kt
                    .column("kind")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            n: N {
                id: imdb
                    .n
                    .column("id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                name: imdb
                    .n
                    .column("name")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|s| s.to_string())
                    .collect(),
                imdb_index: imdb
                    .n
                    .column("imdb_index")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                imdb_id: imdb
                    .n
                    .column("imdb_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_iter()
                    .collect(),
                gender: imdb
                    .n
                    .column("gender")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                name_pcode_cf: imdb
                    .n
                    .column("name_pcode_cf")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                name_pcode_nf: imdb
                    .n
                    .column("name_pcode_nf")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                surname_pcode: imdb
                    .n
                    .column("surname_pcode")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                md5sum: imdb
                    .n
                    .column("md5sum")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
            },
            cc: CC {
                id: imdb
                    .cc
                    .column("id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                movie_id: imdb
                    .cc
                    .column("movie_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_iter()
                    .collect(),
                subject_id: imdb
                    .cc
                    .column("subject_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                status_id: imdb
                    .cc
                    .column("status_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
            },
            cct: CCT {
                id: imdb
                    .cct
                    .column("id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                kind: imdb
                    .cct
                    .column("kind")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            mk: MK {
                id: imdb
                    .mk
                    .column("id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                movie_id: imdb
                    .mk
                    .column("movie_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                keyword_id: imdb
                    .mk
                    .column("keyword_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
            },
            k: K {
                id: imdb
                    .k
                    .column("id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                keyword: imdb
                    .k
                    .column("keyword")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|s| s.to_string())
                    .collect(),
                phonetic_code: imdb
                    .k
                    .column("phonetic_code")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
            },
            an: AN {
                id: imdb
                    .an
                    .column("id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                person_id: imdb
                    .an
                    .column("person_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                name: imdb
                    .an
                    .column("name")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|name| name.to_string())
                    .collect(),
                imdb_index: imdb
                    .an
                    .column("imdb_index")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|idx| idx.map(|i| i.to_string()))
                    .collect(),
                name_pcode_cf: imdb
                    .an
                    .column("name_pcode_cf")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|idx| idx.map(|i| i.to_string()))
                    .collect(),
                name_pcode_nf: imdb
                    .an
                    .column("name_pcode_nf")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|idx| idx.map(|i| i.to_string()))
                    .collect(),
                surname_pcode: imdb
                    .an
                    .column("surname_pcode")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|idx| idx.map(|i| i.to_string()))
                    .collect(),
                md5sum: imdb
                    .an
                    .column("md5sum")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|idx| idx.map(|i| i.to_string()))
                    .collect(),
            },
            rt: RT {
                id: imdb
                    .rt
                    .column("id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                role: imdb
                    .rt
                    .column("role")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|name| name.to_string())
                    .collect(),
            },
            mc: MC {
                id: imdb
                    .mc
                    .column("id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                movie_id: imdb
                    .mc
                    .column("movie_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                company_id: imdb
                    .mc
                    .column("company_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                company_type_id: imdb
                    .mc
                    .column("company_type_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                note: imdb
                    .mc
                    .column("note")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|idx| idx.map(|i| i.to_string()))
                    .collect(),
            },
            cn: CN {
                id: imdb
                    .cn
                    .column("id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect(),
                name: imdb
                    .cn
                    .column("name")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|s| s.to_string())
                    .collect(),
                country_code: imdb
                    .cn
                    .column("country_code")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                imdb_id: imdb
                    .cn
                    .column("imdb_id")
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_iter()
                    .collect(),
                name_pcode_nf: imdb
                    .cn
                    .column("name_pcode_nf")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                name_pcode_sf: imdb
                    .cn
                    .column("name_pcode_sf")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                md5sum: imdb
                    .cn
                    .column("md5sum")
                    .unwrap()
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
            },
        }
    }
}

impl Default for ImdbData {
    fn default() -> Self {
        Self::new()
    }
}

impl ImdbData {
    pub fn new() -> Self {
        let an = ParquetReader::new(std::fs::File::open("imdb/aka_name.parquet").unwrap())
            .finish()
            .unwrap();
        let at = ParquetReader::new(std::fs::File::open("imdb/aka_title.parquet").unwrap())
            .finish()
            .unwrap();
        let ci = ParquetReader::new(std::fs::File::open("imdb/cast_info.parquet").unwrap())
            .finish()
            .unwrap();
        let chn = ParquetReader::new(std::fs::File::open("imdb/char_name.parquet").unwrap())
            .finish()
            .unwrap();
        let cct = ParquetReader::new(std::fs::File::open("imdb/comp_cast_type.parquet").unwrap())
            .finish()
            .unwrap();
        let cn = ParquetReader::new(std::fs::File::open("imdb/company_name.parquet").unwrap())
            .finish()
            .unwrap();
        let ct = ParquetReader::new(std::fs::File::open("imdb/company_type.parquet").unwrap())
            .finish()
            .unwrap();
        let cc = ParquetReader::new(std::fs::File::open("imdb/complete_cast.parquet").unwrap())
            .finish()
            .unwrap();
        let it = ParquetReader::new(std::fs::File::open("imdb/info_type.parquet").unwrap())
            .finish()
            .unwrap();
        let k = ParquetReader::new(std::fs::File::open("imdb/keyword.parquet").unwrap())
            .finish()
            .unwrap();
        let kt = ParquetReader::new(std::fs::File::open("imdb/kind_type.parquet").unwrap())
            .finish()
            .unwrap();
        let lt = ParquetReader::new(std::fs::File::open("imdb/link_type.parquet").unwrap())
            .finish()
            .unwrap();
        let mc = ParquetReader::new(std::fs::File::open("imdb/movie_companies.parquet").unwrap())
            .finish()
            .unwrap();
        let mi_idx =
            ParquetReader::new(std::fs::File::open("imdb/movie_info_idx.parquet").unwrap())
                .finish()
                .unwrap();
        let mi = ParquetReader::new(std::fs::File::open("imdb/movie_info.parquet").unwrap())
            .finish()
            .unwrap();
        let mk = ParquetReader::new(std::fs::File::open("imdb/movie_keyword.parquet").unwrap())
            .finish()
            .unwrap();
        let ml = ParquetReader::new(std::fs::File::open("imdb/movie_link.parquet").unwrap())
            .finish()
            .unwrap();
        let n = ParquetReader::new(std::fs::File::open("imdb/name.parquet").unwrap())
            .finish()
            .unwrap();
        let pi = ParquetReader::new(std::fs::File::open("imdb/person_info.parquet").unwrap())
            .finish()
            .unwrap();
        let rt = ParquetReader::new(std::fs::File::open("imdb/role_type.parquet").unwrap())
            .finish()
            .unwrap();
        let t = ParquetReader::new(std::fs::File::open("imdb/title.parquet").unwrap())
            .finish()
            .unwrap();

        ImdbData {
            an,
            at,
            ci,
            chn,
            cct,
            cn,
            ct,
            cc,
            it,
            k,
            kt,
            lt,
            mc,
            mi_idx,
            mi,
            mk,
            ml,
            n,
            pi,
            rt,
            t,
        }
    }
}
