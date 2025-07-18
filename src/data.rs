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
        let mi_idx = ParquetReader::new(std::fs::File::open("imdb/movie_info_idx.parquet").unwrap())
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
