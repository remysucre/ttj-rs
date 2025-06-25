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
        let an = LazyFrame::scan_parquet("imdb/aka_name.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let at = LazyFrame::scan_parquet("imdb/aka_title.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let ci = LazyFrame::scan_parquet("imdb/cast_info.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let chn = LazyFrame::scan_parquet("imdb/char_name.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let cct = LazyFrame::scan_parquet("imdb/comp_cast_type.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let cn = LazyFrame::scan_parquet("imdb/company_name.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let ct = LazyFrame::scan_parquet("imdb/company_type.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let cc = LazyFrame::scan_parquet("imdb/complete_cast.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let it = LazyFrame::scan_parquet("imdb/info_type.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let k = LazyFrame::scan_parquet("imdb/keyword.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let kt = LazyFrame::scan_parquet("imdb/kind_type.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let lt = LazyFrame::scan_parquet("imdb/link_type.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let mc = LazyFrame::scan_parquet("imdb/movie_companies.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let mi_idx = LazyFrame::scan_parquet("imdb/movie_info_idx.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let mi = LazyFrame::scan_parquet("imdb/movie_info.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let mk = LazyFrame::scan_parquet("imdb/movie_keyword.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let ml = LazyFrame::scan_parquet("imdb/movie_link.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let n = LazyFrame::scan_parquet("imdb/name.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let pi = LazyFrame::scan_parquet("imdb/person_info.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let rt = LazyFrame::scan_parquet("imdb/role_type.parquet", Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let t = LazyFrame::scan_parquet("imdb/title.parquet", Default::default())
            .unwrap()
            .collect()
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
