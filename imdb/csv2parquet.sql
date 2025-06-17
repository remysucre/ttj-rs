CREATE TABLE aka_name (
    id integer NOT NULL PRIMARY KEY,
    person_id integer NOT NULL,
    name text NOT NULL,
    imdb_index character varying(12),
    name_pcode_cf character varying(5),
    name_pcode_nf character varying(5),
    surname_pcode character varying(5),
    md5sum character varying(32)
);

CREATE TABLE aka_title (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL,
    title text NOT NULL,
    imdb_index character varying(12),
    kind_id integer NOT NULL,
    production_year integer,
    phonetic_code character varying(5),
    episode_of_id integer,
    season_nr integer,
    episode_nr integer,
    note text,
    md5sum character varying(32)
);

CREATE TABLE cast_info (
    id integer NOT NULL PRIMARY KEY,
    person_id integer NOT NULL,
    movie_id integer NOT NULL,
    person_role_id integer,
    note text,
    nr_order integer,
    role_id integer NOT NULL
);

CREATE TABLE char_name (
    id integer NOT NULL PRIMARY KEY,
    name text NOT NULL,
    imdb_index character varying(12),
    imdb_id integer,
    name_pcode_nf character varying(5),
    surname_pcode character varying(5),
    md5sum character varying(32)
);

CREATE TABLE comp_cast_type (
    id integer NOT NULL PRIMARY KEY,
    kind character varying(32) NOT NULL
);

CREATE TABLE company_name (
    id integer NOT NULL PRIMARY KEY,
    name text NOT NULL,
    country_code character varying(255),
    imdb_id integer,
    name_pcode_nf character varying(5),
    name_pcode_sf character varying(5),
    md5sum character varying(32)
);

CREATE TABLE company_type (
    id integer NOT NULL PRIMARY KEY,
    kind character varying(32) NOT NULL
);

CREATE TABLE complete_cast (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer,
    subject_id integer NOT NULL,
    status_id integer NOT NULL
);

CREATE TABLE info_type (
    id integer NOT NULL PRIMARY KEY,
    info character varying(32) NOT NULL
);

CREATE TABLE keyword (
    id integer NOT NULL PRIMARY KEY,
    keyword text NOT NULL,
    phonetic_code character varying(5)
);

CREATE TABLE kind_type (
    id integer NOT NULL PRIMARY KEY,
    kind character varying(15) NOT NULL
);

CREATE TABLE link_type (
    id integer NOT NULL PRIMARY KEY,
    link character varying(32) NOT NULL
);

CREATE TABLE movie_companies (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL,
    company_id integer NOT NULL,
    company_type_id integer NOT NULL,
    note text
);

CREATE TABLE movie_info (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL,
    info_type_id integer NOT NULL,
    info text NOT NULL,
    note text
);

CREATE TABLE movie_info_idx (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL,
    info_type_id integer NOT NULL,
    info text NOT NULL,
    note text
);

CREATE TABLE movie_keyword (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL,
    keyword_id integer NOT NULL
);

CREATE TABLE movie_link (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL,
    linked_movie_id integer NOT NULL,
    link_type_id integer NOT NULL
);

CREATE TABLE name (
    id integer NOT NULL PRIMARY KEY,
    name text NOT NULL,
    imdb_index character varying(12),
    imdb_id integer,
    gender character varying(1),
    name_pcode_cf character varying(5),
    name_pcode_nf character varying(5),
    surname_pcode character varying(5),
    md5sum character varying(32)
);

CREATE TABLE person_info (
    id integer NOT NULL PRIMARY KEY,
    person_id integer NOT NULL,
    info_type_id integer NOT NULL,
    info text NOT NULL,
    note text
);

CREATE TABLE role_type (
    id integer NOT NULL PRIMARY KEY,
    role character varying(32) NOT NULL
);

CREATE TABLE title (
    id integer NOT NULL PRIMARY KEY,
    title text NOT NULL,
    imdb_index character varying(12),
    kind_id integer NOT NULL,
    production_year integer,
    imdb_id integer,
    phonetic_code character varying(5),
    episode_of_id integer,
    season_nr integer,
    episode_nr integer,
    series_years character varying(49),
    md5sum character varying(32)
);

copy aka_name from 'aka_name.csv' escape '\';
copy aka_title from 'aka_title.csv' escape '\';
copy cast_info from 'cast_info.csv' escape '\';
copy char_name from 'char_name.csv' escape '\';
copy comp_cast_type from 'comp_cast_type.csv' escape '\';
copy company_name from 'company_name.csv' escape '\';
copy company_type from 'company_type.csv' escape '\';
copy complete_cast from 'complete_cast.csv' escape '\';
copy info_type from 'info_type.csv' escape '\';
copy keyword from 'keyword.csv' escape '\';
copy kind_type from 'kind_type.csv' escape '\';
copy link_type from 'link_type.csv' escape '\';
copy movie_companies from 'movie_companies.csv' escape '\';
copy movie_info from 'movie_info.csv' escape '\';
copy movie_info_idx from 'movie_info_idx.csv' escape '\';
copy movie_keyword from 'movie_keyword.csv' escape '\';
copy movie_link from 'movie_link.csv' escape '\';
copy name from 'name.csv' escape '\';
copy person_info from 'person_info.csv' escape '\';
copy role_type from 'role_type.csv' escape '\';
copy title from 'title.csv' escape '\';

copy aka_name to 'aka_name.parquet' (format parquet);
copy aka_title to 'aka_title.parquet' (format parquet);
copy cast_info to 'cast_info.parquet' (format parquet);
copy char_name to 'char_name.parquet' (format parquet);
copy comp_cast_type to 'comp_cast_type.parquet' (format parquet);
copy company_name to 'company_name.parquet' (format parquet);
copy company_type to 'company_type.parquet' (format parquet);
copy complete_cast to 'complete_cast.parquet' (format parquet);
copy info_type to 'info_type.parquet' (format parquet);
copy keyword to 'keyword.parquet' (format parquet);
copy kind_type to 'kind_type.parquet' (format parquet);
copy link_type to 'link_type.parquet' (format parquet);
copy movie_companies to 'movie_companies.parquet' (format parquet);
copy movie_info to 'movie_info.parquet' (format parquet);
copy movie_info_idx to 'movie_info_idx.parquet' (format parquet);
copy movie_keyword to 'movie_keyword.parquet' (format parquet);
copy movie_link to 'movie_link.parquet' (format parquet);
copy name to 'name.parquet' (format parquet);
copy person_info to 'person_info.parquet' (format parquet);
copy role_type to 'role_type.parquet' (format parquet);
copy title to 'title.parquet' (format parquet);
