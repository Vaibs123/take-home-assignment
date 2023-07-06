create table crime(
    unique_key bigint not null,
    case_number character varying(20),
    date timestamp,
    block character varying(100),
    primary_type character varying(100),
    description character varying(1000),
    location_description character varying(1000),
    arrest bool,
    beat int,
    district int,
    ward int,
    community_area int,
    year int,
    latitude float,
    longitude float,
    primary key (unique_key)
);
