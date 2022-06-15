DROP SCHEMA IF EXISTS public;

CREATE SCHEMA public;

CREATE TABLE vals (
  val_smallint smallint NOT NULL,
  val_integer integer NOT NULL,
  val_bigint bigint NOT NULL,
  val_decimal decimal NOT NULL,
  val_numeric numeric NOT NULL,
  val_numeric_precision numeric(2) NOT NULL,
  val_numeric_precision_scale numeric(2, 1),
  val_real real NOT NULL,
  val_double_precision double precision NOT NULL,
  val_smallserial smallserial NOT NULL,
  val_serial serial NOT NULL,
  val_bigserial bigserial NOT NULL,
  val_money money NOT NULL,
  val_varchar varchar NOT NULL,
  val_varchar_limit varchar(2) NOT NULL,
  val_char char(10) NOT NULL,
  val_text text NOT NULL,
  val_timestamp_without_time_zone timestamp without time zone NOT NULL,
  val_timestamp_without_time_zone_0_precision timestamp(0) without time zone NOT NULL,
  val_timestamp_without_time_zone_1_precision timestamp(1) without time zone NOT NULL,
  val_timestamp_without_time_zone_2_precision timestamp(2) without time zone NOT NULL,
  val_timestamp_without_time_zone_3_precision timestamp(3) without time zone NOT NULL,
  val_timestamp_without_time_zone_4_precision timestamp(4) without time zone NOT NULL,
  val_timestamp_without_time_zone_5_precision timestamp(5) without time zone NOT NULL,
  val_timestamp_without_time_zone_6_precision timestamp(6) without time zone NOT NULL,
  val_timestamp_with_time_zone timestamp with time zone NOT NULL,
  val_timestamp_with_time_zone_0_precision timestamp(0) with time zone NOT NULL,
  val_timestamp_with_time_zone_1_precision timestamp(1) with time zone NOT NULL,
  val_timestamp_with_time_zone_2_precision timestamp(2) with time zone NOT NULL,
  val_timestamp_with_time_zone_3_precision timestamp(3) with time zone NOT NULL,
  val_timestamp_with_time_zone_4_precision timestamp(4) with time zone NOT NULL,
  val_timestamp_with_time_zone_5_precision timestamp(5) with time zone NOT NULL,
  val_timestamp_with_time_zone_6_precision timestamp(6) with time zone NOT NULL,
  val_time_without_time_zone time without time zone NOT NULL,
  val_time_without_time_zone_0_precision time(0) without time zone NOT NULL,
  val_time_without_time_zone_1_precision time(1) without time zone NOT NULL,
  val_time_without_time_zone_2_precision time(2) without time zone NOT NULL,
  val_time_without_time_zone_3_precision time(3) without time zone NOT NULL,
  val_time_without_time_zone_4_precision time(4) without time zone NOT NULL,
  val_time_without_time_zone_5_precision time(5) without time zone NOT NULL,
  val_time_without_time_zone_6_precision time(6) without time zone NOT NULL,
  val_time_with_time_zone time with time zone NOT NULL,
  val_time_with_time_zone_0_precision time(0) with time zone NOT NULL,
  val_time_with_time_zone_1_precision time(1) with time zone NOT NULL,
  val_time_with_time_zone_2_precision time(2) with time zone NOT NULL,
  val_time_with_time_zone_3_precision time(3) with time zone NOT NULL,
  val_time_with_time_zone_4_precision time(4) with time zone NOT NULL,
  val_time_with_time_zone_5_precision time(5) with time zone NOT NULL,
  val_time_with_time_zone_6_precision time(6) with time zone NOT NULL
);

INSERT INTO vals
  VALUES (1, -- smallint
    1, -- integer
    1, -- bigint
    1.2345678901234567890, -- decimal
    1.2345678901234567890, -- numeric
    1.2345678901234567890, -- numeric_precision
    1.2345678901234567890, -- numeric_precision_scale,
    1.2345678901234567890, -- real
    1.2345678901234567890, -- double_precision,
    DEFAULT, -- smallserial
    DEFAULT, -- serial
    DEFAULT, -- bigserial,
    99.98996998, -- money
    'some text', -- varchar
    's', -- varchar limit
    'some text', -- char
    'some text', -- text,
    NOW(), -- timestamp_without_time_zone
    NOW(), -- timestamp_without_time_zone_0_precision
    NOW(), -- timestamp_without_time_zone_1_precision
    NOW(), -- timestamp_without_time_zone_2_precision
    NOW(), -- timestamp_without_time_zone_3_precision
    NOW(), -- timestamp_without_time_zone_4_precision
    NOW(), -- timestamp_without_time_zone_5_precision
    NOW(), -- timestamp_without_time_zone_6_precision
    NOW(), -- timestamp_with_time_zone
    NOW(), -- timestamp_with_time_zone_0_precision
    NOW(), -- timestamp_with_time_zone_1_precision
    NOW(), -- timestamp_with_time_zone_2_precision
    NOW(), -- timestamp_with_time_zone_3_precision
    NOW(), -- timestamp_with_time_zone_4_precision
    NOW(), -- timestamp_with_time_zone_5_precision
    NOW(), -- timestamp_with_time_zone_6_precision
    NOW(), -- time_without_time_zone
    NOW(), -- time_without_time_zone_0_precision
    NOW(), -- time_without_time_zone_1_precision
    NOW(), -- time_without_time_zone_2_precision
    NOW(), -- time_without_time_zone_3_precision
    NOW(), -- time_without_time_zone_4_precision
    NOW(), -- time_without_time_zone_5_precision
    NOW(), -- time_without_time_zone_6_precision
    NOW(), -- time_with_time_zone
    NOW(), -- time_with_time_zone_0_precision
    NOW(), -- time_with_time_zone_1_precision
    NOW(), -- time_with_time_zone_2_precision
    NOW(), -- time_with_time_zone_3_precision
    NOW(), -- time_with_time_zone_4_precision
    NOW(), -- time_with_time_zone_5_precision
    NOW() -- time_with_time_zone_6_precision
);

