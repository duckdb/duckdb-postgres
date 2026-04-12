create table nulltest (c1 integer, c2 integer, c3 integer, c4 integer, c5 integer, c6 integer, c7 integer, c8 integer, c9 integer, c10 integer);
insert into nulltest values (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
insert into nulltest values (1, NULL, 3, 4, NULL, 6, 7, 8, NULL, 10);
insert into nulltest values (NULL, NULL, 3, 4, 5, 6, 7, NULL, NULL, NULL);
insert into nulltest values (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);


	create type color_t as enum('blue', 'red', 'gray', 'black');

create table cars
 (
   brand   text,
   model   text,
   color   color_t
 );

insert into cars(brand, model, color)
     values ('ferari', 'testarosa', 'red'),
            ('aston martin', 'db2', 'blue'),
            ('bentley', 'mulsanne', 'gray'),
            ('ford', 'T', 'black');

create table intervals as select '42 day'::INTERVAL interval_days UNION ALL SELECT '42 month'::INTERVAL UNION ALL SELECT '42 year'::INTERVAL UNION ALL SELECT  '42 minute'::INTERVAL UNION ALL SELECT  '42 second'::INTERVAL UNION ALL SELECT '0.42 second'::INTERVAL UNION ALL SELECT '-42 day'::INTERVAL interval_days  UNION ALL SELECT NULL::INTERVAL;



CREATE TABLE
  duckdb_arr_test
  (
    id      INTEGER     NOT NULL,
    my_ints INTEGER[]   NOT NULL,
    e INTEGER
  );

INSERT INTO
  duckdb_arr_test
  (
    id,
    my_ints,
    e
  )
VALUES
  (
    123,
    ARRAY[11, 22, 33],
    42
  )
;
INSERT INTO
  duckdb_arr_test
  (
    id,
    my_ints,
    e
  )
VALUES
  (
    234,
    ARRAY[]::INTEGER[],
    42
  )
;
INSERT INTO
  duckdb_arr_test
  (
    id,
    my_ints,
    e
  )
VALUES
  (
    456,
    ARRAY[44, 55, 66],
    42
  )
;


CREATE TABLE oids (i oid);
INSERT INTO oids VALUES (42), (43);



CREATE TABLE daterange (room int, during tsrange);
INSERT INTO daterange VALUES
    (1108, '[2010-01-01 14:30, 2010-01-01 15:30)');


CREATE TABLE daterange_array (room int, during tsrange[]);
INSERT INTO daterange_array VALUES
    (1108, array['[2010-01-01 14:30, 2010-01-01 15:30)'::tsrange]);



    CREATE DOMAIN my_type_v30 AS VARCHAR(30) NOT NULL;

CREATE DOMAIN my_id AS INT4;

CREATE TABLE my_table (
    table_id my_id PRIMARY KEY,
    table_var varchar(10),
    table_v30 my_type_v30
);
insert into my_table values (42, 'something', 'something else');


CREATE SCHEMA some_schema;

create type some_schema.some_enum as enum('one', 'two');

CREATE TABLE some_schema.some_table (
    some_field some_schema.some_enum
);
insert into some_schema.some_table values ('two');


	create table fail(n numeric(12,7));
      insert into fail values
(42.8875000),
(42.0000000),
(42.1000000),
(42.1200000),
(42.1230000),
(42.1234000),
(42.1234500),
(42.1234560),
(42.1234567) ;


CREATE TABLE dum();
CREATE TABLE dee();
INSERT INTO dee DEFAULT VALUES;

-- table with duplicate column names
CREATE TABLE tbl_with_case_sensitive_columns (
	"MyColumn" INT,
	"mycolumn" INT
);
INSERT INTO tbl_with_case_sensitive_columns VALUES (42, 84);

-- tables with the same name but different case
CREATE TABLE "SameCaseName" (
	i INT
);
INSERT INTO "SameCaseName" VALUES (42);

CREATE TABLE samecasename (
	s VARCHAR
);
INSERT INTO samecasename VALUES ('hello world');

-- chars
CREATE TABLE chars(c CHAR(10));
INSERT INTO chars VALUES ('hello'), ('world'), ('maxlength1'), ('hello     '), ('     '), (NULL);

CREATE TABLE chars_array(c CHAR(10)[]);
INSERT INTO chars_array VALUES (ARRAY['hello', 'world', 'maxlength1', 'hello     ', '     ', NULL]);

-- varchar with length limit
CREATE TABLE varchars_fixed_len(c VARCHAR(10));
INSERT INTO varchars_fixed_len VALUES ('hello'), ('world'), ('maxlength1'), ('hello     '), ('     '), (NULL);

-- tables with constraints
create table tbl_with_constraints(pk int primary key, c1 int not null, c2 int, c3 int not null);
create table tbl_with_more_constraints(pk1 int, pk2 int, fk1 int references tbl_with_constraints(pk), primary key (pk1, pk2));
create table tbl_with_unique_constraints(pk int unique, c1 int not null, c2 int, c3 int not null, unique(c2, c3));

create schema main;
create table main.main_tbl(i int);
insert into main.main_tbl values (42), (NULL);

-- partitioned table (parent has relpages=-1 after analyze)
create table part_tbl (id int, val text) partition by range (id);
create table part_tbl_1 partition of part_tbl for values from (1) to (100);
create table part_tbl_2 partition of part_tbl for values from (100) to (200);
insert into part_tbl values (1,'a'),(50,'b'),(100,'c'),(150,'d');
analyze part_tbl;
