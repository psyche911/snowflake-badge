---- Lesson 2 Inbound Shares ----

alter database XXXX_SAMPLE_DATA
rename to SNOWFLAKE_SAMPLE_DATA;

grant imported privileges
on database SNOWFLAKE_SAMPLE_DATA
to role SYSADMIN;

--Check the range of values in the Market Segment Column
SELECT DISTINCT c_mktsegment
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

--Find out which Market Segments have the most customers
SELECT c_mktsegment, COUNT(*)
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
GROUP BY c_mktsegment
ORDER BY COUNT(*);

-- Nations Table
SELECT N_NATIONKEY, N_NAME, N_REGIONKEY
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION;

-- Regions Table
SELECT R_REGIONKEY, R_NAME
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;

-- Join the Tables and Sort
SELECT R_NAME as Region, N_NAME as Nation
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION 
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION 
ON N_REGIONKEY = R_REGIONKEY
ORDER BY R_NAME, N_NAME ASC;

--Group and Count Rows Per Region
SELECT R_NAME as Region, count(N_NAME) as NUM_COUNTRIES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION 
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION 
ON N_REGIONKEY = R_REGIONKEY
GROUP BY R_NAME;


---- Lesson 3 Joining Local Data With Shared Data ----

// Set Up a New Database Called INTL_DB
use role SYSADMIN;

create database INTL_DB;

use schema INTL_DB.PUBLIC;

// Create a Warehouse for Loading INTL_DB
use role SYSADMIN;

create warehouse INTL_WH 
with 
warehouse_size = 'XSMALL' 
warehouse_type = 'STANDARD' 
auto_suspend = 600 --600 seconds/10 mins
auto_resume = TRUE;

use warehouse INTL_WH;

// Create Table INT_STDS_ORG_3166
create or replace table intl_db.public.INT_STDS_ORG_3166 
(iso_country_name varchar(100), 
 country_name_official varchar(200), 
 sovreignty varchar(40), 
 alpha_code_2digit varchar(2), 
 alpha_code_3digit varchar(3), 
 numeric_country_code integer,
 iso_subdivision varchar(15), 
 internet_domain_code varchar(10)
);

// Create a File Format to Load the Table
create or replace file format util_db.public.PIPE_DBLQUOTE_HEADER_CR 
  type = 'CSV' --use CSV for any flat file
  compression = 'AUTO' 
  field_delimiter = '|' --pipe or vertical bar
  record_delimiter = '\r' --carriage return
  skip_header = 1  --1 header row
  field_optionally_enclosed_by = '\042'  --double quotes
  trim_space = FALSE;

// Check to see if you have a stage in your account already 
//(this will be true if you are using the same Trial Account from Badge 1)
show stages in account; 

// Method 1: grant SYSADMIN rights to use the stage
GRANT USAGE ON STAGE util_db.public.like_a_window_into_an_s3_bucket TO ROLE SYSADMIN;

// Method 2: Create a new stage using the code below while in the SYSADMIN role
create stage util_db.public.aws_s3_bucket url = 's3://uni-cmcw';

// View the files in the stage either by navigating to the stage and enabling the directory table, 
// or by running a list command like this: 
list @util_db.public.aws_s3_bucket;

copy into INT_STDS_ORG_3166
from @util_db.public.aws_s3_bucket
files = ( 'ISO_Countries_UTF8_pipe.csv')
file_format = ( format_name='util_db.public.PIPE_DBLQUOTE_HEADER_CR' );

// Check the Created and Loaded the Table
select count(*) as found, '249' as expected 
from INTL_DB.PUBLIC.INT_STDS_ORG_3166; 

// Rename database "INTRL_DB" into "INTL_DB"
ALTER DATABASE INTRL_DB
RENAME TO INTL_DB;

// Rename table "INTRL_DB.PUBLIC.INT_3166" into "INTL_DB.PUBLIC.INT_STDS_ORG_3166"
ALTER TABLE INTRL_DB.PUBLIC.INT_3166
RENAME TO INTL_DB.PUBLIC.INT_STDS_ORG_3166;

// Empty the table ("truncate") and Load it again!
TRUNCATE TABLE INTL_DB.PUBLIC.INT_STDS_ORG_3166;


// Test Whether You Set Up Your Table in the Right Place with the Right Name
// Can "ask" the Information Schema Table called "Tables" to count the number of times 
// a table with that name, in a certain schema, in a certain database (catalog) exists. 
// If it exists, we should get back the count of 1. 
/*
select count(*) as OBJECTS_FOUND
from <database name>.INFORMATION_SCHEMA.TABLES 
where table_schema=<schema name> 
and table_name= <table name>;
*/
select count(*) as OBJECTS_FOUND
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3166';


// Test That You Loaded the Expected Number of Rows
// Can "ask" the Information Schema Table called "Tables" 
// if our table has the expected number of rows
/*
select count(*) as OBJECTS_FOUND
from <database name>.INFORMATION_SCHEMA.TABLES 
where table_schema=<schema name> 
and table_name= <table name>;
*/
select count(*) as OBJECTS_FOUND
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3166';

// Test That You Loaded the Expected Number of Rows
// Can "ask" the Information Schema Table called "Tables" if our table has the expected number of rows
/*
select row_count
from <database name>.INFORMATION_SCHEMA.TABLES 
where table_schema=<schema name> 
and table_name= <table name>;
*/
select row_count
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3166';


// Join Local Data with Shared Data
select  
     iso_country_name
    ,country_name_official,alpha_code_2digit
    ,r_name as region
from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
on upper(i.iso_country_name)= n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
on n_regionkey = r_regionkey;


// Convert the Select Statement into a View
create view intl_db.public.NATIONS_SAMPLE_PLUS_ISO 
( iso_country_name
  ,country_name_official
  ,alpha_code_2digit
  ,region) AS
    select  
         iso_country_name
        ,country_name_official,alpha_code_2digit
        ,r_name as region
    from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
    left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
    on upper(i.iso_country_name)= n.n_name
    left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
    on n_regionkey = r_regionkey
;
select *
from intl_db.public.NATIONS_SAMPLE_PLUS_ISO;


// Create two more tables and another file format
// Load the data into the tables

//Create Table Currencies
create table intl_db.public.CURRENCIES 
(
  currency_ID integer, 
  currency_char_code varchar(3), 
  currency_symbol varchar(4), 
  currency_digital_code varchar(3), 
  currency_digital_name varchar(30)
)
  comment = 'Information about currencies including character codes, symbols, digital codes, etc.';

//Create Table Country to Currency
create table intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE 
  (
    country_char_code varchar(3), 
    country_numeric_code integer, 
    country_name varchar(100), 
    currency_name varchar(100), 
    currency_char_code varchar(3), 
    currency_numeric_code integer
  ) 
  comment = 'Mapping table currencies to countries';

//Create a File Format to Process files with Commas, Linefeeds and a Header Row
create file format util_db.public.CSV_COMMA_LF_HEADER
  type = 'CSV' 
  field_delimiter = ',' 
  record_delimiter = '\n' -- the n represents a Line Feed character
  skip_header = 1 
;

list @util_db.public.aws_s3_bucket;

copy into CURRENCIES
from @util_db.public.aws_s3_bucket
files = ( 'currencies.csv')
file_format = ( format_name='util_db.public.CSV_COMMA_LF_HEADER' );

copy into COUNTRY_CODE_TO_CURRENCY_CODE
from @util_db.public.aws_s3_bucket
files = ( 'country_code_to_currency_code.csv')
file_format = ( format_name='util_db.public.CSV_COMMA_LF_HEADER' );

// Create a View of the Tweet Data Looking "Normalized"
// Create a View
create view intl_db.public.SIMPLE_CURRENCY 
( cty_code
  ,cur_code) AS
    select  
         country_char_code
        ,currency_char_code
    from INTL_DB.PUBLIC.country_code_to_currency_code
;

select count (*) 
from intl_db.public.simple_currency;


---- Lesson 4 Sharing Data with Other Accounts ----

// Convert "Regular" Views to Secure Views
alter view intl_db.public.NATIONS_SAMPLE_PLUS_ISO
set secure; 

alter view intl_db.public.SIMPLE_CURRENCY

set secure; 
