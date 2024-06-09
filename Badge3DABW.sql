---- Lesson 1 Intro to Streamlit ----

//  Create Create a FRUIT_OPTIONS Table
create or replace table smoothies.public.FRUIT_OPTIONS 
(FRUIT_ID integer, 
 FRUIT_NAME varchar(25)
);


COPY INTO "SMOOTHIES"."PUBLIC"."FRUIT_OPTIONS"
FROM '@"SMOOTHIES"."PUBLIC"."%FRUIT_OPTIONS"/__snowflake_temp_import_files__/'
FILES = ('fruits_available_for_smoothies.txt')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER='%',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
)
ON_ERROR=ABORT_STATEMENT
PURGE=TRUE;


CREATE FILE FORMAT SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM
    TYPE=CSV,
    SKIP_HEADER=2,
    FIELD_DELIMITER='%',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
;


COPY INTO "SMOOTHIES"."PUBLIC"."FRUIT_OPTIONS"
FROM @SMOOTHIES.PUBLIC.MY_INTERNAL_STAGE
FILES = ('fruits_available_for_smoothies.txt')
FILE_FORMAT = ( FORMAT_NAME = SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM )
ON_ERROR=ABORT_STATEMENT 
VALIDATION_MODE= RETURN_ERRORS
PURGE=TRUE;

SELECT $1, $2
FROM @SMOOTHIES.PUBLIC.MY_INTERNAL_STAGE/fruits_available_for_smoothies.txt
(FILE_FORMAT => SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM);

COPY INTO "SMOOTHIES"."PUBLIC"."FRUIT_OPTIONS"
FROM (select $2 as FRUIT_ID, $1 as FRUIT_NAME
from @SMOOTHIES.PUBLIC.MY_INTERNAL_STAGE/fruits_available_for_smoothies.txt)
FILE_FORMAT = ( FORMAT_NAME = SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM )
ON_ERROR=ABORT_STATEMENT
PURGE=TRUE;

---- Lesson 1 ----

// Create a Place to Store Order Data
create table SMOOTHIES.PUBLIC.ORDERS
(
 INGREDIENTS varchar(200)
);

insert into SMOOTHIES.PUBLIC.ORDERS (INGREDIENTS)
values('Blueberries Cantaloupe Dragon Fruit Honeydew Lime');

select * from smoothies.public.orders;

// Truncate the Orders Table
truncate table smoothies.public.orders;


---- Lesson 4 Prototype ----

// Use the ALTER Command to Add a New Column to Your Orders Table
alter table smoothies.public.orders add column name_on_order varchar(100);

insert into smoothies.public.orders(ingredients, name_on_order) values ('Dragon Fruit Honeydew Guava Apple Kiwi', 'MellyMel');

select * from smoothies.public.orders
where name_on_order is not null;

alter table smoothies.public.orders drop column order_filled;

alter table smoothies.public.orders add column order_filled boolean default false;

update smoothies.public.orders
set order_filled = true
where name_on_order is null;


---- Lesson 5 Pending Orders App Improvements ----

truncate table smoothies.public.orders;

// Add the Unique ID Column  
alter table SMOOTHIES.PUBLIC.ORDERS 
add column order_uid integer --adds the column
default smoothies.public.order_seq.nextval  --sets the value of the column to sequence
constraint order_uid unique enforced; --makes sure there is always a unique value in the column;

select * from smoothies.public.orders;

// Unique id column is the first column
// Datetime stamps often come last
create or replace table smoothies.public.orders (
       order_uid number(38,0) default smoothies.public.order_seq.nextval,
       order_filled boolean default false,
       name_on_order varchar(100),
       ingredients varchar(200),
       order_ts timestamp_ltz(9) default current_timestamp(),
       constraint order_uid unique (order_uid)
);


---- Lesson 7 Variables and Variable-Driven Loading ----

set mystery_bag = 'What is in here?';
select $mystery_bag;

set var1 = 2;
set var2 = 5;
set var3 = 7;

// Only need the dollar sign symbol when referring to a local variable
select $var1+$var2+$var3;

create function sum_mystery_bag_vars (var1 number, var2 number, var3 number)
    returns number as 'select var1+var2+var3';

select sum_mystery_bag_vars (12, 36, 204);

// Using a System Function to Fix a Variable Value
set alternating_caps_phrase = 'aLtErNaTiNg CaPs!';
select $alternating_caps_phrase;
select initcap($alternating_caps_phrase);

create function neutralize_whining (text varchar(200))
    returns varchar(200) as 'select initcap(text)';

set text='bUt mOm i wAsHeD tHe dIsHes yEsTeRdAy';

select neutralize_whining($text);

-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
   SELECT 'DABW008' as step 
   ,( select sum(hash_ing) from
      (select hash(ingredients) as hash_ing
       from smoothies.public.orders
       where order_ts is not null 
       and name_on_order is not null 
       and (name_on_order = 'Kevin' and order_filled = FALSE and hash_ing = 7976616299844859825) 
       or (name_on_order ='Divya' and order_filled = TRUE and hash_ing = -6112358379204300652)
       or (name_on_order ='Xi' and order_filled = TRUE and hash_ing = 1016924841131818535))
     ) as actual 
   , 2881182761772377708 as expected 
   ,'Followed challenge lab directions' as description
); 


---- Lesson 10 Using API Data With Variables ----

alter table smoothies.public.fruit_options add column fruit_options varchar(100);
alter table smoothies.public.fruit_options add column search_on varchar(100);

update smoothies.public.fruit_options
set search_on = 'Kiwi';

select * from smoothies.public.fruit_options;

update smoothies.public.fruit_options
set search_on = 'Apple'
where fruit_name = 'Apples';

select * from smoothies.public.orders;
