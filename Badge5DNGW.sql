// Setting Some Defaults
alter user DENISEM set default_role = 'SYSADMIN';
alter user DENISEM set default_warehouse = 'COMPUTE_WH';
alter user DENISEM set default_namespace = 'UTIL_DB.PUBLIC';

use role sysadmin;
create database ags_game_audience;
drop schema public;
create schema raw;

list @uni_kishore/kickoff;

create or replace file format ff_json_logs 
  type = 'JSON'
  strip_outer_array = true;

//Exploring the File before Loading it
--Query files while they were still sitting out in a file (not-loaded) in an external stage
--Use a File Format to make the results of that query more readable
select $1
from @uni_kishore/kickoff
(file_format => ff_json_logs);

//Load the File into the Table
--If no file name in the FROM line
--COPY INTO statement will load EVERY file in the folder 
--if more than one file is there, and the file name is not specified
--But here only one file in the kickoff folder
copy into ags_game_audience.raw.game_logs
from @uni_kishore/kickoff   --no file name
file_format = (format_name=ff_json_logs);

//Build a Select Statement that Separates Every Attribute into its Own Column
--JSON parsing PATHS and data type CASTING
select 
    raw_log:agent::text as agent
    , raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601
    , raw_log:user_event::text as user_event
    , raw_log:user_login::text as user_login
    , *
from game_logs;

//Wrapping Selects in Views 
create view logs as
select 
    raw_log:agent::text as agent
    , raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601
    , raw_log:user_event::text as user_event
    , raw_log:user_login::text as user_login
    , *
from game_logs;

select * from logs;

//Change the Time Zone for Your Current Worksheet
--what time zone is your account(and/or session) currently set to? Is it -0700?
select current_timestamp(); --UTC-7(-0700), default time zone of "America/Los_Angeles"

--worksheets are sometimes called sessions -- we'll be changing the worksheet time zone
alter session set timezone = 'UTC';
select current_timestamp();

--how did the time differ after changing the time zone for the worksheet?
alter session set timezone = 'Africa/Nairobi';
select current_timestamp();

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();

alter session set timezone = 'Asia/Shanghai';
select current_timestamp();

--show the account parameter called timezone
show parameters like 'timezone';


//Exploring another File
select $1
from @uni_kishore/updated_feed
(file_format => ff_json_logs);

copy into ags_game_audience.raw.game_logs
from @uni_kishore/updated_feed   --no file name
file_format = (format_name=ff_json_logs);

select 
    raw_log:ip_address::text as ip_address
    , raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601
    , raw_log:user_event::text as user_event
    , raw_log:user_login::text as user_login
    , *
from game_logs;


//Filter Out the Old Records
//1st set of records included the AGENT field, 
//but 2nd set of records would have an empty AGENT value.
--looking for empty AGENT column
select * 
from ags_game_audience.raw.LOGS
where agent is null;


//1st set of records did NOT include IP_ADDRESS, 
//but 2nd set of records, there should be an IP_ADDRESS.
--looking for non-empty IP_ADDRESS column
select 
RAW_LOG:ip_address::text as IP_ADDRESS
,*
from ags_game_audience.raw.LOGS
where RAW_LOG:ip_address::text is not null;


//Update Your LOGS View
create or replace view logs as
select 
    raw_log:ip_address::text as ip_address
    , raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601
    , raw_log:user_event::text as user_event
    , raw_log:user_login::text as user_login
    , *
from game_logs
where raw_log:ip_address::text is not null;

/*
ISO 8601 uses a 24-Hour clock,
7:22PM is 19:22 on a 24-Hour clock,
If the time captured had been converted to UTC, 
it wouldn't show as 19:22 on Saturday evening, 
but as 1:22 -- very early on Sunday morning.
*/
select *
from logs
where user_login ilike '%prajina%';


//Paste the IP
select parse_ip('100.41.16.160', 'inet');

//Pull out the values from the PARSE_IP results by 
//adding a colon and the name after the close parentheses
select parse_ip('107.217.231.17','inet'):host;
--or
select parse_ip('107.217.231.17','inet'):family;


//Look Up Kishore & Prajina's Time Zone
--Look up Kishore and Prajina's Time Zone in the IPInfo share 
--using his headset's IP Address with the PARSE_IP function.
select start_ip, end_ip, start_ip_int, end_ip_int, city, region, country, timezone
from IPINFO_IP_GEOLOCATION_SAMPLE.demo.location
where parse_ip('100.41.16.160', 'inet'):ipv4 --Kishore's Headset's IP Address
BETWEEN start_ip_int AND end_ip_int;

//Look Up Everyone's Time Zone
--Join the log and location tables to add time zone to each row using the PARSE_IP function.
select logs.*
       , loc.city
       , loc.region
       , loc.country
       , loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_IP_GEOLOCATION_SAMPLE.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4 
BETWEEN start_ip_int AND end_ip_int;

//Use the IPInfo Functions for a More Efficient Lookup
--Use two functions supplied by IPShare to help with an efficient IP Lookup Process!
SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone 
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_IP_GEOLOCATION_SAMPLE.demo.location loc 
ON IPINFO_IP_GEOLOCATION_SAMPLE.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_IP_GEOLOCATION_SAMPLE.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;


//Add a Local Time Zone Column & DOW ("Day of Week") Column
SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone 
, convert_timezone('UTC', timezone, logs.datetime_iso8601) as game_event_ltz --local time zone
, dayname(game_event_ltz) as dow_name   --weekdays/wknds playing game?
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_IP_GEOLOCATION_SAMPLE.demo.location loc 
ON IPINFO_IP_GEOLOCATION_SAMPLE.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_IP_GEOLOCATION_SAMPLE.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;


//Assigning a Time of Day
-- Your role should be SYSADMIN
-- Your database menu should be set to AGS_GAME_AUDIENCE
-- The schema should be set to RAW

--a Look Up table to convert from hour number to "time of day name"
create table ags_game_audience.raw.time_of_day_lu
(  hour number
   ,tod_name varchar(25)
);

--insert statement to add all 24 rows to the table
insert into time_of_day_lu
values
(6,'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');

select * from time_of_day_lu;

--Check your table to see if you loaded it properly
select tod_name, listagg(hour,',') 
from time_of_day_lu
group by tod_name;

--Extract hour from timestamp
select hour(convert_timezone('UTC', timezone, logs.datetime_iso8601))
from logs
JOIN IPINFO_IP_GEOLOCATION_SAMPLE.demo.location loc 
ON IPINFO_IP_GEOLOCATION_SAMPLE.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_IP_GEOLOCATION_SAMPLE.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;

--Add Time Of Day Column
SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone 
, convert_timezone('UTC', timezone, logs.datetime_iso8601) as game_event_ltz --local time zone
, dayname(game_event_ltz) as dow_name   --weekdays/wknds playing game?
, tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_IP_GEOLOCATION_SAMPLE.demo.location loc 
ON IPINFO_IP_GEOLOCATION_SAMPLE.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_IP_GEOLOCATION_SAMPLE.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN AGS_GAME_AUDIENCE.RAW.time_of_day_lu lu
ON hour(convert_timezone('UTC', timezone, logs.datetime_iso8601)) = lu.hour;

//Convert a Select to a Table
--Wrap any Select in a CTAS statement
create schema ags_game_audience.enhanced;

create or replace table ags_game_audience.enhanced.logs_enhanced as(
SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as game_event_name
, logs.datetime_iso8601 as game_event_utc
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, convert_timezone('UTC', timezone, logs.datetime_iso8601) as game_event_ltz --local time zone
, dayname(game_event_ltz) as dow_name   --weekdays/wknds playing game?
, tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_IP_GEOLOCATION_SAMPLE.demo.location loc 
ON IPINFO_IP_GEOLOCATION_SAMPLE.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_IP_GEOLOCATION_SAMPLE.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN AGS_GAME_AUDIENCE.RAW.time_of_day_lu lu
ON hour(convert_timezone('UTC', timezone, logs.datetime_iso8601)) = lu.hour
);


//SYSADMIN Privileges for Executing Tasks

use role accountadmin;

--You have to run this grant or you won't be able to test your tasks while in SYSADMIN role
--this is true even if SYSADMIN owns the task!!
grant execute task on account to role SYSADMIN;

use role sysadmin; 

--Now you should be able to run the task, even if your role is set to SYSADMIN
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--the SHOW command might come in handy to look at the task 
show tasks in account;

--you can also look at any task more in depth using DESCRIBE
describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;


//Executing the Task to TRY to Load More Rows
--make a note of how many rows you have in the table
select count(*) --160
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--check to see how many rows were added (if any!)
select count(*) 
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;


//Trunc & Reload Like It's Y2K!
--first we dump all the rows out of the table
truncate table ags_game_audience.enhanced.LOGS_ENHANCED;

--then we put them all back in
INSERT INTO ags_game_audience.enhanced.LOGS_ENHANCED (
SELECT logs.ip_address 
, logs.user_login as GAMER_NAME
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz
, DAYNAME(game_event_ltz) as DOW_NAME
, TOD_NAME
from ags_game_audience.raw.LOGS logs
JOIN ipinfo_ip_geolocation_sample.demo.location loc 
ON ipinfo_ip_geolocation_sample.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND ipinfo_ip_geolocation_sample.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
ON HOUR(game_event_ltz) = tod.hour);

--Hey! We should do this every 5 minutes from now until the next millennium - Y3K!!!
--Alexa, play Yeah by Usher!


//Create a Backup Copy of the Table
--clone the table to save this version as a backup
--since it holds the records from the UPDATED FEED file, we'll name it _UF
create or replace table ags_game_audience.enhanced.LOGS_ENHANCED_UF 
clone ags_game_audience.enhanced.LOGS_ENHANCED;


//SQL merge lets you compare new records to already loaded records and 
//do different things based on what you learn by doing the comparison
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING RAW.LOGS r
ON r.user_login = e.GAMER_NAME
AND r.datetime_iso8601 = e.GAME_EVENT_UTC
AND r.user_event = e.GAME_EVENT_NAME
WHEN MATCHED THEN
UPDATE SET IP_ADDRESS = 'Hey I updated matching rows!';

select * from enhanced.logs_enhanced;


//Build Your Insert Merge
--Build the new command by cobbling together bits of code from previous statements
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (
        SELECT logs.ip_address 
        , logs.user_login as GAMER_NAME
        , logs.user_event as GAME_EVENT_NAME
        , logs.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.LOGS logs
        JOIN ipinfo_ip_geolocation_sample.demo.location loc 
        ON ipinfo_ip_geolocation_sample.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        AND ipinfo_ip_geolocation_sample.public.TO_INT(logs.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
        ) r
ON r.gamer_name = e.GAMER_NAME
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name
WHEN NOT MATCHED THEN
insert (IP_ADDRESS,
        GAMER_NAME,
        GAME_EVENT_NAME,
        GAME_EVENT_UTC,
        CITY,
        REGION,
        COUNTRY,
        GAMER_LTZ_NAME,
        GAME_EVENT_LTZ,
        DOW_NAME,
        TOD_NAME) --list of columns
values (IP_ADDRESS,
        GAMER_NAME,
        GAME_EVENT_NAME,
        GAME_EVENT_UTC,
        CITY,
        REGION,
        COUNTRY,
        GAMER_LTZ_NAME,
        GAME_EVENT_LTZ,
        DOW_NAME,
        TOD_NAME) --list of columns (but we can mark as coming from the r select)
;


//Testing Cycle (Optional)
--Testing cycle for MERGE. Use these commands to make sure the Merge works as expected

--Write down the number of records in your table 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; --160

--Run the Merge a few times. No new rows should be added at this time 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if your row count changed 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; --160

--Insert a test record into your Raw Table 
--You can change the user_event field each time to create "new" records 
--editing the ip_address or datetime_iso8601 can complicate things more than they need to 
--editing the user_login will make it harder to remove the fake records after you finish testing 
INSERT INTO ags_game_audience.raw.game_logs 
select PARSE_JSON('{"datetime_iso8601":"2025-01-01 00:00:00.000", "ip_address":"196.197.196.255", "user_event":"fake event", "user_login":"fake user"}');

--After inserting a new row, run the Merge again 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if any rows were added 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--When you are confident your merge is working, you can delete the raw records 
delete from ags_game_audience.raw.game_logs where raw_log like '%fake user%';

--You should also delete the fake rows from the enhanced table
delete from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where gamer_name = 'fake user';

--Row count should be back to what it was in the beginning
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; 

copy into ags_game_audience.raw.pl_game_logs
from @uni_kishore_pipeline   --no file name then all files
file_format = (format_name=ff_json_logs);

/*
What if, for some crazy reason, you wanted to double-load your files? 
You could add a FORCE=TRUE; as the last line of your COPY INTO statement and 
then you would double the number of rows in your table. 
Then, what if you wanted to start over and load just one copy of each file?
You could TRUNCATE TABLE PL_GAME_LOGS; then set FORCE=FALSE and run your COPY INTO again. 
*/

select * from pl_game_logs;


EXECUTE TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;

create or replace view pl_logs as
select 
    raw_log:ip_address::text as ip_address
    , raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601
    , raw_log:user_event::text as user_event
    , raw_log:user_login::text as user_login
    , *
from pl_game_logs
where raw_log:ip_address::text is not null;

select * from pl_logs;

EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

TRUNCATE table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

resume and suspend them using worksheet code. 

--Turning ON a task is done with a RESUME command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;

--Turning OFF a task is done with a SUSPEND command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

// Checking Tallies Along the Way
--Step 1 - how many files in the bucket?
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

--Step 2 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

--Step 3 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

--Step 4 - number of rows in enhanced table (should be file count x 10 but fewer rows is okay because not all IP addresses are available from the IPInfo share)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;


//Grant Serverless Task Management to SYSADMIN
use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN;

--switch back to sysadmin
use role sysadmin;


//Replace the WAREHOUSE Property in Your Tasks
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL';


//Replace or Update the SCHEDULE Property
--Change the SCHEDULE for GET_NEW_FILES so it runs more often
schedule='5 Minutes';

--Remove the SCHEDULE property and have LOAD_LOGS_ENHANCED run  
--each time GET_NEW_FILES completes
after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;


---- Lesson 7 DE Practice Improvement & Cloud Foundations ----

//A New Select with Metadata and Pre-Load JSON Parsing 
SELECT 
    METADATA$FILENAME as log_file_name --new metadata column
    , METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
    , current_timestamp(0) as load_ltz --new local time of load
    , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
    , get($1,'user_event')::text as USER_EVENT
    , get($1,'user_login')::text as USER_LOGIN
    , get($1,'ip_address')::text as IP_ADDRESS    
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
(file_format => 'ff_json_logs');

// Method 1: CTAS
create or replace TABLE AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS as (
SELECT 
    METADATA$FILENAME as log_file_name --new metadata column
    , METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
    , current_timestamp(0) as load_ltz --new local time of load
    , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
    , get($1,'user_event')::text as USER_EVENT
    , get($1,'user_login')::text as USER_LOGIN
    , get($1,'ip_address')::text as IP_ADDRESS    
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
(file_format => 'ff_json_logs')
);


// Method 2: Create the New COPY INTO 
--truncate the table rows that were input during the CTAS, if that's what you did
truncate table ED_PIPELINE_LOGS;

--reload the table using your COPY INTO
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);


---- Lesson 8 Snowpipe ----

//Method 3: Create Your Snowpipe!
CREATE OR REPLACE PIPE PIPE_GET_NEW_FILES
auto_ingest=true
aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
AS 
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);


//Old pipeline uses two tasks
//New pipeline uses one task and one Snowpipe

create or replace table ags_game_audience.enhanced.LOGS_ENHANCED_BACKUP 
clone ags_game_audience.enhanced.LOGS_ENHANCED;

truncate table ags_game_audience.enhanced.logs_enhanced;


//Use this command if Snowpipe seems like it is stalled out:
ALTER PIPE ags_game_audience.raw.PIPE_GET_NEW_FILES REFRESH;

//Use this command if want to check that the pipe is running:
select parse_json(SYSTEM$PIPE_STATUS( 'ags_game_audience.raw.PIPE_GET_NEW_FILES' ));


//Create a Stream
--create a stream that will keep track of changes to the table
create or replace stream ags_game_audience.raw.ed_cdc_stream 
on table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

--look at the stream you created
show streams;

--check to see if any changes are pending (expect FALSE the first time you run it)
--after the Snowpipe loads a new file, expect to see TRUE
select system$stream_has_data('ed_cdc_stream');

//View Our Stream Data
--query the stream
select * 
from ags_game_audience.raw.ed_cdc_stream; 

--check to see if any changes are pending
select system$stream_has_data('ed_cdc_stream');

--if your stream remains empty for more than 10 minutes, make sure your PIPE is running
select SYSTEM$PIPE_STATUS('PIPE_GET_NEW_FILES');

--if you need to pause or unpause your pipe
alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = true;
alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = false;

//Process the Rows from the Stream
--make a note of how many rows are in the stream
select * 
from ags_game_audience.raw.ed_cdc_stream; --10

 
--process the stream by using the rows in a merge 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_ip_geolocation_sample.demo.location loc 
        ON ipinfo_ip_geolocation_sample.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_ip_geolocation_sample.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
 
--Did all the rows from the stream disappear? 
select * 
from ags_game_audience.raw.ed_cdc_stream; 


//Create a CDC-Fueled, Time-Driven Task
--Create a new task that uses the MERGE you just tested
create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
	as 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_ip_geolocation_sample.demo.location loc 
        ON ipinfo_ip_geolocation_sample.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_ip_geolocation_sample.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
        
--Resume the task so it is running
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;


---- Lesson 9 Curated Data ----

create schema ags_game_audience.CURATED;

//Rolling Up Login and Logout Events with ListAgg
--the ListAgg function can put both login and logout into a single column in a single row
-- if we don't have a logout, just one timestamp will appear
select GAMER_NAME
      , listagg(GAME_EVENT_LTZ,' / ') as login_and_logout
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED 
group by gamer_name;


//Windowed Data for Calculating Time in Game Per Player
select GAMER_NAME
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
order by game_session_length desc;

//Code for the Heatgrid
--We added a case statement to bucket the session lengths
select case when game_session_length < 10 then '< 10 mins'
            when game_session_length < 20 then '10 to 19 mins'
            when game_session_length < 30 then '20 to 29 mins'
            when game_session_length < 40 then '30 to 39 mins'
            else '> 40 mins' 
            end as session_length
            ,tod_name
from (
select GAMER_NAME
       , tod_name
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED_UF)
where logout is not null;

