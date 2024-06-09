select * from zenas_athleisure_db.information_schema.stages;

---- Lesson 3 Leaving the Data Where it Lands ----

list @uni_klaus_zmd;

// Query Data in the ZMD (stage)
select $1
from @uni_klaus_zmd; 

// Query Data in Just One File at a Time 
select $1
from @uni_klaus_zmd/product_coordination_suggestions.txt; 


// Learn about data by trying out some different file format settings
// To test whether the carets are supposed to separate one row from another
// Create an Exploratory File Format
create file format zmd_file_format_1
RECORD_DELIMITER = '^';

// Use the Exploratory File Format in a Query
select $1
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_1);

// Testing Our Second Theory
create file format zmd_file_format_2
FIELD_DELIMITER = '^';  

select $1
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_2);

select $1, $2, $3, $4
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_2);

// A Third Possibility?
// What if the carets separate records and a different symbol is used to separate the columns? 
// Need to define both the field delimiter and the row delimiter to make it work
create file format zmd_file_format_3
FIELD_DELIMITER = '='
RECORD_DELIMITER = '^'; 

select $1, $2
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);


select $1
from @uni_klaus_zmd/sweatsuit_sizes.txt;

// Revise zmd_file_format_1
// Either DROP the old file format and create a new one with the same name, or 
// Add the phrase "OR REPLACE" to the "CREATE FILE FORMAT" statement
create or replace file format zmd_file_format_1
RECORD_DELIMITER = ';'
TRIM_SPACE = True;

select $1 as sizes_available
from @uni_klaus_zmd/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 );

// Revise zmd_file_format_2
// Add the TRIM_SPACE property to the file format
create or replace file format zmd_file_format_2
FIELD_DELIMITER = '|'
RECORD_DELIMITER = ';'
TRIM_SPACE = True --removing leading space from row 9, 10
;  

select $1, $2, $3
from @uni_klaus_zmd/swt_product_line.txt
(file_format => zmd_file_format_2);


// Make the Sweatsuit Sizes Data Look Great! 

// In SQL use ASCII references to deal with these characters
// 13 is the ASCII for Carriage return
// 10 is the ASCII for Line Feed

// SQL has a function, CHR() to reference ASCII characters by their numbers
// chr(13) = Carriage Return character
// chr(10) = Line Feed character
// chr(13)||chr(10) = CRLF

// File Format cannot fix CRLF shown as 'space' in data
// SELECT statement to fix it
select REPLACE($1, chr(13)||chr(10)) as sizes_available
from @uni_klaus_zmd/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 )
where sizes_available <> '';


// Convert Your Select to a View
create view zenas_athleisure_db.products.sweatsuit_sizes as 
select REPLACE($1, chr(13)||chr(10)) as sizes_available
from @uni_klaus_zmd/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 )
where sizes_available <> '';

select * 
from zenas_athleisure_db.products.sweatsuit_sizes;


// Make the Sweatband Product Line File Look Great! 
// Explore the raw data
select $1, $2, $3
from @uni_klaus_zmd/swt_product_line.txt;

// Revised File Format
create or replace file format zmd_file_format_2
FIELD_DELIMITER = '|'
RECORD_DELIMITER = ';'
TRIM_SPACE = True;

// Query the formatted data
select REPLACE($1, chr(13)||chr(10)) as product_code, 
    $2 as headband_description, 
    $3 as wristband_description
from @uni_klaus_zmd/swt_product_line.txt
(file_format => zmd_file_format_2);

// Create view
create view zenas_athleisure_db.products.sweatband_product_line as 
select REPLACE($1, chr(13)||chr(10)) as product_code, 
    $2 as headband_description, 
    $3 as wristband_description
from @uni_klaus_zmd/swt_product_line.txt
(file_format => zmd_file_format_2);

select * 
from zenas_athleisure_db.products.sweatband_product_line;


// Make the Product Coordination Data Look great!
select $1, $2
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

create view zenas_athleisure_db.products.sweatband_coordination as 
select REPLACE($1, chr(13)||chr(10)) as product_code, 
    $2 as has_matching_sweatsuit
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

select *
from zenas_athleisure_db.products.sweatband_coordination;


---- Lesson 4 External Unstructured Data ----

list @uni_klaus_sneakers;

// Not working -- error
list @uni_klaus_chothing;
select $1 from @uni_klaus_clothing/90s_tracksuit.png;


// Query with 2 Built-In Meta-Data Columns
select metadata$filename, metadata$file_row_number
from @uni_klaus_clothing/90s_tracksuit.png;

// Group by file name and aggregate number of rows
select metadata$filename, 
    count(metadata$file_row_number) as number_of_rows
from @uni_klaus_clothing
group by metadata$filename;


// Enabling, Refreshing and Querying Directory Tables

--Directory Tables (DT)
select * from directory(@uni_klaus_clothing);

--Turn DT on, first
alter stage uni_klaus_clothing 
set directory = (enable = true);

--Now?
select * from directory(@uni_klaus_clothing);

--Refresh the directory table
alter stage uni_klaus_clothing refresh;

--Now?
select * from directory(@uni_klaus_clothing);


// Checking Whether Functions will Work on Directory Tables 
--testing UPPER and REPLACE functions on directory table
select UPPER(RELATIVE_PATH) as uppercase_filename
, REPLACE(uppercase_filename,'/') as no_slash_filename
, REPLACE(no_slash_filename,'_',' ') as no_underscores_filename
, REPLACE(no_underscores_filename,'.PNG') as just_words_filename
from directory(@uni_klaus_clothing);

// Nest them all into a single column
select REPLACE(REPLACE(REPLACE(UPPER(RELATIVE_PATH),'/'),'_',' '),'.PNG') as product_name
from directory(@uni_klaus_clothing);


//  JOIN a Directory Table to a regular, internal Snowflake table?
--create an internal table for some sweat suit info
create or replace TABLE ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS (
	COLOR_OR_STYLE VARCHAR(25),
	DIRECT_URL VARCHAR(200),
	PRICE NUMBER(5,2)
);

--fill the new table with some data
insert into  ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS 
          (COLOR_OR_STYLE, DIRECT_URL, PRICE)
values
('90s', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/90s_tracksuit.png',500)
,('Burgundy', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/burgundy_sweatsuit.png',65)
,('Charcoal Grey', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/charcoal_grey_sweatsuit.png',65)
,('Forest Green', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/forest_green_sweatsuit.png',65)
,('Navy Blue', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/navy_blue_sweatsuit.png',65)
,('Orange', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/orange_sweatsuit.png',65)
,('Pink', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/pink_sweatsuit.png',65)
,('Purple', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/purple_sweatsuit.png',65)
,('Red', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/red_sweatsuit.png',65)
,('Royal Blue',	'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/royal_blue_sweatsuit.png',65)
,('Yellow', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/yellow_sweatsuit.png',65);

select * from zenas_athleisure_db.products.sweatsuits;

select * from directory(@uni_klaus_clothing);

-- 3 way join - internal table, directory table, and view based on external data
select color_or_style
, direct_url
, price
, size as image_size
, last_modified as image_last_modified
from sweatsuits s
join directory(@uni_klaus_clothing) d
on replace(s.direct_url, 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing') = d.relative_path
--on substr(s.direct_url, 54, 50) = d.relative_path
;

// CROSS JOIN is also called "cartesian products" 
// Cartesian products are often referring to a bad join that resulted in many more records than they intended
// Cross Joins are different than Outer Joins
// While both joins can result in an "explosion" of rows, the resulting columns look different.

select * from sweatsuit_sizes;

create or replace view zenas_athleisure_db.products.catalog as 
select color_or_style
, direct_url
, price
, size as image_size
, last_modified as image_last_modified
, sizes_available
from sweatsuits 
join directory(@uni_klaus_clothing) 
on relative_path = SUBSTR(direct_url,54,50)
cross join sweatsuit_sizes;


// Add the Upsell Table and Populate It
-- Add a table to map the sweat suits to the sweat band sets
create table ZENAS_ATHLEISURE_DB.PRODUCTS.UPSELL_MAPPING
(
SWEATSUIT_COLOR_OR_STYLE varchar(25)
,UPSELL_PRODUCT_CODE varchar(10)
);

--populate the upsell table
insert into ZENAS_ATHLEISURE_DB.PRODUCTS.UPSELL_MAPPING
(
SWEATSUIT_COLOR_OR_STYLE
,UPSELL_PRODUCT_CODE 
)
VALUES
('Charcoal Grey','SWT_GRY')
,('Forest Green','SWT_FGN')
,('Orange','SWT_ORG')
,('Pink', 'SWT_PNK')
,('Red','SWT_RED')
,('Yellow', 'SWT_YLW');

select * from upsell_mapping;


// View for the Athleisure Web Catalog Prototype
-- Zena needs a single view she can query for her website prototype
create view catalog_for_website as 
select color_or_style
,price
,direct_url
,size_list
,coalesce('BONUS: ' ||  headband_description || ' & ' || wristband_description, 'Consider White, Black or Grey Sweat Accessories')  as upsell_product_desc
from
(   select color_or_style, price, direct_url, image_last_modified,image_size
    ,listagg(sizes_available, ' | ') within group (order by sizes_available) as size_list
    from catalog
    group by color_or_style, price, direct_url, image_last_modified, image_size
) c
left join upsell_mapping u
on u.sweatsuit_color_or_style = c.color_or_style
left join sweatband_coordination sc
on sc.product_code = u.upsell_product_code
left join sweatband_product_line spl
on spl.product_code = sc.product_code
where price < 200 -- high priced items like vintage sweatsuits aren't a good fit for this website
and image_size < 1000000 -- large images need to be processed to a smaller size
;

select * from catalog_for_website;


---- Lesson 5 ----

use database mels_smoothie_challenge_db;
use schema mels_smoothie_challenge_db.trails;

list @trails_geojson;
list @trails_parquet;

// Create two Files Format
create or replace file format mels_smoothie_challenge_db.trails.FF_JSON 
  type = 'JSON';

create or replace file format mels_smoothie_challenge_db.trails.FF_PARQUET 
  type = 'PARQUET';

// Query Your TRAILS_GEOJSON Stage
select $1
from @trails_geojson
(file_format => ff_json);

select $1
from @trails_parquet
(file_format => ff_parquet);


// Look at the Parquet Data
// Sophisticated query to parse the data into columns
select 
    $1:sequence_1 as sequence_1,
    $1:trail_name::varchar as trail_name,
    $1:sequence_2 as sequence_2,
    $1:elevation as elevation,
    $1:latitude as latitude,
    $1:longitude as longitude
from @trails_parquet
(file_format => ff_parquet)
order by sequence_1;

--Nicely formatted trail data
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng, --remember we did a gut check on this data
 $1:longitude::number(11,8) as lat
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

create or replace view cherry_creek_trail as 
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng, --remember we did a gut check on this data
 $1:longitude::number(11,8) as lat
from @trails_parquet
(file_format => ff_parquet)
order by point_id;


// Use || to Chain Lat and Lng Together into Coordinate Sets
--Using concatenate to prepare the data for plotting on a map
select top 100 
 lng||' '||lat as coord_pair
,'POINT('||coord_pair||')' as trail_point
from cherry_creek_trail;

--To add a column, we have to replace the entire view
--changes to the original are shown in red
create or replace view cherry_creek_trail as
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng,
 $1:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair
from @trails_parquet
(file_format => ff_parquet)
order by point_id;


// Collapse Sets Of Coordinates into Linestrings
// LISTAGG function and the new COORD_PAIR column to make LINESTRINGS
/*
syntax for LINESTRINGS

LINESTRING(
Coordinate Pair
COMMA
Coordinate Pair
COMMA
Coordinate Pair
(etc)
) 
*/

select coord_pair from cherry_creek_trail;

select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
where point_id <= 10
group by trail_name;

// Make The Whole Trail into a Single LINESTRING
select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
--where point_id <= 10
group by trail_name;


---- Lesson 6 GeoSpatial Views ----

// If you can't remember file format names, just use SHOW commands
show file formats;

select * 
from @trails_geojson
(file_format => ff_json);

// Normalize the Data Without Loading It
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);


create or replace view denver_area_trails as
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);


---- Lesson 7 GeoSpatial Functions ----

// Re-Using Earlier Code (with a Small Addition)
select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
--,st_length(my_linestring) as length_of_trail --this line is new! but it won't work!
,st_length(to_geography(my_linestring)) as length_of_trail --TO_GEOGRAPHY() Function make it properly
from cherry_creek_trail
group by trail_name;

select * from denver_area_trails;

// Use GeoSpatial functions to derive the length of the trails 
select 
    feature_name
    --,'LINESTRING('|| replace(replace(replace(replace(feature_coordinates, ',', ' '), '] [', ', '), '[['), ']]') ||')' as trail_string
    , st_length(to_geography('LINESTRING('|| replace(replace(replace(replace(feature_coordinates, ',', ' '), '] [', ', '), '[['), ']]') ||')')) as trail_length
from denver_area_trails;

 
// Get the data definition by navigating to the home screen, 
// navigating to the object, and copying it from there, or
// Use GET_DDL() function to get a copy of a 
// CREATE OR REPLACE VIEW code block for your existing view
select get_ddl('view', 'DENVER_AREA_TRAILS');

create or replace view DENVER_AREA_TRAILS(
	FEATURE_NAME,
	FEATURE_COORDINATES,
	GEOMETRY,
    TRAIL_LENGTH,
	FEATURE_PROPERTIES,
	SPECS,
	WHOLE_OBJECT
) as
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
    , st_length(to_geography('LINESTRING('|| replace(replace(replace(replace(feature_coordinates, ',', ' '), '] [', ', '), '[['), ']]') ||')')) as trail_length
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);


// Get a copy of a CREATE OR REPLACE VIEW code block for your existing view
select * from cherry_creek_trail;
select * from denver_area_trails;

// Create a View on Cherry Creek Data to Mimic the Other Trail Data
--Create a view that will have similar columns to DENVER_AREA_TRAILS 
--Even though this data started out as Parquet, and we're joining it with geoJSON data
--So let's make it look like geoJSON instead.
create view DENVER_AREA_TRAILS_2 as
select 
trail_name as feature_name
,'{"coordinates":['||listagg('['||lng||','||lat||']',',')||'],"type":"LineString"}' as geometry
,st_length(to_geography(geometry)) as trail_length
from cherry_creek_trail
group by trail_name;

select * from denver_area_trails_2;

// Use A Union All to Bring the Rows Into a Single Result Set
--Create a view that will have similar columns to DENVER_AREA_TRAILS 
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS_2;

--Add more GeoSpatial Calculations to get more GeoSpecial Information! 
--Find min and max longitudes and latitudes of each trail
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS_2;

--Make a View
create view trails_and_boundaries as 
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS_2;

select * from trails_and_boundaries;

// A Polygon Can be Used to Create a Bounding Box
select 'POLYGON((' || 
    min(min_eastwest) || ' ' || max(max_northsouth) || ',' ||
    max(max_eastwest) || ' ' || max(max_northsouth) || ',' ||
    max(max_eastwest) || ' ' || min(min_northsouth) || ',' ||
    min(min_eastwest) || ' ' || min(min_northsouth) || '))' as my_polygon
from trails_and_boundaries;


-- Melanie's Location into a 2 Variables (mc for melanies cafe)
set mc_lat='-104.97300245114094';
set mc_lng='39.76471253574085';

--Confluence Park into a Variable (loc for location)
set loc_lat='-105.00840763333615'; 
set loc_lng='39.754141917497826';

--Test your variables to see if they work with the Makepoint function
select st_makepoint($mc_lat,$mc_lng) as melanies_cafe_point;
select st_makepoint($loc_lat,$loc_lng) as confluent_park_point;

--use the variables to calculate the distance from 
--Melanie's Cafe to Confluent Park
select st_distance(
        st_makepoint($mc_lat,$mc_lng)
        ,st_makepoint($loc_lat,$loc_lng)
        ) as mc_to_cp;


// When a user defines a function it's called a User-Defined Function (or UDF). 
// Give UDF a name, DISTANCE_TO_MC (for Distance to Melanie's Café)
// Pass in the point to measure the distance FROM. 
// Call that the "location" and shorten it to "LOC"
// Pass in LOC_LAT as the Latitude and LOC_LNG as the Longitude
CREATE OR REPLACE FUNCTION distance_to_mc(loc_lat number(38,32), loc_lng number(38,32))
  RETURNS FLOAT
  AS
  $$
   st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,st_makepoint(loc_lat,loc_lng)
        )
  $$
  ;

//Test the New Function
--Tivoli Center into the variables
set tc_lat='-105.00532059763648'; 
set tc_lng='39.74548137398218';

select distance_to_mc($tc_lat,$tc_lng);


// Create a List of Competing Juice Bars in the Area
select * 
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%');

    
--Convert the List into a View
create view COMPETITION as
select * 
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%');

--Which Competitor is Closest to Melanie's?
SELECT
 name
 ,cuisine
 , ST_DISTANCE(
    st_makepoint('-104.97300245114094','39.76471253574085')
    , coordinates
  ) AS distance_from_melanies
 ,*
FROM  competition
ORDER by distance_from_melanies;


// Changing the Function to Accept a GEOGRAPHY Argument 
CREATE OR REPLACE FUNCTION distance_to_mc(lat_and_lng GEOGRAPHY)
  RETURNS FLOAT
  AS
  $$
   st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,lat_and_lng
        )
  $$
  ;

--Use it In Our Sonra Select
SELECT
 name
 ,cuisine
 ,distance_to_mc(coordinates) AS distance_from_melanies
 ,*
FROM  competition
ORDER by distance_from_melanies;

// When speaking about a FUNCTION plus its ARGUMENTS we can refer to it as the FUNCTION SIGNATURE. 

//Different Options, Same Outcome!
-- Tattered Cover Bookstore McGregor Square
set tcb_lat='-104.9956203'; 
set tcb_lng='39.754874';

--this will run the first version of the UDF
select distance_to_mc($tcb_lat,$tcb_lng);

--this will run the second version of the UDF, bc it converts the coords 
--to a geography object before passing them into the function
select distance_to_mc(st_makepoint($tcb_lat,$tcb_lng));

--this will run the second version bc the Sonra Coordinates column
-- contains geography objects already
select name
, distance_to_mc(coordinates) as distance_to_melanies 
, ST_ASWKT(coordinates)
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP
where shop='books' 
and name like '%Tattered Cover%'
and addr_street like '%Wazee%';


// Create a View of Bike Shops in the Denver Data
// Create a List of Bike Shops in the Area
select * 
from openstreetmap_denver.denver.V_OSM_DEN_SHOP_OUTDOORS_AND_SPORT_VEHICLES
where shop='bicycle';

drop view DENVER_BIKE_SHOPS;

create or replace view DENVER_BIKE_SHOPS as
select 
    name,
    distance_to_mc(coordinates) AS distance_to_melanies,
    st_aswkt(coordinates) as coordinates
from openstreetmap_denver.denver.V_OSM_DEN_SHOP_OUTDOORS_AND_SPORT_VEHICLES
where shop='bicycle';

select * from denver_bike_shops
order by distance_from_melanies;


---- Lesson 9 ----

select * from mels_smoothie_challenge_db.trails.cherry_creek_trail;

// Create this same data structure with an External Table
--Change the name of our view to have "V_" in front of the name
alter view mels_smoothie_challenge_db.trails.cherry_creek_trail
rename to mels_smoothie_challenge_db.trails.v_cherry_creek_trail;

--Create an External Table
create or replace external table T_CHERRY_CREEK_TRAIL(
	my_filename varchar(50) as (metadata$filename::varchar(50))
) 
location= @trails_parquet
auto_refresh = true
file_format = (type = parquet);

--Modify V_CHERRY_CREEK_TRAIL Code to Create the New Table
--Use GET_DDL() function to get a copy of our view code
select get_ddl('view','mels_smoothie_challenge_db.trails.v_cherry_creek_trail');

create or replace view V_CHERRY_CREEK_TRAIL(
	POINT_ID,
	TRAIL_NAME,
	LNG,
	LAT,
	COORD_PAIR
) as
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng,
 $1:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

--Re-arrange some of view codes and put it into external table definition
create or replace external table mels_smoothie_challenge_db.trails.T_CHERRY_CREEK_TRAIL(
	POINT_ID number as ($1:sequence_1::number),
	TRAIL_NAME varchar(100) as  ($1:trail_name::varchar),
	LNG number(11,8) as ($1:latitude::number(11,8)),
	LAT number(11,8) as ($1:longitude::number(11,8)),
	COORD_PAIR varchar(50) as (lng::varchar||' '||lat::varchar)
) 
location= @mels_smoothie_challenge_db.trails.trails_parquet
auto_refresh = true
file_format = mels_smoothie_challenge_db.trails.ff_parquet;

select count(*) from smv_cherry_creek_trail;
