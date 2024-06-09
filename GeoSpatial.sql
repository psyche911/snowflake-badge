// Give me the length of a Way
SELECT
ID,
ST_LENGTH(COORDINATES) AS LENGTH
FROM DENVER.V_OSM_DEN_WAY;

// List the number of nodes in a Way
SELECT
ID,
ST_NPOINTS(COORDINATES) AS NUM_OF_NODES
FROM DENVER.V_OSM_DEN_WAY;

// Give me the distance between two Ways
SELECT
 A.ID AS ID_1,
 B.ID AS ID_2,
 ST_DISTANCE(A.COORDINATES, B.COORDINATES) AS DISTANCE
FROM (SELECT
 ID,
 COORDINATES
FROM DENVER.V_OSM_DEN_WAY
WHERE ID = 705859567) AS A
INNER JOIN (SELECT
 ID,
 COORDINATES
FROM DENVER.V_OSM_DEN_WAY
WHERE ID = 705859570) AS B;

// Give me all amenities from education category in a radius of 2,000 metres from a point
SELECT
*
FROM DENVER.V_OSM_DEN_AMENITY_EDUCATION
WHERE ST_DWITHIN(ST_POINT(-1.049212522000000e+02,
    3.969829250000000e+01),COORDINATES,2000);

// Give me all food and beverage Shops in a radius of 2,000 metres from a point

SELECT
*
FROM DENVER.V_OSM_DEN_SHOP_FOOD_BEVERAGES  
WHERE ST_DWITHIN(ST_POINT(-1.049632800000000e+02,
    3.974338330000000e+01),COORDINATES,2000);


----  IPinfo IP Geolocation Sample ----
// Previewing the data
/*
The first top 10 rows from the IP geolocation demo database.
*/
SELECT *
FROM demo.location
LIMIT 10;

// Get Specific IP address data
/*
Use this query if you want to get geolocation information of a single IP address. Replace the IP address provided with your desired IP address.
*/
-- '24.183.120.0' ⇒ Input IP Address

SELECT *
FROM demo.location
WHERE ipinfo.public.TO_INT('24.183.120.0') BETWEEN start_ip_int AND end_ip_int;

-----------------
-- Explanation --
-----------------

-- TO_INT is a custom function that converts IP address values to their integer equivalent
-- start_ip_int represents the integer equivalent of the start_ip column
-- end_ip_int represents that integer equivalent of the end_ip column
-- The BETWEEN function checks to see if your input IP address falls between an the IP Range of start_ip_int and end_ip_int;

// Get the number of IP addresses by City (Groupby - Count)
/*
Get the number of IP addresses located in each city.
*/
SELECT
  COUNT(start_ip) as num_ips,
  city
FROM demo.location
GROUP BY city
ORDER BY num_ips DESC;

// Specific data query from IP address lookup
/*
Extract specific geolocation details such as, city, region, country, geographic coordinates (latitude & longitude) and timezone from a single IP address lookup.
*/
-- '24.183.120.0' ⇒ Input IP Address

SELECT 
  city,
  region,
  country,
  lat as latitude,
  lng as longitude,
  postal,
  timezone
FROM demo.location
WHERE ipinfo.public.TO_INT('24.183.120.0') BETWEEN start_ip_int AND end_ip_int;

// Optimized join on IP Addresses
/*
Joining a table that has IP addresses to IPinfo’s geolocation table. This join operation uses the Join Key column to facilitate the join operation, creating a joined table that contains the input IP address and IPinfo’s geolocation insights.
*/
-- Placeholder CTE representing the log database that contains IP addresses
-- contains two IP adddresses on the 'ip' column

WITH log AS (
    SELECT '172.4.12.1' as ip UNION SELECT '172.4.12.2'
)

-- JOIN operation code

SELECT
  input_db.ip, // 'ip' column of the log database
  ipinfo_demo.city,
  ipinfo_demo.region,
  ipinfo_demo.country,
  ipinfo_demo.postal,
  ipinfo_demo.lat,
  ipinfo_demo.lng,
  ipinfo_demo.timezone 
FROM log input_db
JOIN demo.location ipinfo_demo
ON ipinfo.public.TO_JOIN_KEY(input_db.ip) = ipinfo_demo.join_key
AND ipinfo.public.TO_INT(input_db.ip) BETWEEN ipinfo_demo.start_ip_int AND ipinfo_demo.end_ip_int;


-----------------
-- Explanation --
-----------------


-- your 'log' database contains the 'ip' column
-- using the ipinfo geolocation database you can create a new table
-- this table will contain the geolocation data of each individual ip addresses;

// Top 10 Nearest IP Address from a location
/*
The Nearest IP address shows the closest IP addresses from a geographic coordinate. We use the “Haversine formula” to find IP addresses from the provided Latitude and Longitude values.
*/
-- 42.556 ⇒ Input Latitude
-- -87.8705 ⇒ Input Longitude

SELECT
  HAVERSINE(42.556, -87.8705, lat, lng) as distance,
  start_ip,
  end_ip,
  city,
  region,
  country,
  postal,
  timezone
FROM demo.location
order by 1
limit 10;


-----------------
-- Explanation --
-----------------


-- Uses the Haversine Formula: https://en.wikipedia.org/wiki/Haversine_formula
-- The haversine formula determines the great-circle distance between two points on a sphere given their longitudes and latitudes.;

