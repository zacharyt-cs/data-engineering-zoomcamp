-- Question 3
select *
from yellow_taxi_trips
where DATE(tpep_pickup_datetime) = '2021-01-15'

-- Question 4
select *
from yellow_taxi_trips
where EXTRACT(MONTH FROM tpep_pickup_datetime) = 1
order by tip_amount DESC;

-- Question 5
SELECT "Zone", count(*)
FROM yellow_taxi_trips, taxi_zone
WHERE "PULocationID" = "LocationID"
and DATE(tpep_pickup_datetime) = '2021-01-14'
GROUP BY "Zone"
ORDER BY count(*) DESC

-- Question 6
SELECT z1."Zone", z2."Zone", AVG(y1."total_amount")
FROM yellow_taxi_trips as y1 LEFT JOIN
taxi_zone as z1 ON ("PULocationID" = z1."LocationID") LEFT JOIN
taxi_zone as z2 ON ("DOLocationID" = z2."LocationID")
GROUP BY z1."Zone", z2."Zone"
ORDER BY AVG(y1."total_amount") DESC