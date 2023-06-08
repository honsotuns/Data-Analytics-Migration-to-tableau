Milestone 3:
Task 5:

SELECT year,
	   
	   (COUNT(origin)) AS total_inbound,
	   (COUNT(dest)) AS total_outbound,
	   (COUNT(origin))+(COUNT(dest)) AS TOTAL_NO_OF_BOTH
FROM flights
GROUP BY year
ORDER BY year desc ;


SELECT dest,
	   (COUNT(dest)) AS "Most_popular_destination"
	 
FROM flights
GROUP BY dest
ORDER BY "Most_popular_destination" DESC
LIMIT 10;