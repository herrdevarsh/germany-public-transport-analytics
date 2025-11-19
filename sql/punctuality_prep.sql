-- Number of trips per route (useful to identify busy routes)
SELECT
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    COUNT(DISTINCT t.trip_id) AS n_trips
FROM routes r
JOIN trips t ON r.route_id = t.route_id
GROUP BY r.route_id, r.route_short_name, r.route_long_name
ORDER BY n_trips DESC
LIMIT 20;

-- Top stops by number of scheduled arrivals (proxy for "busy")
SELECT
    s.stop_id,
    s.stop_name,
    COUNT(*) AS n_arrivals
FROM stop_times st
JOIN stops s ON st.stop_id = s.stop_id
GROUP BY s.stop_id, s.stop_name
ORDER BY n_arrivals DESC
LIMIT 20;
