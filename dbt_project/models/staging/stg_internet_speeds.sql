SELECT
    quadkey,
    Country_Name,
    Continent,
    download_mbps,
    upload_mbps,
    latency_idle_ms,
    Performance_Grade,
    Population
FROM internet_speeds
WHERE download_mbps > 0
