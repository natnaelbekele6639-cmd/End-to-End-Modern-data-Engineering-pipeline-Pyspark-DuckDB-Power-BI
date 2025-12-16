SELECT
    Country_Name,
    Continent,
    COUNT(*) AS total_tiles,
    AVG(download_mbps) AS avg_download_mbps,
    AVG(upload_mbps) AS avg_upload_mbps,
    AVG(latency_idle_ms) AS avg_latency_ms,
    SUM(Population) AS total_population
FROM {{ ref('stg_internet_speeds') }}
GROUP BY Country_Name, Continent
