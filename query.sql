#standardSQL
SELECT
  (max_table.max-32)*5/9 max_celsius,
  (min_table.min-32)*5/9 min_celsius,
  max_table.state
FROM (
  SELECT
    max,
    state,
    stn
  FROM (
    SELECT
      max,
      year,
      state,
      stn,
      ROW_NUMBER() OVER(PARTITION BY state ORDER BY max DESC) rn
    FROM (
      SELECT
        max,
        year,
        stn,
        wban
      FROM
        `bigquery-public-data.noaa_gsod.gsod*`
      WHERE
        _TABLE_SUFFIX BETWEEN '1990'
        AND '2000') a
    JOIN
      `bigquery-public-data.noaa_gsod.stations` b
    ON
      a.stn=b.usaf
      AND a.wban=b.wban
    WHERE
      state IS NOT NULL
      AND max<1000
      AND country='US' )
  WHERE
    rn=1
  ORDER BY
    YEAR DESC ) max_table
LEFT JOIN (
  SELECT
    min,
    (min-32)*5/9 celsius,
    state,
    stn
  FROM (
    SELECT
      min,
      year,
      state,
      stn,
      ROW_NUMBER() OVER(PARTITION BY state ORDER BY min DESC) rn
    FROM (
      SELECT
        min,
        year,
        stn,
        wban
      FROM
        `bigquery-public-data.noaa_gsod.gsod*`
      WHERE
        _TABLE_SUFFIX BETWEEN '1990'
        AND '2000') a
    JOIN
      `bigquery-public-data.noaa_gsod.stations` b
    ON
      a.stn=b.usaf
      AND a.wban=b.wban
    WHERE
      state IS NOT NULL
      AND min<1000
      AND country='US' )
  WHERE
    rn=1
  ORDER BY
    YEAR DESC ) min_table
ON
  min_table.state = max_table.state'