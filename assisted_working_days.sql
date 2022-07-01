WITH dates AS(
    SELECT  generate_series(date '{{START_DATE}}', date '{{END_DATE}}', '1 day')::date as dates
),
base AS(
    SELECT
      applied_date::date, 
      COUNT(1) AS Total_leads
    FROM 
      lenderoffers.applications la
    WHERE
      la.applied_oic !~* '(system|andriod|portfolio|reg)'
      and la.applied_date >= '{{START_DATE}}'
      and la.applied_date <= '{{END_DATE}}'
    GROUP BY 1
    HAVING count(1) > 100
    ORDER BY 1
)
SELECT
    COUNT(1) FILTER (WHERE applied_date IS NOT NULL) as date_past
FROM
    dates d
    LEFT JOIN 
        base b 
        ON d.dates = b.applied_date