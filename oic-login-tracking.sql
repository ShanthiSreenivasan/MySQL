with data as (SELECT
    log_date::date as call_date,
    oic,
    case when extract(dow from log_date::date) in (0, 6) then 1 else 0 end as is_weekend,
    count(*) as total_calls,
    count(*) filter (where dispo != 'NC') as effective_calls,
    min(log_date::time) as first_call_time,
    max(log_date::time) as last_call_time,
    case when count(*) = 0 then 0 when count(*) < 40 then 0.5 else 1 end as attendance_count,
    round(extract(epoch from age(max(log_date), min(log_date)))::numeric  / (60.0 * 60.0), 2) as hours_logged
FROM
    lenderoffers.lo_calls
WHERE
    log_date >= '{{ START_DATE }}'::date
    and log_date < '{{ END_DATE }}'::date
    and oic !~* '^d(coll|isp|vert|koc)'
GROUP BY
    1, 2
ORDER BY
    1, 2),
    data2 as (select 
         a.applied_oic,
         a.applied_date :: date as applied_date,
         sum(case 
         when a.applied_date is not null and a.sku ~* 'Shriram' then 1
         when a.sku ~* 'RBL CC' and a.applied_date is not null then 2
         when a.sku=sp.sku and a.customer_type=sp.customer_type and sp.api_flag=1  and a.appops_status_code >= '390' then sp.points::integer
         when a.sku=sp.sku and a.customer_type=sp.customer_type and sp.api_flag=0  and a.appops_status_code >='250' then sp.points::integer
         else 0 end) as points_sku,
         count(a.lead_id) as referrals_generated
         from 
         lenderoffers.applications a
         left join 
         master_tables.sku_points sp
         on a.sku=sp.sku
         and a.customer_type=sp.customer_type
         where          
         applied_date::date  >= '{{ START_DATE }}'
         and applied_date::date < '{{ END_DATE }}' 
         group by 1,2
         ), 
data3 as (
    SELECT
    sub_oic,
    payment_date::date as payment_date,
    COUNT(*) FILTER (WHERE service_type ~* 'CHR') as "CHR Subscriptions",
    COUNT(*) FILTER (WHERE service_type ~* 'BYS') as "BYS Subscriptions"

FROM 
    product.vas_subscriptions
WHERE   
    payment_date >= '{{ START_DATE }}'
    AND payment_date < '{{ END_DATE }}'
    AND sub_oic !~* 'sys|port'
GROUP BY 1,2
)
    select a.*,b.points_sku,b.referrals_generated, "CHR Subscriptions", "BYS Subscriptions" from data a left join data2 b 
    on a.oic=b.applied_oic
    and a.call_date =b.applied_date
    left join data3 c on c.sub_oic = a.oic and a.call_date = c.payment_date
