SELECT
    a.lead_id,
    a.user_id,
    --rajan changes start
    ud.first_name,
    ud.phone_home,
    --rajan changes end
    a.customer_type,
    a.product,
    a.sku,
    a.lender,
    a.aip_approval_date,
    a.applied_date,
    a.utm_source,
    a.utm_content,
    a.applied_oic,
    a.utm_term,
    dsa.*,
    profiled_utm.profiled_utm_source,
    case when profiled_utm.profiled_utm_source~*'(homecredit|flexiloans)'
    and profiled_utm.profiled_utm_source not in('homecredit')
    then '1' end as alliance_flag,
    case when profiled_utm.profiled_utm_source~*'(adcanopus_IC7|adsplay_IC6|affle_IC2|intellectads_IC5|intellectads|mediazotic_IC3|svgmedia_IC4|ventesavenues|ventesavenues_IC1|netcore_IC10|yellowmars_IC12|adcanopus_IC7)'
    then '1' end as nw_flag,
    cp_latest.*, 
    (select token from user_tokens where user_id = a.user_id and is_active = 1 order by modified_ts desc limit 1) as tkn,
    regen_count.*
   FROM
    lenderoffers.applications a
    left join lateral (
    select 
    as2.slug as DSA_name
    from lender_base_campaign lbc
    left join agent_masters am
      on lbc.agent_master_id =am.id
    left join agent_supervisors as2
    	on am.agency_slug =as2.id
    /*left join lateral(
      select 
      customer_type,
      customer_profile_id,
      created_on as profiled_date
      from cm_cp_processed cp1
      where cp1.user_id = lbc.user_id  
        and to_char(cp1.created_on,'yyyy-mm-dd') >= to_char(lbc.updated_at ,'yyyy-mm-dd')
      order by cp1.created_on
      limit 1
    )cp on true*/
    where a.user_id =lbc.user_id 
      and lbc.is_profiled =1
      and lbc.lender_name ~* 'o2o'
      /*and cp.profiled_date>= '{{ START_DATE }}'
      and cp.profiled_date < '{{ END_DATE }}'*/
    )dsa on true
    left join user_details ud
      on a.user_id=ud.user_id
    LEFT JOIN LATERAL(
    SELECT  
    cp1.user_id,
    --cp1.customer_type,
     case when cp1.p_customer_type =1 then 'Green'
    when cp1.p_customer_type =2 then 'Red'
    when cp1.p_customer_type =3 then 'Amber'
    else 'NH' end as customer_type,
    cp1.customer_profile_id,
    cp1.created_on as profiled_date,
    usl.*
    FROM 
    cm_cp_processed cp1
    LEFT JOIN LATERAL(
        SELECT
            utm_source_text as profiled_utm_source,
            created_at
        FROM 
            utm_source_log usl
        WHERE
            usl.user_id = cp1.user_id
            AND usl.created_at < cp1.created_on
        ORDER BY usl.created_at DESC
        LIMIT 1
    ) usl ON TRUE
    WHERE
    a.user_id=cp1.user_id
    AND cp1.customer_profile_id = (SELECT customer_profile_id FROM cm_cp_processed WHERE user_id = cp1.user_id ORDER BY created_on LIMIT 1)
    --AND profiled_utm_source ~* '(flexiloans|homecredit|adcanopus_IC7|adsplay_IC6|affle_IC2|intellectads_IC5|intellectads|mediazotic_IC3|svgmedia_IC4|ventesavenues|ventesavenues_IC1)'
    ) profiled_utm on true
  left join lateral(
    select
    cp.p_credit_score as credit_score
    from cm_cp_processed cp
    where 
    cp.user_id=a.user_id
    AND cp.customer_profile_id = (SELECT customer_profile_id FROM cm_cp_processed WHERE user_id = cp.user_id ORDER BY created_on desc LIMIT 1)
    )cp_latest on true
left join lateral(
select
count(cl.id) as regen_lead_count,
string_agg(cl.lender, ' , ') as regen_lenders,
string_agg(cl.sku_name , ' , ') as regen_sku,
string_agg(cl.id::varchar , ' , ') as regen_leads
from consolidated_leads cl 
where 
cl.oic~*'lead'
--and cl.id =b.lead_id
and cl.user_id =a.user_id
)regen_count on true
where
    a.aip_approval_date >= '{{START_DATE}}'
    and a.aip_approval_date < '{{END_DATE}}'