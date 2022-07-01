select
    vs.user_id,
    ud.phone_home,
    --cp.customer_type,
     case when p_customer_type =1 then 'Green'
    when p_customer_type =2 then 'Red'
    when p_customer_type =3 then 'Amber'
    else 'NH' end as customer_type,
    ud.first_name,
    ud.last_name,
    vs.id as lead_id,
    vs.product_status,
    vs.type,
    get_latest_utm(vs.user_id, vs.modified_ts) as utm_source_text,
    vs.modified_ts,
    (
        select max(lc.updated_at)
        from leads l inner join lead_contacts lc on l.id = lc.lead_id
        where
            l.user_id = vs.user_id
            and contact_type = 'Sms'
            and description ~* 'promise to pay.*(bys|chr)'
    ) as sms_sent_on,
    vs.oic
from
    consolidated_leads vs
    LEFT JOIN
        user_details ud
        on ud.user_id = vs.user_id
    LEFT JOIN
        cm_cp_processed cp
        on cp.user_id = vs.user_id
        AND cp.customer_profile_id = (SELECT MAX(customer_profile_id) FROM cm_cp_processed WHERE user_id = vs.user_id)
WHERE
    vs.modified_ts >= '{{START_DATE}}'
    AND vs.modified_ts < '{{END_DATE}}'
    AND vs.product_status ~* '(CHR.+070|BYS.+070)'
    {{ #OICS }}
    AND vs.sub_oic IN ({{ OICS }})
    {{ /OICS }}