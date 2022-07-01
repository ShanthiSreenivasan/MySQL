select DISTINCT ON (vs.user_id)
    vs.user_id,
    ud.phone_home,
    --cp.customer_type,
     case when p_customer_type =1 then 'Green'
    when p_customer_type =2 then 'Red'
    when p_customer_type =3 then 'Amber'
    else 'NH' end as customer_type,
    ud.first_name,
    ud.last_name,
    vs.lead_id,
    vs.service_type,
    first_profile_date,
    CASE
      WHEN (cp2.first_profile_date >= '{{PREV_MON_START}}' AND cp2.first_profile_date  < '{{END_DATE}}') THEN 'V0'
      WHEN (cp2.first_profile_date >= '{{PREV_5_MONTH_START}}' AND cp2.first_profile_date < '{{PREV_MON_START}}') THEN 'V1'
      WHEN (cp2.first_profile_date < '{{PREV_5_MONTH_START}}') then 'V2'
      ELSE to_char(cp2.first_profile_date,'yyyy-mm')
    END AS profile_vintage,
    get_latest_utm(vs.user_id, vs.payment_date) as utm_source_text,
    vs.payment_date,
    (
        select max(lc.updated_at)
        from leads l inner join lead_contacts lc on l.id = lc.lead_id
        where
            l.user_id = vs.user_id
            and contact_type = 'Sms'
            and description ~* 'promise to pay.*(bys|chr|bhr)'
    ) as sms_sent_on,
    --vs.sub_oic as oic,
     CASE
        WHEN usl.utm_source_text ~* '(^cis-pa|^email|^sms|sms$)' and usl.utm_source_text !~* 'monitoring' then 'PORTFOLIO'
        WHEN usl.utm_source_text ~* '^(CRM|byssms|chrsms|bhrsms)-(?=.*[0-9].*)(?=.*[A-Z].*)[A-Z0-9]+$' THEN substring(usl.utm_source_text from '-([A-Z0-9]+)$')
        WHEN usl.utm_source_text ~* '^(CRM|bysemail|chremail|bhremail)-(?=.*[0-9].*)(?=.*[A-Z].*)[A-Z0-9]+$' THEN substring(usl.utm_source_text from '-([A-Z0-9]+)$')
        WHEN usl.utm_source_text ~ '^(?=.*[0-9].*_)(?=.*[A-Z].*_)[A-Z0-9]+_androidapp' THEN substring(usl.utm_source_text from '^([A-Z0-9]+)_')
        WHEN lc.oic IS NOT NULL THEN lc.oic
        WHEN usl.utm_source_text ~* '(monitoring|^email-[RAG]2[RAG][di]?|^email-chra|^email-cis-(310|350|4050))' then 'System'
        ELSE 'System'
    END AS oic
       from product.vas_subscriptions vs
    LEFT JOIN
        user_details ud
        on ud.user_id = vs.user_id
      LEFT JOIN LATERAL (
        select 
            utm_source_text,
            created_at as login_date
        from 
            utm_source_log 
        where
            user_id = vs.user_id
            and created_at < vs.payment_date
        order by 
            created_at desc
        limit 1
    ) usl on true
   LEFT JOIN LATERAL (
        SELECT oic 
        FROM leads l inner join lead_contacts lc on l.id = lc.lead_id 
        WHERE 
            l.user_id = vs.user_id
            and description ~* '(bhrl)' 
            and lc.updated_at >= (vs.payment_date - interval '48 hours') 
            and lc.updated_at < vs.payment_date 
            and lc.contact_type ~* 'sms'
        ORDER BY 
            lc.updated_at 
        LIMIT 1
    ) lc ON TRUE
    LEFT JOIN
        cm_cp_processed cp
        on cp.customer_profile_id = vs.report_id
  left join lateral (select min(created_on) as first_profile_date
                      from cm_cp_processed 
                      where user_id = vs.user_id 
                      )cp2 on true
WHERE
    vs.payment_date >= '{{ START_DATE }}'
    AND vs.payment_date < '{{ END_DATE }}'
    AND vs.service_type in ('IDPP')
    {{ #OICS }}
    AND vs.sub_oic IN ({{ OICS }})
    {{ /OICS }}