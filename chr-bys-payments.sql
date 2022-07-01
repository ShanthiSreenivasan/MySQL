select DISTINCT ON (vs.user_id)
    vs.user_id,
    ud.phone_home,
   -- cp.customer_type,
     case when p_customer_type =1 then 'Green'
    when p_customer_type =2 then 'Red'
    when p_customer_type =3 then 'Amber'
    else 'NH' end as customer_type,
    ud.first_name,
    ud.last_name,
    vs.lead_id,
    vs.service_type,
    vs.order_total,
    first_profile_date,
    ue.*,
    CASE
      WHEN (cp2.first_profile_date >= '{{PREV_MON_START}}' AND cp2.first_profile_date  < '{{END_DATE}}') THEN 'V0'
      WHEN (cp2.first_profile_date >= '{{PREV_5_MONTH_START}}' AND cp2.first_profile_date < '{{PREV_MON_START}}') THEN 'V1'
      WHEN (cp2.first_profile_date < '{{PREV_5_MONTH_START}}') then 'V2'
      ELSE to_char(cp2.first_profile_date,'yyyy-mm')
    END AS profile_vintage,
    case when
      month_diff(cp2.first_profile_date, '{{START_DATE}}'::date) <= 6 then
          'M-' || month_diff(cp2.first_profile_date, '{{START_DATE}}'::date)
      when (month_diff(cp2.first_profile_date, '{{START_DATE}}'::date) > 6 or cp2.first_profile_date is null) then
          'M-6 >'
  end as profile_vintage_M,
    saleable.nsaleable,  
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
from
    product.vas_subscriptions vs
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
    left join lateral (
        select 
            data ->> 'utm_term' as utm_term
        from
            user_events ue
        where   
            user_id = vs.user_id
            and created_at = usl.login_date
        limit 1
    ) ue on true 
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
  left join lateral (select max(customer_profile_id) as latest_profile_id
                      from cm_cp_processed 
                      where user_id = vs.user_id 
                      )cp3 on true
  left join lateral (
		select count(*) as nsaleable,
		(array_agg(short_name)) as lenders,
		(array_agg(account_no)) as account_no,
		(array_agg(at2.name)) as products,
		(array_agg(acs.account_status_text)) as account_status 
		from cm_ccap_processed cpa
				left join master_tables.lenders len
					on len.id = cpa.lender_id
				inner join master_tables.account_status acs
					on acs.id = cpa.p_account_status
					inner join master_tables.account_types at2 
					on at2.account_type_id=cpa.p_account_type
				inner join master_tables.lender_saleable_accounts lsa
					on lsa.lender_id = cpa.lender_id
				and lsa.product_family_id = cpa.product_family_id
				and lsa.account_status_id = acs.id
				and lsa.account_status_id not in (2,3,7,8,22,21)
				and lsa.is_active = 1
		where customer_profile_id = cp3.latest_profile_id
	)saleable on true                 
WHERE
    vs.payment_date >= '{{ START_DATE }}'
    AND vs.payment_date < '{{ END_DATE }}'
    AND vs.service_type in ('CHR', 'BYS', 'CHRA')
    {{ #OICS }}
    AND vs.sub_oic IN ({{ OICS }})
    {{ /OICS }}