SELECT
    cl.user_id,
   -- cp.customer_type,
     case when p_customer_type =1 then 'Green'
    when p_customer_type =2 then 'Red'
    when p_customer_type =3 then 'Amber'
    else 'NH' end as customer_type,
    get_latest_utm(cl.user_id, cl.updated_at) latest_utm_source,
    cp.p_credit_score as credit_score,
    age_from_dob(u.dob) AS age,
    u.nature_of_employment,
    u.gender,
    u.zip,
    lua.answers ->> 'city' as residence_city,
    p.locality as city_from_pincode,
    lua.answers ->> 'nth' as nth,
    lua.answers ->> 'pat' as pat,
    lua.answers ->> 'propertyCity' as property_city,
    lua.answers ->> 'salaryAccount' as salary_account,
    cl.id AS lead_id,
    cl.updated_at,
    cl.product,
    cl.product_status,
    cl.crm_status_text,
    first_call_stage.product_status first_call_stage,
    first_contact_stage.product_status first_contact_stage,
    cll.product_status prior_stage,
    cl.sku_name,
    call_stat.call_state,
    lo_funnel.*,
    a.applied_date,
    case when exists (
        select 1 
            from consolidated_leads 
        where 
            user_id = cl.user_id 
            and product = cl.product 
            and created_at < cl.created_at 
            and product_status ~* '10$'
    ) then 1 else 0 end as duplicate_lead
FROM
    consolidated_leads cl
    LEFT JOIN
        lenderoffers.applications a
        ON a.lead_id = cl.id
    LEFT JOIN LATERAL (
        SELECT max(customer_profile_id) AS customer_profile_id
        FROM cm_cp_processed
        WHERE 
            user_id = cl.user_id
            AND created_on <= coalesce(a.applied_date, 'infinity'::timestamp)
    ) active_prof ON TRUE
    LEFT JOIN
        cm_cp_processed cp
        ON cp.customer_profile_id = active_prof.customer_profile_id
    LEFT JOIN
        users u
        ON u.user_id = cp.user_id
    LEFT JOIN LATERAL (
        SELECT 
            CASE
                WHEN bool_and(call_logs.crm_status_text ~* '(nc|rnr|do not call)') THEN 
                    'NC'
                WHEN bool_or(call_logs.crm_status_text ~* '(nc|rnr|do not call)') THEN
                    'Contact'
                ELSE 'No Call'
            END AS call_state
        FROM
            call_logs
        WHERE 
            lead_id = cl.id
    ) call_stat ON TRUE
    left join lateral (
		select
			bool_or(product_status ~* '021$')::integer pass_021,
			bool_or(product_status ~* '022$')::integer pass_022,
			bool_or(product_status ~* '03$')::integer pass_03,
			(bool_or(product_status ~* '88$') and not bool_or(product_status ~* '05$'))::integer pass_88,
			bool_or(product_status ~* '05$')::integer pass_05,
			bool_or(product_status ~* '051$')::integer pass_051,
			bool_or(product_status ~* '06$')::integer pass_06,
			bool_or(product_status ~* '07$' and a.applied_date is not null)::integer pass_07
		from
			consolidated_lead_logs
		where
			lead_id = cl.id
	) lo_funnel on true
    LEFT JOIN LATERAL (
        select product_status
        from call_logs
        where lead_id = cl.id
        order by log_date limit 1
    ) first_call_stage on true
    LEFT JOIN LATERAL (
        select 
            jsonb_object_agg(coalesce(slug, question_field_name), answer) as answers
        from (
            SELECT DISTINCT on (q.question_id)
                q.question_id,
                q.slug,
                q.question_field_name,
                lua.answer
            from
                lender_user_answers lua
                inner join
                    master_tables.questions q
                    on q.question_id = lua.question_id
            WHERE
                lua.lead_id = cl.id
            order by q.question_id, lua.created_at desc
        ) base
    ) lua on true
    LEFT JOIN LATERAL (
        select product_status
        from call_logs
        where lead_id = cl.id and crm_status_text !~* '(nc|rnr|do not call)'
        order by log_date limit 1
    ) first_contact_stage on true
    LEFT JOIN LATERAL (
        SELECT product_status 
        FROM (
            select product_status, log_updated_at, dense_rank() over (order by log_updated_at desc, product_status desc) rank
            from consolidated_lead_logs 
            where lead_id = cl.id
        ) samp 
        WHERE rank = 2
        limit 1
    ) cll on true
    LEFT JOIN  
        master_tables.pincodes p
        on p.pincode = (lua.answers ->> 'residentialPincode')
    WHERE
        date(cl.created_at) BETWEEN '{{START_DATE}}' AND '{{END_DATE}}'
        AND cl.type = 'LenderOffer';