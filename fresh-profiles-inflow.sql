select
    cp.user_id,
   -- cp.customer_type,
     case when p_customer_type =1 then 'Green'
    when p_customer_type =2 then 'Red'
    when p_customer_type =3 then 'Amber'
    else 'NH' end as customer_type,
    date(cp.created_on) as profile_date,
    call_metrics.*,
    log_metrics.*,
    (app.first_applied_date is not null)::integer as is_applied,
    app.*,
    first_call_met.lead_type as first_call_lead_type,
    first_call_met.lead_id as first_call_lead_id,
    first_call_met.product as first_call_product,
    first_call_met.stage as first_call_stage,
    first_call_met.crm_status_text as first_call_crm,
    first_call_met.log_date as first_log_date,
    case
		when dialer.dialer_crm is null then 'Other M0'
		else dialer.dialer_crm
	end as dialer_crm
from
    cm_cp_processed cp
    left join lateral (
        select min(applied_date) as first_applied_date
        from lenderoffers.applications
        where user_id = cp.user_id
    ) app on true
    LEFT JOIN LATERAL (
        SELECT
            (count(*) > 0)::integer as is_called,
            bool_or(dispo != 'NC')::integer as is_contacted,
            count(*) as attempts,
            count(*) filter (where dispo = 'NC') as eff_attempts
        FROM
            lenderoffers.lo_calls
        where
            user_id = cp.user_id
            and log_date < coalesce(app.first_applied_date + interval '2 minutes', 'infinity'::timestamp)
    ) call_metrics ON TRUE
    left join lateral (
        select
            bool_or(lead_type = 'LenderOffer')::integer is_prod_int,
            CASE
                WHEN bool_or(product_status ~* '07$') THEN
                    '07'
                WHEN bool_or(product_status ~* '(110|210)$') THEN
                    '110'
                WHEN bool_or(product_status ~* '(051?|06)$') THEN
                    '05'
                WHEN bool_or(product_status ~* '88$') THEN
                    '88'
                WHEN bool_or(product_status ~* '02[23]$') THEN
                    '022'
                WHEN bool_or(product_status ~* '021?$') THEN
                    '02'
                ELSE '00'
            END AS progress,
            string_agg(DISTINCT split_part(product_status, '-', 1), ' | ') FILTER (WHERE product_status ~* '05$') AS offered_products,
            string_agg(DISTINCT split_part(product_status, '-', 1), ' | ') FILTER (WHERE product_status ~* '0(1|21?)$') AS interest_shown_products,
            string_agg(DISTINCT split_part(product_status, '-', 1), ' | ') FILTER (WHERE product_status ~* '^(CC|pl|sbpl)-0(1|21?)$') AS core_interest_shown_products,
            string_agg(DISTINCT split_part(product_status, '-', 1), ' | ') FILTER (WHERE product_status ~* '88$') AS rejected_products
        from
            consolidated_lead_logs
        where
            user_id = cp.user_id
            and lead_type in ('User', 'LenderOffer')
    ) log_metrics on true
    left join lateral (
        select
            lead_type,
            lead_id,
            product,
            stage,
            crm_status_text,
            log_date
        from
            lenderoffers.lo_calls
        where
            user_id = cp.user_id
            and log_date < coalesce(app.first_applied_date + interval '2 minutes', 'infinity'::timestamp)
        order by
            log_date
        limit 1
    ) first_call_met on true
    left join lateral (
        select crm_status_text as dialer_crm
        from consolidated_lead_logs
        where
            user_id = cp.user_id
            and crm_status_text ~* '^dial'
        order by
            log_updated_at desc
        limit 1
    ) dialer on true
where
    cp.customer_profile_id = (select min(customer_profile_id) from cm_cp_processed where user_id = cp.user_id)
    and date(cp.created_on) between '{{START_DATE}}' and '{{END_DATE}}'
    and cp.p_customer_type is not null