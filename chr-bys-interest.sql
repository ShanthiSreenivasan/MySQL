WITH vas_interest as (
    SELECT DISTINCT ON (cll.user_id)
        cll.user_id, cll.lead_id, cll.log_updated_at, cll.product_status
    FROM
        consolidated_lead_logs cll
    WHERE
        cll.product_status ~* '(chr|bys).*0[45]0$'
        and cll.log_updated_at >= '{{START_DATE}}'
        AND cll.log_updated_at < '{{END_DATE}}'
    ORDER BY
        cll.user_id, cll.log_updated_at DESC
)
SELECT
    vi.*,
    --cp.customer_type,
    case when p_customer_type =1 then 'Green'
    when p_customer_type =2 then 'Red'
    when p_customer_type =3 then 'Amber'
    else 'NH' end as customer_type,
    cp.p_credit_score as credit_score,
    ud.phone_home,
    ud.email_id,
    ud.first_name,
    ud.last_name,
    coalesce(ui.city, u.city) as city,
	coalesce(ui.residential_pincode, u.zip) as pincode,
    cl.crm_status_text,
    calls.*,
    cp.no_of_active_accounts,
    coalesce(age_from_dob(u.dob),ui.age) as age,
    coalesce(ui.monthly_income,uim.composite_income) as monthly_income,
    cpa.* 
FROM
    vas_interest vi
    LEFT JOIN
        user_details as ud
        ON ud.user_id = vi.user_id
    LEFT JOIN
        cm_cp_processed as cp
        ON cp.user_id = vi.user_id
        and cp.customer_profile_id = (select max(customer_profile_id) from cm_cp_processed where user_id = cp.user_id)
    LEFT JOIN 
        lenderoffers.user_inputs ui
        ON ui.user_id = vi.user_id
    LEFT JOIN 
        users u
        ON u.user_id = vi.user_id
    LEFT JOIN 
        consolidated_leads cl
        ON cl.id = vi.lead_id
    left join lateral (
		 select count(*) as calls,
	         count(*) filter (where dispo not in ('NC','CB')) as contacted
	         from lenderoffers.lo_calls 
	         where user_id = vi.user_id
	         and log_date >= current_date - interval '3 months'
	)calls on true
    left join 
		user_income uim
		ON uim.user_id = cp.user_id
    LEFT JOIN LATERAL(
        SELECT
            bool_or(aty.name = 'credit card')::integer as has_credit_card,
            bool_or(aty.name = 'personal loan')::integer as has_personal_loan,
            bool_or(aty.name = 'consumer loan')::integer as has_consumer_loan
        FROM 
            cm_ccap_processed cpa
           	left join master_tables.account_types aty
	          on cpa.p_account_type =aty.account_type_id   
        WHERE
            cpa.customer_profile_id = cp.customer_profile_id
            AND p_is_active = 1
    ) cpa ON TRUE
WHERE
    NOT EXISTS (
        SELECT 1
        FROM product.vas_subscriptions
        WHERE user_id = vi.user_id and service_type in ('CHR', 'BYS', 'CHRA')
    )