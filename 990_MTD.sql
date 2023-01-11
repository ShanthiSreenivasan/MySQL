WITH base AS (
SELECT
    la.user_id,
    la.lead_id,
    la.lender,
    la.appops_status_code,
    la.product_status,
    la.product ,
    la.applied_date,
    la.first_profile_date 
FROM 
    lenderoffers.applications la
    inner join lateral
    (select lead_id 
     from consolidated_lead_logs cll 
     where 
     cll.lead_id=la.lead_id 
     and cll.log_created_at>='2021-08-01'
     order by log_created_at 
     limit 1
    )cll on true
),
base_2 as(
SELECT
    b.*,
    (SELECT created_on FROM customer_profiles WHERE user_id = b.user_id order by created_on LIMIT 1) as first_profile_date,
    cp.created_on as latest_profile_date,
    cp.customer_type,
    cp.credit_score,
    ud.phone_home,
    ud.email_id,
    ud.first_name,
    ud.last_name,
    u.referral_code,
    (select token from user_tokens where user_id = u.user_id and is_active = 1 order by modified_ts desc limit 1) as tkn,
    coalesce(lua.employment_type, u.nature_of_employment) as employment_type,
    coalesce(lua.monthly_income,(SELECT composite_income_new FROM user_income WHERE user_id = b.user_id ORDER BY created_at DESC LIMIT 1),(SELECT composite_income FROM user_income WHERE user_id = b.user_id ORDER BY created_at DESC LIMIT 1)) as monthly_income,
    lua.salary_account,
    coalesce(lua.age, age_from_dob(u.dob)) as age,
    coalesce(lua.city, u.city) as city,
    u.state,
    coalesce(lua.residential_pincode, u.zip) as pincode,
    (SELECT created_at FROM utm_source_log WHERE user_id = b.user_id ORDER BY created_at DESC LIMIT 1) as latest_login_date

FROM 
    base b
    inner join  
        customer_profiles cp
        ON cp.user_id = b.user_id
        AND cp.customer_profile_id = (SELECT customer_profile_id FROM customer_profiles WHERE user_id = b.user_id and is_active = 1 order by created_on DESC limit 1)
    
	inner join 
        user_details ud 
        on ud.user_id = b.user_id
    inner join 
        users u 
        on u.user_id = b.user_id
    left join 
        lenderoffers.user_inputs lua 
        on lua.user_id = b.user_id 
)

select * from base_2 b