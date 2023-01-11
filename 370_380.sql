create table test.rajan_370380 as(
WITH base AS (
SELECT
    la.user_id,
    la.lead_id,
    la.lender,
    la.appops_status_code,
    la.product_status,
    la.product ,
    la.applied_date,
    case when scuf.lender~*'shriram' and scuf.applied_date>='2020-01-01'
    then 1
    end as SCUF_applied_2years_flag,
    case when scuf.lender~*'shriram'
    then 1
    end as SCUF_applied_LTD_flag
FROM 
    lenderoffers.applications la
left join lateral(
	select
	*
	from lenderoffers.applications app
	where
	app.user_id =la.user_id
	)scuf on true
WHERE
    la.appops_status_code~*'(370|380)'
    and la.applied_date>='2021-01-01'
--    and la.updated_at>='2021-07-01'
--    and la.updated_at<'2021-08-01'
),
base_2 as(
SELECT
    b.*,
    (SELECT created_on FROM cm_cp_processed WHERE user_id = b.user_id order by created_on LIMIT 1) as first_profile_date,
    cp.created_on as latest_profile_date,
    case when cp.p_customer_type=1 then 'Green'
    when cp.p_customer_type=2 then 'Red'
    when cp.p_customer_type=3 then 'Amber'
    end as customer_type,
    cp.p_credit_score,
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
        cm_cp_processed cp
        ON cp.user_id = b.user_id
        AND cp.customer_profile_id = (SELECT customer_profile_id FROM cm_cp_processed WHERE user_id = b.user_id and is_active = 1 order by created_on DESC limit 1)
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
select 
b.*,
uai.usr_level_whatsapp_consent,
lai.whatsapp_consent as lead_level_whatsapp_consent
from base_2 b
left join users_additional_info uai 
	on b.user_id=uai.user_id 
left join lead_additional_info lai
	on b.lead_id=lai.lead_id
where
b.user_id not in(select user_id from test.homecredit_first_profiled_180_day_june hfpdj)
--and b.latest_login_date>='2021-07-01'
)

select 
--count(distinct lead_id)
* 
from test.rajan_370380
--where latest_login_date>='2021-07-01'