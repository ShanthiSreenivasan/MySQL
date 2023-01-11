create table test.rajan_everSTBL_base as(
with base as(
select
distinct glp.user_id ,
--coalesce(glp2.monthly_income ,glp2.composite_income ,glp2.composite_income_new ) as monthly_income,
coalesce (minv.monthly_income ,glp2.monthly_income_1 ,glp2.monthly_income_2 ,glp2.composite_income_new ,glp2.composite_income ,glp.eq_income ) as monthly_income ,
glp2.salary_receive_type ,
glp2.employment_type ,
--glp.pincode ,
glp.latest_credit_score credit_score,
glp3.first_profile_date ,
glp.latest_profile_date ,
glp.latest_profile_id as customer_profile_id,
glp.phone_home,
glp.email_id,
glp.first_name,
glp.last_name,
glp2.referral_code ,
glp2.salary_account ,
glp2.age ,
glp2.city ,
glp2.pincode ,
glp3.latest_login_date ,
cpa.*
from test.green_ltd_part1_0101 glp
left join test.green_ltd_part2_0101 glp2 
	on glp.user_id =glp2.user_id 
left join test.green_ltd_part3_0101 glp3 
	on glp.user_id =glp3.user_id 
left join test.green_ltd_part4_0101 glp4
	on glp.user_id =glp4.user_id
left join test.green_ltd_part5_0101 glp5
	on glp.user_id =glp5.user_id
left join test.green_ltd_part6_0101 glp6
	on glp.user_id =glp6.user_id
LEFT JOIN test.monthly_income_new_v2 minv 
	on glp.user_id =minv.user_id 
--left join test.monthly_income_new min2 	
--	on glp.user_id =min2.user_id 
left join lateral  (
select product_status as last1mnth_status
from consolidated_leads a 
where a.user_id=glp.user_id
and  a.created_at >='2021-12-01'
and a.product_status ='STBL-88'
)  cpa on true
inner join lateral  (
select user_id
from consolidated_leads a 
where a.user_id=glp.user_id
and a.product in('STBL')
)  cpa1 on true
)
select * from base
)

--select count(distinct user_id)from test.rajan_everSTBL_base
--where
--pincode not between '500000' and '550000'
--and user_id not in(select user_id from test.homecredit_first_profiled_180_day_June)
--and  last1mnth_status is null

--base_2 as(
--SELECT
--b.*,
--cpa1.*
--FROM base b
--INNER JOIN lateral(
--SELECT
--'1' as active_closed_stbl
--FROM customer_profile_accounts cpa 
--where b.customer_profile_id=cpa.customer_profile_id 
----and (cpa.is_active~*'Yes' or cpa.is_closed~*'Yes')
--and cpa.product_family_id =23
--)cpa1 on true
--)
--select * from base_2)

create table test.rajan_everSTBL_base_2 as(
with base_3 as (
SELECT
    b.*,
--    (SELECT created_on FROM customer_profiles WHERE user_id = b.user_id order by created_on LIMIT 1) as first_profile_date,
--    cp.created_on as latest_profile_date,
--    ud.phone_home,
--    ud.email_id,
--    ud.first_name,
--    ud.last_name,
--    u.referral_code,
    (select token from user_tokens where user_id = b.user_id and is_active = 1 order by modified_ts desc limit 1) as tkn
--    coalesce(lua.employment_type, u.nature_of_employment) as employment_type,
--    coalesce(lua.monthly_income,(SELECT composite_income FROM user_income WHERE user_id = b.user_id ORDER BY created_at DESC LIMIT 1)) as monthly_income,
--    lua.salary_account,
--    lua.work_exp,
--    coalesce(lua.age, age_from_dob(u.dob)) as age,
--    coalesce(lua.city, u.city) as city,
--    coalesce(lua.residential_pincode, u.zip) as pincode,
--    (SELECT created_at FROM utm_source_log WHERE user_id = b.user_id ORDER BY created_at DESC LIMIT 1) as latest_login_date
FROM 
--    base b
    test.rajan_everSTBL_base b
--    inner join  
--        customer_profiles cp
--        ON cp.user_id = b.user_id
--        AND cp.customer_profile_id = (SELECT customer_profile_id FROM customer_profiles WHERE user_id = b.user_id and is_active = 1 order by created_on DESC limit 1)
--	inner join 
--        user_details ud 
--        on ud.user_id = b.user_id
--    inner join 
--        users u 
--        on u.user_id = b.user_id
--    left join 
--        lenderoffers.user_inputs lua 
--        on lua.user_id = b.user_id 
        ),
base_4 as(        
select 
b.* ,
uai.usr_level_whatsapp_consent 
from base_3 b
left join users_additional_info uai
	on b.user_id=uai.user_id
where 
pincode not between '500000' and '550000'
and b.user_id not in(select user_id from test.homecredit_first_profiled_180_day_June)
),
base_5 as(
select
b.*,
stbl_07.*
from base_4 b
left join lateral(
select 
coalesce (bool_or(a.product in('STBL') and a.applied_date>='2021-12-01')::integer,0)as applied_atleast1stbl_30days,
coalesce (bool_or(a.product in('STBL') and a.applied_date<'2021-12-01' and a.applied_date>='2021-11-01')::integer,0)as applied_atleast1stbl_60days,
coalesce (bool_or(a.product in('STBL') and a.applied_date<'2021-11-01' and a.applied_date>='2021-10-01')::integer,0)as applied_atleast1stbl_90days,
coalesce (bool_or(a.product in('STBL') and a.applied_date<'2021-10-01')::integer,0)as applied_atleast1stbl_90plusdays,
coalesce (bool_or(a.product in('STBL') and a.lender~*'cash')::integer,0)as applied_atleast1stbl_cashe,
coalesce (bool_or(a.product in('STBL') and a.lender~*'money')::integer,0)as applied_atleast1stbl_MV,
coalesce (bool_or(a.product in('STBL') and a.lender~*'early')::integer,0)as applied_atleast1stbl_ES,
coalesce (bool_or(a.product in('STBL') and a.lender~*'kredit')::integer,0)as applied_atleast1stbl_KB,
coalesce (bool_or(a.product in('STBL') and a.lender~*'home')::integer,0)as applied_atleast1stbl_HC,
coalesce (bool_or(a.product in('STBL'))::integer,0)as applied_atleast1stbl,
coalesce (bool_or(a.product in('STBL') and a.appops_status_code in('710'))::integer,0)as applied_stbl_710
from lenderoffers.applications a
where
a.user_id =b.user_id
and a.product in('STBL')
)stbl_07 on true
)
--select count(distinct user_id)from base_4
select * from base_5
)

select * from test.rajan_everSTBL_base_2