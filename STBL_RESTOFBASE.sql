--drop table test.rishabh_base
create table test.rishabh_base as(
with base as(
select * from(
select 
--distinct gl.user_id,
gl.user_id ,
coalesce (minv.monthly_income ,gl.monthly_income) as monthly_income,
gl.employment_type ,
latest_credit_score credit_score,
latest_customer_type, 
salary_receive_type,
gl."age",
cpa.*,
gl.first_profile_date,
gl.latest_profile_date,
gl.phone_home,
gl.email_id,
gl.first_name,
gl.last_name,
gl.referral_code,
gl.salary_account,
gl.city,
gl.pincode
--gl.latest_login_date
from test.green_ltd_2201 gl
left join test.monthly_income_new_v2 minv 
	on gl.user_id =minv.user_id 
--left join test.monthly_income_new min2 
--	on gl.user_id =min2.user_id
left join lateral  (
select product_status as last1mnth_status
from consolidated_leads a 
where a.user_id=gl.user_id
and  a.created_at >='2021-12-01'
and a.product_status ='STBL-88'
)  cpa on true
where 
latest_credit_score>='500'
and (gl.employment_type~*'salaried' or gl.employment_type is null)
and pincode not between '500000' and '550000'
)b
where monthly_income>='10000'
)
select * from base
--select 
--count(distinct user_id)
--from test.green_ltd_2201 gl
--from base
--where 
--last1mnth_status is null
)

--,
create table test.rajan_stbl_roi as(
with base_2 as(
select
b.*,
cashe.*,
--ps.*,
es_1.*,
es_2.*,
mv_a.*,
mv_b.*,
mv_c.*
--from base b
from test.rishabh_base b
left join lateral 
(select '1' as cashe_flag 
from test.rajan_cashe_flag_APRIL_green cashe
	where cashe.user_id=b.user_id
)cashe on true
--left join lateral(
--select '1' as ps_flag 
--from test.rajan_ps_green ps
--	where b.user_id=ps.user_id 
--)ps on true
left join lateral(
select '1' as es_tier1_flag 
from test.rajan_es_eligible_APRIL_tier1_green es1
	where b.user_id=es1.user_id 
)es_1 on true
left join lateral(
select '1' as es_tier2_flag 
from test.rajan_es_eligible_APRIL_tier2_green es2
	where b.user_id=es2.user_id 
)es_2 on true
left join lateral(
select '1' as mv_A_flag 
from test.rajan_mv_flag_APRIL_green_typeA mva
	where b.user_id=mva.user_id 
)mv_a on true
left join lateral(
select '1' as mv_B_flag 
from test.rajan_mv_flag_APRIL_green_typeB mvb
	where b.user_id=mvb.user_id 
)mv_b on true
left join lateral(
select '1' as mv_C_flag 
from test.rajan_mv_flag_APRIL_green_typeC mvc
	where b.user_id=mvc.user_id 
)mv_c on true
where 
last1mnth_status is null 
)
--es_green as(
--select user_id from test.rajan_es_eligible_APRIL_tier1_green
--union
--select user_id from test.rajan_es_eligible_APRIL_tier2_green
--),
--base_3 as (
--select
--b.*,
--es.*
--from base_2 b
--left join lateral 
--(
--select '1' as es_flag 
--from es_green es
--where b.user_id=es.user_id
--)es on true
--),
--mv_green as(
--select user_id from test.rajan_mv_flag_APRIL_green_typeA
--union
--select user_id from test.rajan_mv_flag_APRIL_green_typeB
--union
--select user_id from test.rajan_mv_flag_APRIL_green_typeC
--),
--base_4 as (
--select
--b.*,
--mv.*
--from base_3 b
--left join lateral 
--(
--select '1' as mv_flag 
--from mv_green mv
--where b.user_id=mv.user_id
--)mv on true
--),
--,base_5 as (
--SELECT
--    b.*,
--    (SELECT created_on FROM customer_profiles WHERE user_id = b.user_id order by created_on LIMIT 1) as first_profile_date,
--    (SELECT created_on FROM customer_profiles WHERE user_id = b.user_id order by created_on desc LIMIT 1) as latest_profile_date,
--    ud.phone_home,
--    ud.email_id,
--    ud.first_name,
--    ud.last_name,
--    u.referral_code,
--    (select token from user_tokens where user_id = u.user_id and is_active = 1 order by modified_ts desc limit 1) as tkn,
----    coalesce(lua.employment_type, u.nature_of_employment) as employment_type,
----    coalesce(lua.monthly_income,(SELECT composite_income FROM user_income WHERE user_id = b.user_id ORDER BY created_at DESC LIMIT 1)) as monthly_income,
--    lua.salary_account,
--    lua.work_exp,
----    coalesce(lua.age, age_from_dob(u.dob)) as age,
--    coalesce(lua.city, u.city) as city,
--    coalesce(lua.residential_pincode, u.zip) as pincode,
--    (SELECT created_at FROM utm_source_log WHERE user_id = b.user_id ORDER BY created_at DESC LIMIT 1) as latest_login_date
--FROM 
--    base_2 b
----    inner join  
----        customer_profiles cp
----        ON cp.user_id = b.user_id
----        AND cp.customer_profile_id = (SELECT customer_profile_id FROM customer_profiles WHERE user_id = b.user_id and is_active = 1 order by created_on DESC limit 1)
--	left join 
--        user_details ud 
--        on ud.user_id = b.user_id
--    left join 
--        users u 
--        on u.user_id = b.user_id
--    left join 
--        lenderoffers.user_inputs lua 
--        on lua.user_id = b.user_id )        
select 
b.*,
case when b.monthly_income>='15000'
and b."age">='22'
and b."age"<='45'
then '1'
end as KB_flag,
uai.usr_level_whatsapp_consent,
(select token from user_tokens where user_id = b.user_id and is_active = 1 order by modified_ts desc limit 1) as tkn,
(SELECT created_at FROM utm_source_log WHERE user_id = b.user_id ORDER BY created_at DESC LIMIT 1) as latest_login_date
--from base_5 b
from base_2 b
left join users_additional_info uai
	on b.user_id=uai.user_id
--where 
--pincode not between '500000' and '550000' and
--b.user_id not in(select user_id from test.homecredit_first_profiled_180_day_June)
)

select 
b.*
--count(*)
--count(distinct user_id)
from test.rajan_stbl_roi b
WHERE
b.user_id not in(select user_id from test.homecredit_first_profiled_180_day_June)
--kb_flag='1'
--cashe_flag='1'
--mv_A_flag='1'
--mv_B_flag='1'
--mv_C_flag='1'
--es_tier2_flag='1'
--es_tier1_flag='1'