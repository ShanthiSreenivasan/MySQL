--Referrals
with base as ( 
select 
a.user_id,
a.lead_id,
a.customer_type,
a.first_profile_date as profiled_date,
a.applied_date
from
lenderoffers.applications a
where 
a.applied_date >= '{{M2}}'
and a.applied_date <= '{{END_DATE}}'
and a.customer_type~*'(green|amber)'
)


SELECT
    case when cp.profiled_date>='{{M0}}'
    then 'M0'
    when cp.profiled_date>='{{M1}}' and cp.profiled_date<'{{M0}}'
    then 'M1'
    when cp.profiled_date>='{{M2}}' and cp.profiled_date<'{{M1}}'
    then 'M2'
    when cp.profiled_date<'{{M2}}'
    then 'M2+'
    end as "Vintage",
    count(distinct cp.user_id) filter (where cp.profiled_date>='{{M0}}') as "{{M0}} profiled",
    count(distinct cp.user_id) filter (where cp.profiled_date>='{{M1}}' and cp.profiled_date<'{{M0}}') as "{{M1}} profiled",
    count(distinct cp.user_id) filter (where cp.profiled_date>='{{M2}}' and cp.profiled_date<'{{M1}}') as "{{M2}} profiled" , 
    count(distinct cp.user_id) filter (where cp.profiled_date<'{{M2}}') as "M2+ profiled" , 
    
    count(distinct cp.lead_id) filter(where cp.applied_date>='{{M0}}') as "{{M0}} applied",
    count(distinct cp.lead_id) filter(where cp.applied_date>='{{M1}}' and cp.applied_date<'{{M0}}') as "{{M1}} applied",
    count(distinct cp.lead_id) filter(where cp.applied_date>='{{M2}}' and cp.applied_date<'{{M1}}') as "{{M2}} applied",
    count(distinct cp.lead_id) filter(where cp.applied_date<'{{M2}}') as "M2+ applied",
    
    count(distinct app_990.lead_id) filter(where app_990.booking_date >='{{M0}}') as "{{M0}} conversion",
    count(distinct app_990.lead_id) filter(where app_990.booking_date >='{{M1}}' and app_990.booking_date <'{{M0}}') as "{{M1}} conversion",
    count(distinct app_990.lead_id) filter(where app_990.booking_date >='{{M2}}' and app_990.booking_date <'{{M1}}') as "{{M2}} conversion",
    count(distinct app_990.lead_id) filter(where app_990.booking_date <'{{M2}}') as "M2+ conversion"

FROM
    base cp
    LEFT JOIN LATERAL (
       SELECT
            distinct cll.lead_id ,
            cll.user_id ,
--            cll.appops_status_code ,
            cll.booking_date
        FROM
            lenderoffers.applications cll 
        WHERE
            user_id = cp.user_id
            and cll.booking_date >= cp.profiled_date
            and cll.booking_date <= '{{END_DATE}}'
            and cll.appops_status_code~*'990'
    ) app_990 ON TRUE
  /*where 
  cp.profiled_date>='{{M2}}'*/
    group by 1