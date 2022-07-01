SELECT
a.lead_id,
a.user_id,
--rajan changes start
ud.first_name,
ud.phone_home,
--rajan changes end
a.offer_application_number,
a.offer_reference_number,
a.first_profile_date,
a.profile_id,
a.customer_type,
a.applied_date,
a.send_to_lender_date,
a.not_interested_date,
a.date_of_referral,
a.feedback_received_date,
a.aip_approval_date,
a.booking_date,
a.applied_oic,
a.send_to_lender_oic,
a.not_int_oic,
a.date_of_referral_oic,
a.product,
a.sku_slug,
a.loan_amount,
a.tenor,
a.emi,
a.loan_disbursed_amount,
a.assigned_oic,
a.product_status,
a.appops_status_code,
a.document_status,
a.appointment_date,
a.followup_date,
a.utm_source,
a.utm_content,
a.utm_term,
a.utm_medium,
a.utm_campaign,
dsa.*,
profiled_utm.profiled_utm_source,
case when profiled_utm.profiled_utm_source~*'(homecredit|flexiloans)'
and profiled_utm.profiled_utm_source not in('homecredit')
then '1' end as alliance_flag,
case when profiled_utm.profiled_utm_source~*'(adcanopus_IC7|adsplay_IC6|affle_IC2|intellectads_IC5|intellectads|mediazotic_IC3|svgmedia_IC4|ventesavenues|ventesavenues_IC1|netcore_IC10|yellowmars_IC12|adcanopus_IC7)'
then '1' end as nw_flag,
a.device_type,
a.device_name,
a.login_date,
a.modified_ts,
a.inserted_at,
case when a.lender ~* 'idfc' and sku_slug ~* 'IDFCPROGB' then 'IDFCPROGB'
else a.lender end as lender,
a.sku,

/*    case when lender ~* 'shriram' and applied_oic ~* '(system|andrio|referral|port)' and cll3.oic_05  !~*  '(system|andrio|referral|port|lead)' then cll3.oic_05
         when lender ~* 'shriram' and applied_oic ~* '(system|andrio|referral|port)' and cll3.oic_06  !~*  '(system|andrio|referral|port|lead)' then cll3.oic_06
         when lender ~* 'shriram' and applied_oic ~* '(system|andrio|referral|port)' and cll3.oic_061 !~*  '(system|andrio|referral|port|lead)' then cll3.oic_061
         when lender ~* 'shriram' and applied_oic ~* '(system|andrio|referral|port)' and cll3.oic_021 !~*  '(system|andrio|referral|port|lead)' then cll3.oic_021
         when lender ~* 'shriram' and applied_oic ~* '(system|andrio|referral|port)' and cll3.oic_022 !~*  '(system|andrio|referral|port|lead)' then cll3.oic_022
         when lender ~* 'shriram' and applied_oic ~* '(system|andrio|referral|port)' and cll3.oic_03  !~*  '(system|andrio|referral|port|lead)' then cll3.oic_03
         when lender ~* 'shriram' and applied_oic ~* '(system|andrio|referral|port)' and cll3.oic_01  !~*  '(system|andrio|referral|port|lead)' then cll3.oic_01
         
         else applied_oic end as attribution, */

case 
    when a.sku ~* 'shriram' and coalesce(u.zip,lua.residential_pincode,ffi.residential_pincode) not similar to  '(50|51|52|53|54|55)%' and applied_oic ~* '(system|andri|port|referral)' then coalesce(oic, applied_oic) 
	  when a.sku ~* 'Allahabad' and applied_oic ~* '(system|andri|port|referral)' then coalesce(oic, applied_oic) 
    when sku_slug ~* 'IDFCPROGB' and applied_oic ~* '(system|andri|port|referral)' then coalesce(oic, applied_oic)
    when a.sku ~* 'HINS' and applied_oic ~* '(system|andri|port|referral)' then coalesce(hins_oic, applied_oic)
else a.applied_oic end as attribution, 
	   
    cll.latest_crm,
    pass_met.*,
    call_att.*,
    CASE WHEN month_diff(first_profile_date, applied_date) < 2 THEN
        'M' || month_diff(first_profile_date, applied_date)
    ELSE 'M1+' END AS profile_vintage,
    ffi.nth,
    ffi.pat,
    ffi.employment_type,
    ffi.city,
    convert_to_numeric(ffi.misc_fields ->> 'itrNetTakeHome') as itr_net_take_home,
    u.state,
    case when a.applied_date is not null and a.sku ~* 'Shriram' then 1
         when a.sku ~* 'RBL CC' and a.applied_date is not null then 2
         when a.sku=sp.sku and a.customer_type=sp.customer_type and sp.api_flag=1  and a.appops_status_code >= '390' then sp.points
         when a.sku=sp.sku and a.customer_type=sp.customer_type and sp.api_flag=0  and a.appops_status_code >='250' then sp.points
         else 0 end as points_sku,

    ffi.residential_pincode,
    (
      SELECT regexp_replace(description::text, '[\n\r]+'::text, ' '::text, 'g'::text)
      FROM lead_contacts
      WHERE lead_id = a.lead_id AND contact_type = 'Notes'
      ORDER BY updated_at DESC
      LIMIT 1
    ) as description,
    CASE WHEN rl.status in (1, 2) THEN 1 ELSE 0 END AS is_rbl_cc_api_approved,
    CASE WHEN (select processing_status_code from sbi_logs where lead_id = a.lead_id and api_level = 'AIS' order by id desc limit 1) IN (1) THEN 1 ELSE 0 END AS is_sbi_cc_api_approved,
    lender_fb.*,
    case when a.sku ~* 'shriram' and coalesce(u.zip,lua.residential_pincode,ffi.residential_pincode) similar to  '(50|51|52|53|54|55)%' then a.sku || ' AP & TG'
       when a.sku ~* 'shriram' and coalesce(u.zip,lua.residential_pincode,ffi.residential_pincode) not similar to  '(50|51|52|53|54|55)%' then a.sku || ' ROI'
       when a.sku ~* 'idfc' and sku_slug ~* 'IDFCPROGB' then 'IDFCPROGB PL'	     
	     else a.sku end as sku_name,
    regen.first_oic , 
    cp_latest.*, 
    consent.*,
    (select token from user_tokens where user_id = a.user_id and is_active = 1 order by modified_ts desc limit 1) as tkn,
    regen_count.*,
    lenders_present.* 
FROM
    lenderoffers.applications a
    LEFT JOIN
      rbl_logs AS rl
      on rl.lead_id = a.lead_id
      and rl.id =  (
        select max(rl2.id)
        from
          rbl_logs rl2
          left join
            master_tables.rbl_api_levels ral
            on rl2.rbl_api_level_id = ral.id
            where
              rl2.lead_id = rl.lead_id
              and coalesce(rl2.api_level, ral.api_level) = 'initial'
      )
    left join lateral (
        select
            crm_status_text as latest_crm
        from
            consolidated_lead_logs
        where
            lead_id = a.lead_id
            and source = 'CRM'
            and lead_log_type = 'Status Change'
            and crm_status_text !~* '(^dial|call drop)'
        order by
            log_updated_at desc
        limit 1
    ) as cll on true
    left join lateral (
        select
            bool_or(product_status ~* '423$')::integer as pass_423,
            bool_or(appops_status_code = '280')::integer as pass_280,
            bool_or(appops_status_code = '300')::integer as pass_300,
            (bool_or(appops_status_code >= '390') and not bool_or(appops_status_code = '380'))::integer as pass_390,
            bool_or(appops_status_code in ('350', '360', '370', '380'))::integer as pass_stage1_reject_stage,
            bool_or(appops_status_code >= '490')::integer as pass_490,
            bool_or(appops_status_code >= '690')::integer as pass_690,
            bool_or(appops_status_code >= '270')::integer as pass_270,
            bool_or(appops_status_code > '270' and appops_status_code not in ('275', '280', '421', '423'))::integer as has_received_feedback
        from
            consolidated_lead_logs
        where
            lead_id = a.lead_id
    ) pass_met on true
    LEFT JOIN LATERAL (
        SELECT
            count(*) as attempts,
            count(*) filter (where crm_status_text !~* '(nc|rnr|ring no)') as effective_attempts,
            (
              array_agg(regexp_replace(crm_status_text, '[\n\r]+', ' ', 'g') order by log_updated_at desc)
              filter (where appops_status_code = '210')
            )[1] as crm_210
        FROM
            consolidated_lead_logs
        WHERE
            lead_id = a.lead_id
            and source = 'CRM'
            and lead_log_type = 'Status Change'
            and crm_status_text !~* '(^dial|call drop)'
            and log_updated_at > a.applied_date
    ) call_att ON true
    LEFT JOIN
        lenderoffers.form_field_inputs ffi
        on ffi.lead_id = a.lead_id
    LEFT JOIN
        users u
        on u.user_id = a.user_id
    left join 
		    lenderoffers.user_inputs lua 
		    on lua.user_id = a.user_id    
    LEFT JOIN LATERAL (
        SELECT
            description as lender_notes,
            reason_for_rejection as lender_reject_reason
        FROM
            lead_contacts
        WHERE
            lead_id = a.lead_id
            and contact_type = 'Lender Notes'
        ORDER BY
            id desc
        LIMIT 1
    ) lender_fb ON TRUE
/*left join lateral (select 
                             max(updated_by_oic) filter (where product_status ~* '05' and updated_by_oic !~* '(system|port|engage|andri|lead)') as oic_05,
                             max(updated_by_oic) filter (where product_status ~* '06' and updated_by_oic !~* '(system|port|engage|andri|lead)') as oic_06,
                             max(updated_by_oic) filter (where product_status ~* '061' and updated_by_oic !~* '(system|port|engage|andri|lead)') as oic_061,
                             max(updated_by_oic) filter (where product_status ~* '021' and updated_by_oic !~* '(system|port|engage|andri|lead)') as oic_021,
                             max(updated_by_oic) filter (where product_status ~* '022' and updated_by_oic !~* '(system|port|engage|andri|lead)') as oic_022,
                             max(updated_by_oic) filter (where product_status ~* '03' and updated_by_oic !~* '(system|port|engage|andri|lead)') as oic_03,
                             max(updated_by_oic) filter (where product_status ~* '01' and updated_by_oic !~* '(system|port|engage|andri|lead)') as oic_01
                             from 
                             consolidated_lead_logs 
                             where 
                             lead_id=a.lead_id
                             and product_status   ~* '(06|05|061|021|022|03|01)'
                             and updated_by_oic  !~* '(system|port|referral|andri|lead)'
                             and crm_status_text !~* '(call back|not contactable|RNR|NC|NI)' 
                  ) cll3 on true */
left join lateral( select 
                       dispo,log_date,oic,stage
                       from 
                       lenderoffers.lo_calls 
                       where user_id=a.user_id
                       --and dispo ~* '(NI|CB|PTP|NE|DNC|others|PTD|DOC)' 
                       and log_date >= applied_date - interval '40 Days'
                       and applied_date >= log_date
                       and oic !~* 'system'
                       and stage ~* '(06|05|021|03|022|51|50|99|110|88|89|07|23)'
                       --and product ~* 'pl'
                       order by log_date desc 
                       limit 1                      
                    )cll1 on true
/*LEFT JOIN LATERAL(
        select 
            CASE 
            WHEN usl.utm_source_text ~* '^(CRM|byssms|chrsms)-(?=.*[0-9].*)(?=.*[A-Z].*)[A-Z0-9]+$' THEN substring(usl.utm_source_text from '-([A-Z0-9]+)$')
            END as hins_oic
            
        from 
            usl_currentmonth usl
        where
            user_id = a.user_id
            and created_at < a.applied_date
        order by 
            created_at desc
        limit 1
    ) cll2 ON TRUE  */                
                  
LEFT JOIN LATERAL(
        select 
            updated_by_oic as hins_oic            
        from 
            consolidated_lead_logs 
        where
            lead_id = a.lead_id
            and log_updated_at >= a.applied_date - interval '48 Hours'
            and log_updated_at < a.applied_date
            ---and product_status ~* '06'
            and updated_by_oic !~* '(system|andri|port|referral)'
        order by 
            log_updated_at DESC
        limit 1
    ) cll2 ON TRUE

left join master_tables.sku_points sp
on a.sku=sp.sku
and a.customer_type=sp.customer_type
left join lateral(
SELECT
fs1.lead_id,
--(array_agg(submitted_by order by submitted_at))[1] as first_oic,
(array_agg(updated_by_oic order by log_updated_at) filter (where product_status ~* '03$'))[1] as first_oic
--from lenderoffers.form_submits fs1
FROM consolidated_lead_logs fs1
where a.lead_id=fs1.lead_id
group by fs1.lead_id
)regen on true
left join lateral (
    select 
    as2.slug as DSA_name
    from lender_base_campaign lbc
    left join agent_masters am
      on lbc.agent_master_id =am.id
    left join agent_supervisors as2
    	on am.agency_slug =as2.id
    /*left join lateral(
      select 
      customer_type,
      customer_profile_id,
      created_on as profiled_date
      from cm_cp_processed cp1
      where cp1.user_id = lbc.user_id  
        and to_char(cp1.created_on,'yyyy-mm-dd') >= to_char(lbc.updated_at ,'yyyy-mm-dd')
      order by cp1.created_on
      limit 1
    )cp on true*/
    where a.user_id =lbc.user_id 
      and lbc.is_profiled =1
      and lbc.lender_name ~* 'o2o'
      /*and cp.profiled_date>= '{{ START_DATE }}'
      and cp.profiled_date < '{{ END_DATE }}'*/
    )dsa on true
    left join user_details ud
      on a.user_id=ud.user_id
LEFT JOIN LATERAL(
    SELECT  
    cp1.user_id,
    --cp1.customer_type,
    case when p_customer_type =1 then 'Green'
    when p_customer_type =2 then 'Red'
    when p_customer_type =3 then 'Amber'
    else 'NH' end as customer_type,
    cp1.customer_profile_id,
    cp1.created_on as profiled_date,
    usl.*
    FROM 
    cm_cp_processed cp1
    LEFT JOIN LATERAL(
        SELECT
            utm_source_text as profiled_utm_source,
            created_at
        FROM 
            utm_source_log usl
        WHERE
            usl.user_id = cp1.user_id
            AND usl.created_at < cp1.created_on
        ORDER BY usl.created_at DESC
        LIMIT 1
    ) usl ON TRUE
    WHERE
    a.user_id=cp1.user_id
    AND cp1.customer_profile_id = (SELECT customer_profile_id FROM cm_cp_processed WHERE user_id = cp1.user_id ORDER BY created_on LIMIT 1)
    --AND profiled_utm_source ~* '(flexiloans|homecredit|adcanopus_IC7|adsplay_IC6|affle_IC2|intellectads_IC5|intellectads|mediazotic_IC3|svgmedia_IC4|ventesavenues|ventesavenues_IC1)'
    ) profiled_utm on true
left join lateral(
    select
    cp.p_credit_score as credit_score
    from cm_cp_processed cp
    where 
    cp.user_id=a.user_id
    AND cp.customer_profile_id = (SELECT customer_profile_id FROM cm_cp_processed WHERE user_id = cp.user_id ORDER BY created_on desc LIMIT 1)
    )cp_latest on true
left join lateral(
  select 
  case when lai.regeneration_consent='1'
  then 'YES'
  when lai.regeneration_consent='0'
  then 'NO'
  else 'No regen consent record present'
  end as regeneration_consent 
  from lead_additional_info lai 
  where
  lai.lead_id =a.lead_id
  )consent on true
left join lateral(
select
count(cl.id) as regen_lead_count,
string_agg(cl.lender, ' , ') as regen_lenders,
string_agg(cl.sku_name , ' , ') as regen_sku,
string_agg(cl.id::varchar , ' , ') as regen_leads
from consolidated_leads cl 
where 
cl.oic~*'lead'
--and cl.id =b.lead_id
and cl.user_id =a.user_id
)regen_count on true
left join lateral(
select 
string_agg(lenders,'|') as lenders_presented 
from(
select
distinct btrim(unnest(string_to_array(fs2.lenders_presented::varchar , ',')),'{}') as lenders
from lenderoffers.form_submits fs2 
where
fs2.lead_id = a.lead_id
)le
)lenders_present on true
WHERE
    date(a.applied_date) between '{{START_DATE}}' and '{{END_DATE}}'