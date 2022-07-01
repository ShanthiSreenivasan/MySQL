with data as (
select product as prod,
      	case when la.sku ~* 'shriram' and ffi.residential_pincode similar to  '(50|51|52|53|54|55)%' then la.sku || ' AP & TG'
         when la.sku ~* 'shriram' and ffi.residential_pincode not similar to  '(50|51|52|53|54|55)%' then la.sku || ' ROI'
	       WHEN la.sku_slug ~* 'IDFCPROGB' THEN 'IDFCPROGB ' || la.product
	     else la.sku end as sku,
      customer_type as ct,
      case when applied_oic !~* '(System|PORTFOLIO|andriodApp|referralCustomerEngagement)' then 'Assisted'
           when applied_oic ~* '(System)' and utm_source ~* '(email|sms)' then 'PA'
           when applied_oic ~* '(System)' and utm_content ~* '(widget|prelogin)' then 'Marketing'
           else 'System' end as applied_through,
      count(la.lead_id) as "MOVED_270"
      
from lenderoffers.applications la
left join 
  lenderoffers.form_field_inputs ffi
  ON ffi.lead_id = la.lead_id
where
    date_of_referral >= '{{START_DATE}}'
    {{ #OICS }}
    and applied_oic in ({{ OICS }})
    {{ /OICS }}
     group by
     rollup (1,2,3,4)
     order by 1,2,3,4)

select
               case when prod is null then 'Total' else prod end  as "Product",
               case when sku is null then 'Total' else sku end as "SKU",
               case when applied_through is null then 'Total' else applied_through  end as "OIC" ,
               case when ct is null then 'Total' else ct  end as "customer_type",
               "MOVED_270"
               from data


