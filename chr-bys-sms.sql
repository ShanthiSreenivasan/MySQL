
with dat as (
	SELECT DISTINCT ON (user_id)
		lc.lead_id,
		l.user_id,
		ud.phone_home,
		ud.first_name,
		ud.last_name,
		lc.oic,
		case when cp.p_customer_type =1 then 'Green'
    when cp.p_customer_type =2 then 'Red'
    when cp.p_customer_type =3 then 'Amber'
    else 'NH' end as customer_type,
        CASE
            WHEN lc.description ~* 'bys' THEN 'BYS'
            WHEN lc.description ~* 'chr' THEN 'CHR'
        END AS service_type,
		lc.updated_at,
    coalesce(ui.city, u.city) as city,
	  coalesce(ui.residential_pincode, u.zip) as pincode
	from
		lead_contacts lc
		left join
			leads l
			on l.id = lc.lead_id
		left join
		  user_details ud
		  on ud.user_id = l.user_id
		LEFT JOIN 
        lenderoffers.user_inputs ui
        ON ui.user_id = ud.user_id
        left join 
		cm_cp_processed cp
		on cp.customer_profile_id = (SELECT MAX(customer_profile_id) FROM cm_cp_processed WHERE user_id = l.user_id) 
    LEFT JOIN 
        users u
        ON u.user_id = ud.user_id
	where
		lc.updated_at >= '{{ START_DATE }}'
		and lc.updated_at < '{{ END_DATE }}'
		and lc.description ~* 'promise to pay.*(bys|chr)'
		and lc.contact_type = 'Sms'
		and lc.oic in ({{OICS}})
		AND NOT EXISTS (SELECT 1 FROM product.vas_subscriptions WHERE user_id  = l.user_id)
	order by
		user_id, created_at
)
select
	*
from
	dat
