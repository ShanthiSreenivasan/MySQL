select
	cl.product,
	--cp.customer_type,
	case when p_customer_type =1 then 'Green'
    when p_customer_type =2 then 'Red'
    when p_customer_type =3 then 'Amber'
    else 'NH' end as customer_type,

	case when app.updated_by_oic = 'System' and utm_source ~* '(email|sms)' then 'PA'
                        when app.updated_by_oic not in ('System', 'PORTFOLIO', 'androidApp', 'androidApp') then 'Assisted'
                        when get_latest_utm(app.user_id,app.log_updated_at) ~*  '(netcoreCC|netcorepl|intellectadscc|adcanopuscc|adcanopuspl|aqugenCC|FB_CC_LAL2_AM|FB_CC_Custom_Am|FB_CC_LAL1_AM|FB_PL1|FB_PL2|FB_CCPL_LAL1|FB_CCPL_LAL2|FB_GCC_L1|FB_GCC_L2|fb_pm|FB_CC_L1DEC|FB_CC_L2|FB_L1|FB_L1_DEC1|FB_L1_RED|FB_L2|FB_L2_DEC2|FB_L2_RED|GL)'
                        then 'Marketing'
                        else 'System' end "channel",

	case
		when month_diff(cp.created_on, cl.created_at) = 0 then 'M0'
		when month_diff(cp.created_on, cl.created_at) = 1 then 'M1'
		when month_diff(cp.created_on, cl.created_at) > 1 then 'M1+'
	end as month_orig,

	count(distinct cl.id) as total,
  count(distinct cl.id) filter (where cll.pass_021 = 1) as pass_021,
	count(distinct cl.id) filter (where cll.pass_022 = 1) as pass_022,
	count(distinct cl.id) filter (where cll.pass_023 = 1) as pass_023,
	count(distinct cl.id) filter (where cll.pass_024 = 1) as pass_024,
	count(distinct cl.id) filter (where cll.pass_03 = 1) as pass_03,
	count(distinct cl.id) filter (where cll.pass_03 = 1 and oic_at_03 = 'System') as pass_03_sys,
	count(distinct cl.id) filter (where cll.pass_05 = 1) as pass_05,
	count(distinct cl.id) filter (where cll.pass_88 = 1) as pass_88,
	count(distinct cl.id) filter (where cll.pass_110 = 1) as pass_110,
	count(distinct cl.id) filter (where cll.pass_07 = 1) as pass_07,
	count(distinct cl.id) filter (where cll.pass_07 = 1 and oic_at_07 = 'System') as pass_07_sys,
	count(distinct cl.id) filter (where cll.pass_270 = 1) as pass_270
FROM
    consolidated_leads cl
    LEFT JOIN
        cm_cp_processed cp
        ON cp.user_id = cl.user_id
        AND cp.customer_profile_id = (SELECT min(customer_profile_id) FROM cm_cp_processed WHERE user_id = cp.user_id)
    LEFT JOIN
        users u
        ON u.user_id = cp.user_id
    left join lateral (
        select
            bool_or(product_status ~* '021$')::integer pass_021,
            bool_or(product_status ~* '022$')::integer pass_022,
            bool_or(product_status ~* '023$')::integer pass_023,
            bool_or(product_status ~* '024$')::integer pass_024,
            bool_or(product_status ~* '03$')::integer pass_03,
            (array_agg(updated_by_oic order by log_updated_at) filter (where product_status ~* '03$'))[1] as oic_at_03,
            (bool_or(product_status ~* '88$') and not bool_or(product_status ~* '05$'))::integer pass_88,
            bool_or(product_status ~* '05$')::integer pass_05,
            bool_or(product_status ~* '07$')::integer pass_07,
            (array_agg(updated_by_oic order by log_updated_at) filter (where product_status ~* '07$'))[1] as oic_at_07,
            bool_or(product_status ~* '110$')::integer pass_110,
            bool_or(product_status ~* '07$' or appops_status_code = '270')::integer pass_270
        from
            consolidated_lead_logs
        where
            lead_id = cl.id
    ) cll on true

left join lateral(select
                          updated_by_oic,
                          user_id,
                          log_updated_at

from consolidated_lead_logs cll
  where cll.lead_id=cl.id
  and product_status ~* '021'
  order by log_updated_at desc limit 1
) app on true

where
	cl.created_at >= '{{START_DATE}}'
	and cl.created_at < '{{END_DATE}}'
	and cl.type = 'LenderOffer'
	and exists (
		select 1 from cm_cp_processed where user_id = cl.user_id
	)
group by
	rollup(1, 2, 3,4)
order by 1, 2, 3,4