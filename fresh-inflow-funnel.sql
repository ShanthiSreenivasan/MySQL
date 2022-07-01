WITH profiles AS (
    SELECT
        case
            when grouping(customer_type) = 0 then
                customer_type
            else
                'Total'
        end as customer_type,
        count(distinct cp.user_id) as profiles
    FROM
        (select cp1.*,
        case when cp1.p_customer_type=1 then 'Green'
        when cp1.p_customer_type=2 then 'Red'
        when cp1.p_customer_type=3 then 'Amber'
        end as customer_type
        from cm_cp_processed cp1)cp
    WHERE
        date(cp.created_on) between '{{START_DATE}}' and '{{END_DATE}}'
        and cp.p_customer_type in ('1', '2', '3')
        and cp.customer_profile_id = (
            select min(customer_profile_id)
            from cm_cp_processed
            where user_id = cp.user_id
        )
    GROUP BY
        ROLLUP(cp.customer_type)
    ORDER BY 1
), funnel as (
    SELECT
        case
            when grouping(customer_type) = 0 then
                customer_type
            else
                'Total'
        end as customer_type,
        case
            when grouping(cll.prod) = 0 then
                cll.prod
            else
                'Total'
        end as product,
        count(distinct cp.user_id) filter (where cll.pass_021 = 1) as pass_021,
        count(distinct cp.user_id) filter (where cll.pass_022 = 1) as pass_022,
        count(distinct cp.user_id) filter (where cll.pass_023 = 1) as pass_023,
        count(distinct cp.user_id) filter (where cll.pass_024 = 1) as pass_024,
        count(distinct cp.user_id) filter (where cll.pass_110 = 1) as pass_110,
        count(distinct cp.user_id) filter (where cll.pass_03 = 1) as pass_03,
        count(distinct cp.user_id) filter (where cll.pass_03 = 1 and oic_at_03 = 'System') as pass_03_sys,
        count(distinct cp.user_id) filter (where cll.pass_88 = 1) as pass_88,
        count(distinct cp.user_id) filter (where cll.pass_05 = 1) as pass_05,
        count(distinct cp.user_id) filter (where cll.pass_07 = 1) as pass_07,
        count(distinct cp.user_id) filter (where cll.pass_07 = 1 and oic_at_07 = 'System') as pass_07_sys,
        count(distinct cp.user_id) filter (where cll.pass_270 = 1) as pass_270
    FROM
        (select cp1.*,
        case when cp1.p_customer_type=1 then 'Green'
        when cp1.p_customer_type=2 then 'Red'
        when cp1.p_customer_type=3 then 'Amber'
        end as customer_type
        from cm_cp_processed cp1)cp
        LEFT JOIN LATERAL (
            select
                split_part(product_status, '-', 1) prod,
                bool_or(product_status ~* '021$')::integer pass_021,
                bool_or(product_status ~* '022$')::integer pass_022,
                bool_or(product_status ~* '023$')::integer pass_023,
                bool_or(product_status ~* '024$')::integer pass_024,
                bool_or(product_status ~* '03$')::integer pass_03,
                (array_agg(updated_by_oic order by log_updated_at) filter (where product_status ~* '03$'))[1] as oic_at_03,
                (bool_or(product_status ~* '88$') and not bool_or(product_status ~* '05$'))::integer pass_88,
                bool_or(product_status ~* '05$')::integer pass_05,
                bool_or(product_status ~* '110$')::integer pass_110,
                bool_or(product_status ~* '07$')::integer pass_07,
                (array_agg(updated_by_oic order by log_updated_at) filter (where product_status ~* '07$'))[1] as oic_at_07,
                bool_or(product_status ~* '270$' or appops_status_code = '270')::integer pass_270
            from
                consolidated_lead_logs
            where
                user_id = cp.user_id
                and lead_type = 'LenderOffer'
            group by
                prod
        ) cll on true
    WHERE
        date(cp.created_on) between '{{START_DATE}}' and '{{END_DATE}}'
        and cp.p_customer_type in ('1', '2', '3')
        and cp.customer_profile_id = (
            select min(customer_profile_id)
            from cm_cp_processed
            where user_id = cp.user_id
        )
        and cll.prod is not null
    GROUP BY
        ROLLUP(cp.customer_type, cll.prod)
), final_funnel as (
    select
       -- p.p_customer_type as "Customer Type",
       /* case when p.p_customer_type =1 then 'Green'
    when p.p_customer_type =2 then 'Red'
    when p.p_customer_type =3 then 'Amber'
    else 'NH' end as "Customer Type",*/
        p.customer_type as "Customer Type",
        p.profiles as "Profiles",
        f.product as "Product",
        f.pass_021 as "Pass 021",
        round(f.pass_021 / nullif(p.profiles, 0) ::numeric * 100, 1) || ' %' as "Product Interest %",
        f.pass_022 as "Pass 022",
        f.pass_023 as "Pass 023",
        f.pass_024 as "Pass 024",

        f.pass_03 as "Pass 03",
        f.pass_03_sys as "Pass 03 (STP)",
        round(f.pass_03 / nullif(p.profiles, 0)::numeric * 100, 1) || ' %' as "Profiles to Form Submit %",
        f.pass_05 as "Pass 05",
        f.pass_07 as "Pass 07",
        f.pass_07_sys as "Pass 07 (STP)",
        f.pass_110 as "Pass 110",
        round(f.pass_07 / nullif(p.profiles, 0)::numeric * 100, 1) || ' %' as "Profiles to Applications %",
        f.pass_270 as "Pass 270"
    from
        profiles p
        inner join
            funnel f
            on f.customer_type = p.customer_type
)
SELECT
    *
FROM
    final_funnel