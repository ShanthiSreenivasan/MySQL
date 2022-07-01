SELECT
    lc.user_id,
    lc.lead_id,
    lc.lead_type,
    lc.log_date,
    lc.crm_status_text,
    lc.dispo,
    lc.product,
    lc.stage,
    lc.appops_status_code,
    lc.oic,
    case when  ll.followup_date::date < '{{END_DATE}}' and crm_status_text ~* 'chr ptp' then 1 else 0 end as missed_ptp_chr,
    case when  ll.followup_date::date < '{{END_DATE}}' and crm_status_text ~* 'bys ptp' then 1 else 0 end as missed_ptp_bys
FROM
    lenderoffers.lo_calls lc left join leads ll on lc.lead_id=ll.id

WHERE
    log_date >= '{{START_DATE}}'
    and log_date < '{{END_DATE}}'