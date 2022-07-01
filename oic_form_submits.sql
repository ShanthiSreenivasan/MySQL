select  distinct on (lead_id)

      submitted_by as oic,
      submitted_at,
      lead_id,product
      
from lenderoffers.form_submits 

where 
 submitted_at >= '{{START_DATE}}'
 and submitted_at < '{{END_DATE}}'
 and submitted_by !~* '(andriodApp|System|PORTFOLIO)'
order by lead_id,submitted_at

