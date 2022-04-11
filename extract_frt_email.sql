select 
	ticket_id,
	to_char(local_date_created::date,'mm/dd/yyyy') as local_date_created ,
	channel,
	assignee_id,
	email_address,
	agent_name,
	frt_calendar::varchar,
	round(avg(frt_calendar) over (partition by agent_name, local_date_created))::varchar as daily,
	round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
	round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly	
from (
	select 
		distinct 
		ticket_id::varchar,
		local_date_created,
		channel,
		assignee_id,
		email_address,
		agent_name,
		frt_calendar	
	from (
	select 
		distinct 
		ticket_id,
		local_date_created,
		date_updated,
		date_updated - local_date_created as recent_,
		max(date_updated - local_date_created) over (partition by ticket_id) as max_recent, 
		_id as assignee_id,
		agent_email as email_address,
		agent_name,
		frt_calendar,
		zen_tickets.channel
	from  
		zendesk_email_tickets
	left join 
		(select assignee_id as _id, agent_name, agent_email from sd_agent_roster where client_account = 'urbanstems') a on assignee_id::varchar = _id::varchar
	left join 
		(select frt_calendar, channel,  ticket_id as _id_, agent_name as name_ from d_zendesk_kpi_summary) zen_tickets on _id_::varchar = ticket_id::varchar and name_ = zen_tickets.name_
	where 
		client_account = 'urbanstems'
		and (status = 'closed' or status = 'solved')
		and zendesk_email_tickets.channel = 'email'
	order by date_updated 
	) b
	where 
		recent_ = max_recent
		and agent_name is not null 
		and local_date_created >= '2022-01-01') raw
order by local_date_created::date desc, agent_name 
limit 10000
