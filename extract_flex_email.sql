select 
	ticket_id,
	to_char(local_date_created::date,'mm/dd/yyyy') as local_date_created ,
	channel2 as channel,
	assignee_id,
	email_address,
	agent_name,
	frt_calendar::varchar,
	round(avg(frt_calendar) over (partition by agent_name, local_date_created))::varchar as daily,
	round(avg(frt_calendar) over (partition by local_date_created))::varchar as team_daily,
	round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
	round(avg(frt_calendar) over (partition by date(date_trunc('week',local_date_created))))::varchar as team_weekly,
	round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
	round(avg(frt_calendar) over (partition by date(date_trunc('month',local_date_created))))::varchar as team_monthly
from (
	select 
		distinct 
		ticket_id::varchar,
		local_date_created,
		channel2,
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
		zendesk_email_tickets.channel as channel2
	from  
		zendesk_email_tickets
	left join 
		(select assignee_id as _id, agent_name, agent_email from sd_agent_roster where client_account = 'urbanstems_flex') a on assignee_id::varchar = _id::varchar
	left join 
		(select distinct ticket_id as _id_, reply_time_calendar_minutes as frt_calendar from zendesk_email_metrics) zen_tickets on _id_::varchar = ticket_id::varchar 
	where 
		client_account = 'urbanstems'
		and (status = 'closed' or status = 'solved')
		and zendesk_email_tickets.channel = 'email'
	order by date_updated 
	) b
	where 
		recent_ = max_recent
		and agent_name is not null) raw
order by 
	local_date_created::date desc, agent_name 
limit 10000
