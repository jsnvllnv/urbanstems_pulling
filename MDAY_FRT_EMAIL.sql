select 
	subteam,
	ticket_id::varchar as ticket_id,
	to_char(local_date_created::date,'mm/dd/yyyy') as local_date_created ,
	hour_,
	channel,
	assignee_id::varchar as assignee_id,
	email_address,
	agent_name,
	frt_calendar::varchar,
	round(avg(frt_calendar) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
	round(avg(frt_calendar) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
--	round(avg(frt_calendar) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
	round(avg(frt_calendar) over (partition by agent_name, local_date_created))::varchar as daily,
	round(avg(frt_calendar) over (partition by local_date_created,subteam))::varchar as t_daily,
--	round(avg(frt_calendar) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
	round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
	round(avg(frt_calendar) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
--	round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
	round(avg(frt_calendar) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
	round(avg(frt_calendar) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly
--	round(avg(frt_calendar) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
from (
	select 
		distinct
		subteam,
		ticket_id::varchar,
		local_date_created,
		hour_,
		channel,
		assignee_id,
		email_address,
		agent_name,
		frt_calendar	
	from (
	select 
		distinct 
		subteam,
		ticket_id,
		local_date_created,
		extract(hour from local_time_created::time) as hour_,
		date_updated,
		date_updated - local_date_created as recent_,
		min(date_updated - local_date_created) over (partition by ticket_id) as min_recent, 
		_id as assignee_id,
		agent_email as email_address,
		agent_name,
		frt_calendar,
		zen_tickets.channel
	from  
		zendesk_email_tickets
	left join 
		(select 
			subteam, 
			zendesk_id as _id, 
			agent_name, 
			agent_email, 
			start_date, 
			end_date, 
			is_core_team 
		from sd_subteam_urbanstems
		where is_core_team = true --and subteam = 'urbanstems'
		) a on (assignee_id::varchar = _id::varchar and local_date_created between start_date and end_date)
	left join 
		(select ticket_id as _id_, reply_time_calendar_minutes as frt_calendar, 'email' as channel from zendesk_email_metrics) zen_tickets on _id_::varchar = ticket_id::varchar
	where 
		client_account = 'urbanstems'
		and (status = 'closed' or status = 'solved')
		and zendesk_email_tickets.channel = 'email'
	order by date_updated 
	) b
	where 
		recent_ = min_recent
		and agent_name is not null 
		and local_date_created >= '2022-01-01') raw
order by local_date_created::date desc, hour_ desc, agent_name 
limit 30000
