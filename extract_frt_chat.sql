select 
		ticket_id::varchar,
		to_char(local_date_created::date,'mm/dd/yyyy') as local_date_created,
		channel,
		response_time_first,
		lower(email_address) as email_address,
		agent_name,
		supervisor,
		daily,
		to_char(date(date_trunc('week', local_date_created)),'mm/dd/yyyy') as week_date,
		round(avg(response_time_first) over (partition by date(date_trunc('week', local_date_created)), agent_name)) as weekly,
		to_char(date(date_trunc('month', local_date_created)),'mm/dd/yyyy') as month_date,
		round(avg(response_time_first) over (partition by date(date_trunc('month', local_date_created)), agent_name)) as monthly
	from
	(select 
		distinct
		ticket_id,
		local_date_created,
		local_date_created - job_effectivity_date as recent,
		min(local_date_created - job_effectivity_date) over (partition by local_date_created, agent_name) as recent_,	
		channel,
		assignee_id,
		email_address,
		agent_name,
		supervisor,
		frt as daily,
		response_time_first
	from (
	select 
		ticket_id,
		assignee_id,
		agent_name,
		agent_email as email_address,
		channel,
		local_date as local_date_created,
		response_time_first,
		round(avg(response_time_first) over (partition by local_date, agent_name)) as frt
	from (
	select  
		distinct
		zendesk_chat_insights.ticket_id,
		a.assignee_id::varchar,
		agent_name,
		agent_email,
		type as channel,
		local_date,
		response_time_first
	--	,avg(response_time_first) over (partition by local_date, a.assignee_id::varchar) as frt
	from 
		zendesk_chat_insights
	left join 
	(select 
		ticket_id,
		assignee_id 
	from 
		zendesk_chat_tickets
	where 
		client_account = 'urbanstems') a 
		on a.ticket_id = zendesk_chat_insights.ticket_id
	left join 
	(
	select 
		assignee_id,
		agent_name,
		agent_email
	from 
		sd_agent_roster
	where 
		client_account = 'urbanstems'
	) b 
	on b.assignee_id::varchar = a.assignee_id::varchar
	where 
		client_account = 'urbanstems'
		and type = 'chat'
	) a ) b
	left join 
		(
		select 
			distinct 
			concat(first_name,' ',last_name) as full_name,
			job_supervisor as supervisor,
			job_effectivity_date
		from 
			sd_hris_team_roster 
		where 
			job_division = 'UrbanStems'
		) a
		on (full_name = agent_name and local_date_created >= job_effectivity_date)
	) a 
	where 
		recent = recent_
order by local_date_created::date desc, agent_name 
limit 10000
