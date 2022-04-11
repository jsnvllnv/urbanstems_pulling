select 
	ticket_id,
	to_char(local_date_created::date,'mm/dd/yyyy') as local_date_created,
	assignee_id,
	agent_name,
	agent_email,
	response_time_first,
	round(avg(response_time_first) over (partition by local_date_created,agent_name)) as daily,
	round(avg(response_time_first) over (partition by local_date_created)) as team_daily,
	to_char(date(date_trunc('week',local_date_created))::date,'mm/dd/yyyy') as week_date,
	round(avg(response_time_first) over (partition by date(date_trunc('week',local_date_created)),agent_name)) as weekly,
	round(avg(response_time_first) over (partition by date(date_trunc('week',local_date_created)))) as team_weekly,
	to_char(date(date_trunc('month',local_date_created))::date,'mm/dd/yyyy') as month_date,
	round(avg(response_time_first) over (partition by date(date_trunc('month',local_date_created)),agent_name)) as monthly,
	round(avg(response_time_first) over (partition by date(date_trunc('month',local_date_created)))) as team_monthly
		from 
			(select
				distinct
				max_date.ticket_id,
				local_date_created,
				max_date.assignee_id,
				agent_name,
				agent_email,
				case when response_time_first='NaN' then null else response_time_first end as response_time_first,
				round(avg(response_time_first) over (partition by local_date_created,agent_name)) as daily,
				round(avg(response_time_first) over (partition by local_date_created)) as team_daily,
				date(date_trunc('week',local_date_created)) as week_date,
				round(avg(response_time_first) over (partition by date(date_trunc('week',local_date_created)),agent_name)) as weekly,
				round(avg(response_time_first) over (partition by date(date_trunc('week',local_date_created)))) as team_weekly,
				date(date_trunc('month',local_date_created)) as week_date,
				round(avg(response_time_first) over (partition by date(date_trunc('month',local_date_created)),agent_name)) as monthly,
				round(avg(response_time_first) over (partition by date(date_trunc('month',local_date_created)))) as team_monthly
					from
						(select 
							distinct
							ticket_id,
							local_date_created,
							date_updated,
							assignee_id,
							max(date_updated) over (partition by ticket_id) as max_date
						from 
							zendesk_chat_tickets
						where 
							client_account = 'urbanstems') as max_date 
				join (select * from sd_agent_roster where client_account = 'urbanstems_flex') sar
					on max_date.assignee_id::varchar = sar.assignee_id 
				join zendesk_chat_insights zci on zci.ticket_id = max_date.ticket_id 
				where max_date = date_updated) as distinct_
	order by local_date_created::date desc
