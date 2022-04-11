select 
	to_char(local_date_created::date, 'mm/dd/yyyy') as local_date_created,
	to_char(date(date_trunc('week',local_date_created))::date, 'mm/dd/yyyy') as local_date_created,
	ticket_id,
	agent_name,
	dur,
	round(avg(dur) over (partition by local_date_created,agent_name)) as daily_wait,
	round(avg(dur) over (partition by date(date_trunc('week',local_date_created)),agent_name)) as weekly_wait,
	round(avg(dur) over (partition by date(date_trunc('month',local_date_created)),agent_name)) as monthly_wait,
	assignee_id,
	subteam,
	round(avg(dur) over (partition by local_date_created, subteam)) as daily_wait_sub,
	round(avg(dur) over (partition by date(date_trunc('week',local_date_created)),subteam)) as weekly_wait_sub,
	round(avg(dur) over (partition by date(date_trunc('month',local_date_created)),subteam)) as monthly_wait_sub,
	round(avg(dur) over (partition by local_date_created)) as daily_wait_team,
	round(avg(dur) over (partition by date(date_trunc('week',local_date_created)))) as weekly_wait_team,
	round(avg(dur) over (partition by date(date_trunc('month',local_date_created)))) as monthly_wait_team
		from
			(select 
				distinct
				zct.ticket_id,
				dur,
				channel,
				local_date_created,
				assignee_id,
				via_source_rel,
				zct.status
			from 
				zendesk_call_tickets zct 
			left join 
				(select 
					distinct 
					ticket_id,
					status,
					dur
						from
							(select 
								distinct
								ticket_id,
								forwarded_to,
								status,
								extract(minutes from to_timestamp(wait_time, 'mi:ss')::time)*60 + extract(seconds from to_timestamp(wait_time, 'mi:ss')::time) as dur,
								wait_time,
								local_date,
								to_timestamp(wait_time, 'mi:ss')::time as wait_time2
									from 
										(select 
											*,
											max(wait_time) over (partition by ticket_id) as max_wait
										from 
											zendesk_call_history zch 
										where 
											client_account = 'urbanstems'
											) mx
								where max_wait = wait_time 
								) dur ) dur1 on dur1.ticket_id = zct.ticket_id 
			where 
				client_account = 'urbanstems'
				and via_source_rel <> 'outbound'
				and (zct.status = 'solved' or zct.status = 'closed')
				) zd
join 
	(
		select 
			zendesk_id,
			agent_name,
			start_date,
			end_date,
			subteam,
			is_core_team
		from 
			sd_subteam_urbanstems 
		where 
			subteam = 'urbanstems'
			or subteam = 'urbanstems_flex'	
	) ssu on (assignee_id::varchar = zendesk_id::varchar and local_date_created between start_date and end_date) 
where 
	is_core_team = true
order by local_date_created::date desc 
limit 10000
