select 
	subteam as client_account,
	to_char(local_date_created::date,'mm/dd/yyyy') as local_date_created,
	hour_,
--	channel,
	ticket_id::varchar as ticket_id,
	assignee_id::varchar as assignee_id,
	email_address,
	agent_name,
	supervisor,
	dur,
	round(avg(dur) over (partition by agent_name, local_date_created, hour_))::varchar as hourly,
	round(avg(dur) over (partition by local_date_created, hour_, subteam))::varchar as t_hourly,
	round(avg(dur) over (partition by supervisor, subteam, local_date_created, hour_))::varchar as v_hourly,
	round(avg(dur) over (partition by agent_name, local_date_created))::varchar as daily,
	round(avg(dur) over (partition by local_date_created,subteam))::varchar as t_daily,
	round(avg(dur) over (partition by supervisor, local_date_created, subteam))::varchar as v_daily,
	round(avg(dur) over (partition by agent_name, date(date_trunc('week',local_date_created))))::varchar as weekly,
	round(avg(dur) over (partition by subteam, date(date_trunc('week',local_date_created))))::varchar as t_weekly,
	round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('week',local_date_created))))::varchar as v_weekly,
	round(avg(dur) over (partition by agent_name, date(date_trunc('month',local_date_created))))::varchar as monthly,
	round(avg(dur) over (partition by subteam, date(date_trunc('month',local_date_created))))::varchar as t_monthly,
	round(avg(dur) over (partition by supervisor, subteam, date(date_trunc('month',local_date_created))))::varchar as v_monthly
		from
			(select 
				distinct 
				subteam,
				ticket_id,
				local_date_created,
				hour_,
				channel,
				assignee_id,
				agent_email as email_address,
				agent_name,
				case when supervisor is null then 'Urbanstems Flex' else supervisor end as supervisor,
				dur
					from 
						(select 
							distinct 
							subteam,
							local_date_created,
							hour_,
							channel,
							ticket_id,
							assignee_id::varchar as assignee_id,
							agent_email,
							agent_name,
							case when supervisor is null then job_supervisor else supervisor end as supervisor,
							dur
								from
									(select 
										distinct
										zct.ticket_id,
										dur,
										channel,
										local_date_created,
										extract(hour from local_time_created::time) as hour_,
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
								where is_core_team = true
								) a on (assignee_id::varchar = _id::varchar and local_date_created between start_date and end_date)
							left join 
								(select 
									distinct
									start_date as date_,
									supervisor,
									agent_name as agent_ 
								from 
									sd_utilization 
								where
									division_name = 'UrbanStems') su on (local_date_created = date_ and agent_ = agent_name)
							left join 
								(select 
									full_name,
									job_supervisor
										from
											(select 
												concat(first_name,' ',last_name) as full_name,
												email_address,
												job_department,
												job_division,
												profile_status,
												job_supervisor,
												status_effectivity_date,
												job_effectivity_date,
												created_on,
												max(status_effectivity_date) over (partition by concat(first_name,' ',last_name)) as max_status,
												max(job_effectivity_date) over (partition by concat(first_name,' ',last_name)) as job_status,
												max(created_on) over (partition by concat(first_name,' ',last_name)) as create_status
											from 
												sd_hris_team_roster shtr 
											where job_division = 'UrbanStems') a
									where 
										max_status = status_effectivity_date 
										and job_status = job_effectivity_date 
										and created_on = create_status
										and profile_status = 'Active'
								) bamboo_visor on (agent_name = full_name)
						where 
							local_date_created >= '2022-04-04') raw_ ) final_
order by subteam,local_date_created desc, hour_ desc, agent_name
limit 10000
