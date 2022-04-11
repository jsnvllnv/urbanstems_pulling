select 
	*,
	case
		when (team_hr_au is null and team_hr is not null) 
			then team_tics/team_hr
		when (team_hr_au is not null and team_hr is not null) 
			then team_tics/team_hr_au 
		when (team_hr_au is not null and team_hr is null) 
			then team_tics/team_hr_au 
		else team_tics
	end as team_prod
		from 
			(select 
				to_char(local_date_created::date,'mm/dd/yyyy') as local_date_created,
				agent_name,
				prod_tics,
				sum(prod_tics) over (partition by local_date_created) as team_tics,
				active_hr,
				sum(active_hr) over (partition by local_date_created) as team_hr_au,
				dur_hr,
				sum(dur_hr) over (partition by local_date_created) as team_hr,
				prod
					from
						(select 
							distinct
							local_date_created,
							agent_name,
						--	channel,
						--	tickets,
							prod_tics,
							active_hr,
							dur_hr,
							case
								when (active_hr is null and dur_hr is not null) 
									then prod_tics/dur_hr
								when (active_hr is not null and dur_hr is not null) 
									then prod_tics/active_hr 
								when (active_hr is not null and dur_hr is null) 
									then prod_tics/active_hr 
								else prod_tics
							end as prod
								from
									(select 
										local_date_created,
										agent_name,
										channel,
										tickets,
										sum(tickets) over (partition by local_date_created, agent_name) as prod_tics
											from
												(select 
													distinct 
													local_date_created,
												--	'Team Total' as agent_name,
													agent_name,
													channel,
													tickets,
													team_tickets 
														from
															(select 
																distinct 
																local_date_created,
																agent_name,
																'voice' as channel,
																tickets,
																team_tickets 
																	from
																		(select 
																			zch.ticket_id, 
																			date(date_trunc('week',local_date)) as local_date_created ,
																			zct.assignee_id, 
																			agent_name,
																			count(zch.ticket_id) over (partition by date(date_trunc('week',local_date)), agent_name) as tickets,
																			count(zch.ticket_id) over (partition by date(date_trunc('week',local_date))) as team_tickets
																		from zendesk_call_history zch
																		join (select * from (select distinct ticket_id, local_date_created, assignee_id from zendesk_call_tickets) a ) zct on zct.ticket_id = zch.ticket_id 
																		join (select assignee_id,agent_name from sd_agent_roster where client_account = 'urbanstems') sar on sar.assignee_id::varchar = zct.assignee_id::varchar 
																			where zch.client_account = 'urbanstems'
																			and zch.ticket_id is not null
																			and forwarded_to <> 'Voicemail') as voice_  
															union all
															select 
																distinct
																date(date_trunc('week',local_date_created)) as local_date_created,
																agent_name,
																'chat' as channel,
																count(ticket_id) over (partition by date(date_trunc('week',local_date_created)),agent_name) as tickets,
																count(ticket_id) over (partition by date(date_trunc('week',local_date_created))) as team_tickets
																	from
																		(select 
																			distinct
																			local_date_created,
																			ticket_id,
																			agent_name
																				from
																					(select 
																						*,
																						max(date_updated) over (partition by zct.ticket_id, local_date_created) as max_date
																					from 
																						zendesk_chat_tickets zct 
																					join (select assignee_id,agent_name from sd_agent_roster where client_account = 'urbanstems') sar on sar.assignee_id::varchar = zct.assignee_id::varchar 
																					join (select ticket_id as ticket_id_ , abandon_time from zendesk_chat_insights where client_account = 'urbanstems') zci on zct.ticket_id = zci.ticket_id_ 
																					where 
																						client_account = 'urbanstems'
																						and abandon_time is null ) as chat_
																			where max_date = date_updated) as unique_chat
															union all
															select 
																distinct
																date(date_trunc('week',local_date_created)) as local_date_created,
																agent_name,
																'email' as channel,
																count(ticket_id) over (partition by date(date_trunc('week',local_date_created)),agent_name) as tickets,
																count(ticket_id) over (partition by date(date_trunc('week',local_date_created))) as team_tickets
																	from
																		(select 
																			distinct
																			local_date_created,
																			ticket_id,
																			agent_name
																				from
																					(select 
																						*,
																						max(date_updated) over (partition by ticket_id, local_date_created) as max_date
																					from 
																						zendesk_email_tickets zet 
																					left join (select assignee_id,agent_name from sd_agent_roster where client_account = 'urbanstems') sar on sar.assignee_id::varchar = zet.assignee_id::varchar 
																					where 
																						client_account = 'urbanstems'
																						and channel = 'email' ) as email_
																			where max_date = date_updated and agent_name is not null) as unique_email
															union all
															select 
																*,
																sum(tickets) over (partition by local_date_created) as team_tickets
																	from 
																		(select 
																			distinct
																			date(date_trunc('week',local_date_created)) as local_date_created,
																			agent_name,
																			'web' as channel,
																			count(ticket_id) over (partition by date(date_trunc('week',local_date_created)),agent_name) as tickets
																				from
																					(select 
																						distinct
																						local_date_created,
																						ticket_id,
																						agent_name
																							from
																								(select 
																									*,
																									max(date_updated) over (partition by ticket_id, local_date_created) as max_date
																								from 
																									zendesk_helpdesk_tickets zht 
																								left join (select assignee_id,agent_name from sd_agent_roster where client_account = 'urbanstems') sar on sar.assignee_id::varchar = zht.assignee_id::varchar 
																								where 
																									client_account = 'urbanstems' ) as web_
																						where max_date = date_updated) as unique_web
																			where agent_name is not null ) a
															union all
															select 
																distinct
																date(date_trunc('week',local_date_created)) as local_date_created,
																agent_name,
																'api' as channel,
																count(ticket_id) over (partition by date(date_trunc('week',local_date_created)),agent_name) as tickets,
																count(ticket_id) over (partition by date(date_trunc('week',local_date_created))) as team_tickets
																	from
																		(select 
																			distinct
																			local_date_created,
																			ticket_id,
																			agent_name
																				from
																					(select 
																						*,
																						max(date_updated) over (partition by ticket_id, local_date_created) as max_date
																					from 
																						zendesk_email_tickets zet 
																					join (select assignee_id,agent_name from sd_agent_roster where client_account = 'urbanstems') sar on sar.assignee_id::varchar = zet.assignee_id::varchar 
																					where 
																						client_account = 'urbanstems'
																						and channel = 'api') as api_
																			where max_date = date_updated) as unique_api) as all_ticket
													order by local_date_created, agent_name, channel) as raw_prod ) as prod_tics
							left join 
							(select 
								distinct
								date(date_trunc('week',start_date)) as start_date,
								su.agent_name as agent_,
								su.agent_email,
								(sum(duration_seconds) over (partition by date(date_trunc('week',start_date)), su.agent_name))/3600 as active_hr
							from 
								sd_utilization su 
							join (select * from sd_agent_roster where client_account = 'urbanstems') sar on su.agent_email = sar.agent_email 
							where 
								su.client_account like 'Urban Stems'
								and function = 'Service Delivery') hrs on (date(date_trunc('week',prod_tics.local_date_created)) = date(date_trunc('week',hrs.start_date)) and agent_ = agent_name) 
							left join 
							(select 
								distinct
								date(date_trunc('week',date)) as local_date,
								agent_name as name_,
								(sum(duration_minutes) over (partition by date(date_trunc('week',date)), agent_name))/60 as dur_hr
							from 
								t_activity_urbanstems) manual_hrs on (date(date_trunc('week',prod_tics.local_date_created)) = date(date_trunc('week',local_date)) and name_ = agent_name)
						) as weekly 
		where 
			local_date_created >= '2021-01-19'				
		) as weekly_team
order by local_date_created::date desc
