select 
	to_char(local_date_created::date, 'mm/dd/yyyy') as local_date_created,
	hour_,
	assignee_id,
	agent_name,
	supervisor,
	subteam,
	is_core_team,
	channel,
	h_tickets,
	h_dur,
	d_dur,
	w_dur,
	m_dur,
	d_dur_manual,
	w_dur_manual,
	m_dur_manual,
	d_tickets,
	w_tickets,
	m_tickets,
	round(h_prod::numeric,2) as h_prod,
	round(d_prod::numeric,2) as d_prod,
	round(w_prod::numeric,2) as w_prod,
	round(m_prod::numeric,2) as m_prod
		from 
			(select 
				*,
				case when h_dur is not null then h_tickets::float/(h_dur/3600) else h_tickets end as h_prod,
				case 
					when ((d_dur is null or d_dur = 0) and (d_dur_manual is null or d_dur_manual = 0)) then d_tickets
					when (d_dur is not null and (d_dur_manual is null or d_dur_manual = 0)) then d_tickets::float/(d_dur/3600)
					when ((d_dur is null or d_dur = 0) and d_dur_manual is not null) then d_tickets::float/(d_dur_manual/3600)
					else d_tickets::float/(d_dur/3600)
				end as d_prod,
				case 
					when ((w_dur is null or w_dur = 0) and (w_dur_manual is null or w_dur_manual = 0)) then w_tickets
					when (w_dur is not null and (w_dur_manual is null or w_dur_manual = 0)) then w_tickets::float/(w_dur/3600)
					when ((w_dur is null or w_dur = 0) and w_dur_manual is not null) then w_tickets::float/(w_dur_manual/3600)
					else w_tickets::float/(w_dur/3600)
				end as w_prod,
				case 
					when ((m_dur is null or m_dur = 0) and (m_dur_manual is null or m_dur_manual = 0)) then m_tickets
					when (m_dur is not null and (m_dur_manual is null or m_dur_manual = 0)) then m_tickets::float/(m_dur/3600)
					when ((m_dur is null or m_dur = 0) and m_dur_manual is not null) then m_tickets::float/(m_dur_manual/3600)
					else m_tickets::float/(m_dur/3600)
				end as m_prod
					from 
						(select 
							*,
							sum(h_tickets) over (partition by local_date_created, agent_name) as d_tickets,
							sum(h_tickets) over (partition by date(date_trunc('week',local_date_created)), agent_name) as w_tickets,
							sum(h_tickets) over (partition by date(date_trunc('month',local_date_created)), agent_name) as m_tickets
								from
									(select 
												distinct
												local_date_created,
												hour_,
												assignee_id,
												agent_name,
												supervisor,
												subteam,
												is_core_team,
												channel,
												h_tickets,
												h_dur,
												d_dur,
												w_dur,
												m_dur,
												d_dur_manual,
												w_dur_manual,
												m_dur_manual
													from
														(select 
															distinct 
															local_date_created,
															hour_,
															assignee_id,
															agent_name as agent_,
															channel,
															h_tickets,
															d_tickets,
															h_team_tickets,
															d_team_tickets  
																from
																	(
																	/*Number of Voice tickets*/
																	select 
																			distinct 
									--										ticket_id,
																			local_date_created,
																			hour_,
																			assignee_id,
																			agent_name,
																			'voice' as channel,
																			count(ticket_id) over (partition by local_date_created, hour_, agent_name) as h_tickets,
																			count(ticket_id) over (partition by local_date_created, agent_name) as d_tickets,
																			count(ticket_id) over (partition by local_date_created,hour_) as h_team_tickets,
																			count(ticket_id) over (partition by local_date_created) as d_team_tickets			
																				from
																					(select 
																						distinct
																						zch.ticket_id, 
																						local_date as local_date_created ,
																						local_time as local_time_created ,
																			            extract(hour from local_time::time) as hour_,
																						zct.assignee_id, 
																						agent_name
																					from zendesk_call_history zch
																					join (select * from (select distinct ticket_id, local_date_created, assignee_id from zendesk_call_tickets where channel = 'voice') a ) zct on zct.ticket_id = zch.ticket_id 
																					join (select assignee_id,agent_name from sd_agent_roster) sar on sar.assignee_id::varchar = zct.assignee_id::varchar 
																						where zch.client_account = 'urbanstems'
																						and zch.ticket_id is not null
																						and forwarded_to <> 'Voicemail') as voice_
																	union all
																	/*Number of Chat tickets*/
																	select 
																		distinct
																		local_date_created,
																		hour_,
																		assignee_id,
																		agent_name,
																		'chat' as channel,
																		count(ticket_id) over (partition by local_date_created, hour_, agent_name) as h_tickets,
																		count(ticket_id) over (partition by local_date_created,agent_name) as d_tickets,
																		count(ticket_id) over (partition by local_date_created, hour_) as h_team_tickets,
																		count(ticket_id) over (partition by local_date_created) as d_team_tickets
																			from
																				(select 
																					distinct
																					local_date_created,
																					local_time_created,
																					extract(hour from local_time_created::time) as hour_,
																					ticket_id,
																					assignee_id,
																					agent_name
																						from
																							(select 
																								*,
																								max(date_updated) over (partition by zct.ticket_id, local_date_created) as max_date
																							from 
																								zendesk_chat_tickets zct 
																							join (select assignee_id as assignee_id_,agent_name from sd_agent_roster) sar on sar.assignee_id_::varchar = zct.assignee_id::varchar 
																							join (select ticket_id as ticket_id_ , abandon_time from zendesk_chat_insights where client_account = 'urbanstems') zci on zct.ticket_id = zci.ticket_id_ 
																							where 
																								client_account = 'urbanstems'
																								and abandon_time is null ) as chat_
																					where max_date = date_updated) as unique_chat
																	union all
																	/*Number of Email tickets*/
																	select 
																		distinct
																		local_date_created,
																		hour_,
																		assignee_id,
																		agent_name,
																		'email' as channel,
																		count(ticket_id) over (partition by local_date_created,hour_,agent_name) as h_tickets,
																		count(ticket_id) over (partition by local_date_created,agent_name) as d_tickets,
																		count(ticket_id) over (partition by local_date_created,hour_) as h_team_tickets,
																		count(ticket_id) over (partition by local_date_created) as d_team_tickets
																			from
																				(select 
																					distinct
																					local_date_created,
																					local_time_created,
																					extract(hour from local_time_created::time) as hour_,
																					ticket_id,
																					assignee_id,
																					agent_name
																						from
																							(select 
																								*,
																								max(date_updated) over (partition by ticket_id, local_date_created) as max_date
																							from 
																								zendesk_email_tickets zet 
																							left join (select assignee_id as assignee_id_,agent_name from sd_agent_roster ) sar on sar.assignee_id_::varchar = zet.assignee_id::varchar 
																							where 
																								client_account = 'urbanstems'
																								and channel = 'email' ) as email_
																					where max_date = date_updated and agent_name is not null 
																					and (status = 'solved' or status = 'closed')
																					) as unique_email
																	union all
																	/*Number of Web tickets*/
																	select 
																		*
																			from 
																				(select 
																					distinct
																					local_date_created,
																					hour_,
																					assignee_id,
																					agent_name,
																					'web' as channel,
																					count(ticket_id) over (partition by local_date_created,hour_,agent_name) as h_tickets,
																					count(ticket_id) over (partition by local_date_created,agent_name) as d_tickets,
																					count(ticket_id) over (partition by local_date_created,hour_) as h_team_tickets,
																					count(ticket_id) over (partition by local_date_created) as d_team_tickets
																						from
																							(select 
																								distinct
																								local_date_created,
																								local_time_created,
																								extract(hour from local_time_created::time) as hour_,
																								ticket_id,
																								assignee_id,
																								agent_name
																									from
																										(select 
																											*,
																											max(date_updated) over (partition by ticket_id, local_date_created) as max_date
																										from 
																											zendesk_helpdesk_tickets zht 
																										left join (select assignee_id as assignee_id_,agent_name from sd_agent_roster) sar on sar.assignee_id_::varchar = zht.assignee_id::varchar 
																										where 
																											client_account = 'urbanstems' ) as web_
																								where max_date = date_updated and (status = 'solved' or status = 'closed')) as unique_web
																					where agent_name is not null ) a
																	union all
																	/*Number of API tickets*/
																	select 
																		distinct
																		local_date_created,
																		hour_,
																		assignee_id,
																		agent_name,
																		'api' as channel,
																		count(ticket_id) over (partition by local_date_created,hour_,agent_name) as h_tickets,
																		count(ticket_id) over (partition by local_date_created,agent_name) as d_tickets,
																		count(ticket_id) over (partition by local_date_created,hour_) as h_team_tickets,
																		count(ticket_id) over (partition by local_date_created) as d_team_tickets
																			from
																				(select 
																					distinct
																					local_date_created,
																					local_time_created,
																					extract(hour from local_time_created::time) as hour_,
																					ticket_id,
																					assignee_id,
																					agent_name
																						from
																							(select 
																								*,
																								max(date_updated) over (partition by ticket_id, local_date_created) as max_date
																							from 
																								zendesk_email_tickets zet 
																							join (select assignee_id as assignee_id_,agent_name from sd_agent_roster) sar on sar.assignee_id_::varchar = zet.assignee_id::varchar 
																							where 
																								client_account = 'urbanstems'
																								and channel = 'api') as api_
																					where max_date = date_updated) as unique_api) as all_ticket 
																					) as raw_prod 
												join sd_subteam_urbanstems ssu on (zendesk_id = assignee_id and local_date_created between start_date and end_date) 
												left join 
												(select 
													start_date as start_date_,
													extract(hour from start_time::time) as hour_au,
													agent_name as agent_au,
													agent_email,
													supervisor,
													duration_seconds,
													sum(duration_seconds) over (partition by start_date,extract(hour from start_time::time),agent_name) as h_dur,
													sum(duration_seconds) over (partition by start_date,agent_name) as d_dur,
													sum(duration_seconds) over (partition by date(date_trunc('week',start_date)),agent_name) as w_dur,
													sum(duration_seconds) over (partition by date(date_trunc('month',start_date)),agent_name) as m_dur
												from 
													sd_utilization su 
												where
													division_account like 'Urban Stems'
													and is_billable = true 
													and (activity like '%Ticket Active%' or activity like '%Tickets Active%' or activity like '%Zendesk%')) au 
												on (local_date_created = start_date_ and hour_ = hour_au and agent_name = agent_au)
												left join 
												(select 
													distinct 
													date,
													case when agent_name is null then agent_alias else agent_name end as agent_manual,
													d_dur_manual,
													w_dur_manual,
													m_dur_manual
														from 
															(select 
																date,
																agent_alias,
																agent_name,
																duration_seconds,
																sum(duration_seconds) over (partition by date, agent_alias) as d_dur_manual,
																sum(duration_seconds) over (partition by date(date_trunc('week',date)), agent_alias) as w_dur_manual,
																sum(duration_seconds) over (partition by date(date_trunc('month',date)), agent_alias) as m_dur_manual
															from 
																t_activity_urbanstems tau ) manual_hr ) final_manual 
													on (local_date_created = date and agent_name = agent_manual)) as extract_raw ) d_tics ) all_period
order by local_date_created::date desc, hour_ desc, agent_name
limit 20000
