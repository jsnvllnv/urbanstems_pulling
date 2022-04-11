select 
	to_char(local_date_created::date,'mm/dd/yyyy') as local_date_created,
	agent_name,
	channel,
	tickets,
	prod_tics,
	team_prod,
	prod_tics::float/40 as flex_prod_w,
	team_prod::float/40 as team_prod_w,
	prod_tics_m,
	team_prod_m,
	to_char(month_date::date,'mm/dd/yyyy') as month_date,
	prod_tics_m::float/160 as flex_prod_m,
	team_prod_m::float/160 as team_prod_m
		from
			(select 
				local_date_created,
				agent_name,
				channel,
				tickets,
				sum(tickets) over (partition by local_date_created, agent_name) as prod_tics,
				sum(tickets) over (partition by local_date_created) as team_prod,
				date(date_trunc('month',local_date_created)) as month_date,
				sum(tickets) over (partition by date(date_trunc('month',local_date_created)), agent_name) as prod_tics_m,
				sum(tickets) over (partition by date(date_trunc('month',local_date_created))) as team_prod_m
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
												join (select assignee_id,agent_name from sd_agent_roster where client_account = 'urbanstems_flex') sar on sar.assignee_id::varchar = zct.assignee_id::varchar 
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
															join (select assignee_id,agent_name from sd_agent_roster where client_account = 'urbanstems_flex') sar on sar.assignee_id::varchar = zct.assignee_id::varchar 
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
															left join (select assignee_id,agent_name from sd_agent_roster where client_account = 'urbanstems_flex') sar on sar.assignee_id::varchar = zet.assignee_id::varchar 
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
																		left join (select assignee_id,agent_name from sd_agent_roster where client_account = 'urbanstems_flex') sar on sar.assignee_id::varchar = zht.assignee_id::varchar 
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
															join (select assignee_id,agent_name from sd_agent_roster where client_account = 'urbanstems_flex') sar on sar.assignee_id::varchar = zet.assignee_id::varchar 
															where 
																client_account = 'urbanstems'
																and channel = 'api') as api_
													where max_date = date_updated) as unique_api) as all_ticket
							order by local_date_created, agent_name, channel) as raw_prod ) as all_
	order by local_date_created::date desc
