select 
	ticket_id,
	channel,
	to_char(local_date_created::date,'mm/dd/yyyy') as local_date_created,
	extract(hour from local_time_created::time) as hour_,
	satisfaction_rating_score,
	satisfaction_rating_comment,
	tags,
	type,
	trunc(uncontrollable_dsat::numeric)::varchar as uncontrollable_dsat,
	zendesk_assignee,
	subteam
from 
		(select 
			*
		from 
			(select 
				distinct 
				csat_.ticket_id::varchar,
				csat_.channel,
				local_date_created,
				local_time_created,
				satisfaction_rating_score,
				satisfaction_rating_comment,
				tags,
				type,
			--	id_,
				ext_interaction_id as uncontrollable_dsat,
				agent_.agent_name as zendesk_assignee,
				date_updated,
				min(date_updated) over (partition by ticket_id) as last_update,
				subteam
					from
						(
							select 
								ticket_id,
								channel,
								local_date_created,
								local_time_created,
								satisfaction_rating_score,
								satisfaction_rating_comment,
								id_,
								tags,
								type,
								date_updated
									from
											(
												select 
													*
														from
															(
																select
																	client_account,
																	ticket_id,
																	channel,
																	local_date_created,
																	local_time_created,
																	satisfaction_rating_score,
																	satisfaction_rating_comment,
																	assignee_id as id_,
																	tags,
																	type,
																	date_updated
																from 
																	zendesk_email_tickets zet
																union all 
																select
																	client_account,
																	ticket_id,
																	channel,
																	local_date_created,
																	local_time_created,
																	satisfaction_rating_score,
																	satisfaction_rating_comment,
																	assignee_id as id_,
																	tags,
																	type,
																	date_updated
																from 
																	zendesk_chat_tickets zct 
																union all 
																select
																	client_account,
																	ticket_id,
																	channel,
																	local_date_created,
																	local_time_created,
																	satisfaction_rating_score,
																	satisfaction_rating_comment,
																	assignee_id as id_,
																	tags,
																	type,
																	date_updated
																from 
																	zendesk_call_tickets zct2 
																union all 
																select
																	client_account,
																	ticket_id,
																	channel,
																	local_date_created,
																	local_time_created,
																	satisfaction_rating_score,
																	satisfaction_rating_comment,
																	assignee_id as id_,
																	tags,
																	type,
																	date_updated
																from 
																	zendesk_helpdesk_tickets zht 
													) as all_tickets
												where 
													satisfaction_rating_score = 'good'
													or satisfaction_rating_score = 'bad'
											) as good_bad
										where 
											client_account = 'urbanstems'
						) as csat_
				left join 
					(
						select 
							agent_name,
							subteam,
							agent_name as name_, 
							start_date,
							end_date,
							zendesk_id
						from 
							sd_subteam_urbanstems ssu 
						where is_core_team is true and subteam not like '%tc%' and subteam not like '%careops%'
					) as agent_ on (zendesk_id::varchar = id_::varchar and local_date_created between start_date and end_date)
			left join 
				t_dsat_urbanstems tdu on trunc(ext_interaction_id::numeric)::varchar = csat_.ticket_id::varchar 
				where 
					agent_.agent_name is not null and 
					local_date_created::date >= '2022-01-01'
				order by local_date_created
			) as final_csat 
		where date_updated = last_update	
		) as duplicates 
order by local_date_created, local_time_created, ticket_id 
