select 
	ticket_id,
	channel,
	to_char(local_date_created::date,'mm/dd/yyyy') as local_date_created,
	local_time_created,
	satisfaction_rating_score,
	satisfaction_rating_comment,
	tags,
	type,
	trunc(uncontrollable_dsat::numeric)::varchar,
	zendesk_assignee 
from 
		(select 
			*,
			row_number() over (partition by ticket_id, local_date_created,zendesk_assignee) as r1
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
				agent_.agent_name as zendesk_assignee
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
								type
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
																	type
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
																	type
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
																	type
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
																	type
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
							assignee_id,
							agent_name 
						from 
							sd_agent_roster sar 
						where 
							client_account = 'urbanstems_seasonal'
							or client_account = 'urbanstems'
							or client_account = 'urbanstems_flex'
					) as agent_ on assignee_id::varchar = id_::varchar
			left join 
				t_dsat_urbanstems tdu on trunc(ext_interaction_id::numeric)::varchar = csat_.ticket_id::varchar 
				where 
					agent_.agent_name is not null and local_date_created >= '2022-01-01'
				order by local_date_created
			) as final_csat	) as duplicates 
		where 
			r1 = 1 and uncontrollable_dsat is null 
order by local_date_created, local_time_created, ticket_id 
