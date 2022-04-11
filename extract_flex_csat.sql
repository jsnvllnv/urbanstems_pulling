select 
	ticket_id,
	channel,
	to_char(local_date_created::date,'mm/dd/yyyy') as local_date_created  ,
	assignee_id,
	name_,
	email_,
	csat 
		from
			(select
				distinct 
				ticket_id,
				channel,
				local_date_created,
				assignee_id,
				satisfaction_rating_score as csat
					from 
						(select 
							distinct
							ticket_id,
							channel,
							local_date_created,
							assignee_id,
							satisfaction_rating_score 
						from 
							zendesk_email_tickets zet 
						where 
							client_account = 'urbanstems'
							and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
						union all
						select 
							distinct
							ticket_id,
							channel,
							local_date_created,
							assignee_id,
							satisfaction_rating_score 
						from 
							zendesk_chat_tickets zct 
						where 
							client_account = 'urbanstems'
							and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
						union all
						select 
							distinct
							ticket_id,
							channel,
							local_date_created,
							assignee_id,
							satisfaction_rating_score 
						from 
							zendesk_helpdesk_tickets zht 
						where 
							client_account = 'urbanstems'
							and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
						union all
						select 
							distinct
							ticket_id,
							channel,
							local_date_created,
							assignee_id,
							satisfaction_rating_score 
						from 
							d_zendesk_kpi_summary dzks 
						where 
							client_account = 'urbanstems'
							and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')
						union all	
						select 
							distinct
							ticket_id,
							channel,
							local_date_created,
							assignee_id,
							satisfaction_rating_score
						from 
							zendesk_call_tickets zct 
						where 
							client_account = 'urbanstems'
							and (satisfaction_rating_score = 'good' or satisfaction_rating_score = 'bad')) as all_csat
			) as distinct_id
		left join 
		(
			select 
				assignee_id as assignee_,
				agent_name as name_,
				agent_email as email_
			from 
				sd_agent_roster sar 
			where 
				client_account = 'urbanstems_flex'
				and assignee_id is not null
		) as core_agents on assignee_::varchar = assignee_id::varchar
left join 
	t_dsat_urbanstems tdu on ticket_id::varchar = trunc(ext_interaction_id::numeric)::varchar
where email_ is not null and local_date_created > '2022-01-31' and ext_interaction_id is null 
order by local_date_created desc, name_
