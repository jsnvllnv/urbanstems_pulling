select 
	agent_email, 
	to_char(shift_start::date,'mm/dd/yyyy hh24:mi:ss') as shift_start, 
	to_char(shift_end::date,'mm/dd/yyyy hh24:mi:ss') as shift_end,
	to_char(clock_in::date,'mm/dd/yyyy hh24:mi:ss') as clock_in,
	to_char(clock_out::date,'mm/dd/yyyy hh24:mi:ss') as clock_out,
	absenteeism::varchar, 
	tardiness::varchar,
	agent_name,
	to_char(shift_start::date,'mm/dd/yyyy') as local_date_created 
from 
	d_attendance_kpi_summary daks  
where 
	client_account = 'urbanstems' 
	and shift_start >= '2021-01-01 00:00:00'
order by shift_start::date desc, agent_name asc
limit 10000
