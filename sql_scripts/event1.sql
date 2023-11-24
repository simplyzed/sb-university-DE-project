INSERT INTO deaise.zale_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
select 
	t.trans_date event_dt,
	t.passport_num passport,
	concat(t.last_name,' ',t.first_name,' ', t.patronymic) fio,
	t.phone phone,
	1 event_type,
	date_trunc('day',
		(select min(max_update_dt) report_dt   
		 from deaise.zale_meta_loadinfo m
		 where src in ('file_transactions','file_passport_blacklist')
		)
	)::date report_dt
from (
	select --*
	event1.trans_date,cli.*
	from deaise.zale_dwh_dim_clients cli
	left join deaise.zale_dwh_fact_passport_blacklist p
		on p.passport_num = cli.passport_num 
	left join deaise.zale_dwh_dim_accounts a
		on a.client = cli.client_id 
	left join deaise.zale_dwh_dim_cards c
		on c.account = a.account 
	left join lateral 
		(select tr.trans_id, tr.trans_date  
		 from deaise.zale_dwh_fact_transactions tr
		 where tr.card_num = c.card_num
		 and (  p.entry_dt <= tr.trans_date
			or cli.passport_valid_to < tr.trans_date 
			--or a.valid_to < tr.trans_date
			)
		 --limit 1
		) event1 on true
	where event1.trans_id is not null
	order by event1.trans_date
) t
