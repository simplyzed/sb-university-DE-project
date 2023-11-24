INSERT INTO deaise.zale_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
select 
	t.trans_date event_dt,
	t.passport_num passport,
	concat(t.last_name,' ',t.first_name,' ', t.patronymic) fio,
	t.phone phone,
	2 event_type,
	date_trunc('day',
		(select min(max_update_dt) report_dt   
		 from deaise.zale_meta_loadinfo m
		 where src in ('file_transactions')
		)
	)::date report_dt
from (
	select --*
		event2.trans_date,cli.*
	from deaise.zale_dwh_dim_clients_hist cli
	left join deaise.zale_dwh_dim_accounts_hist a
		on a.client = cli.client_id 
		and a.effective_to = to_date('9999-12-31','YYYY-MM-DD')
        and a.deleted_flg = 'N'
	left join deaise.zale_dwh_dim_cards_hist c
		on c.account = a.account 
		and c.effective_to = to_date('9999-12-31','YYYY-MM-DD')
        and c.deleted_flg = 'N'
	left join lateral 
		(select tr.trans_id, tr.trans_date  
		 from deaise.zale_dwh_fact_transactions tr
		 where tr.card_num = c.card_num
		 and (  --p.entry_dt <= tr.trans_date
				--or cli.passport_valid_to < tr.trans_date 
			a.valid_to < tr.trans_date
			)
		 --limit 1
		) event2 on true
	where event2.trans_id is not null
		and cli.effective_to = to_date('9999-12-31','YYYY-MM-DD')
        and cli.deleted_flg = 'N'
	order by event2.trans_date
) t