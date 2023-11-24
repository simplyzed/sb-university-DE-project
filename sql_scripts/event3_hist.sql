INSERT INTO deaise.zale_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
with event3 as (
	select 
		cli.*,
		pos.terminal_city,
		tr.trans_date,
		lead (trans_date) over (partition by tr.card_num order by trans_date) next_dt,
		lead (terminal_city) over (partition by tr.card_num order by trans_date) next_city
	from deaise.zale_dwh_fact_transactions tr
	left join deaise.zale_dwh_dim_cards_hist c
		on tr.card_num = c.card_num 
		and c.effective_to = to_date('9999-12-31','YYYY-MM-DD')
        and c.deleted_flg = 'N'
	left join deaise.zale_dwh_dim_accounts_hist a
		on c.account = a.account  
		and a.effective_to = to_date('9999-12-31','YYYY-MM-DD')
        and a.deleted_flg = 'N'
	left join deaise.zale_dwh_dim_clients_hist cli
		on a.client = cli.client_id 
		and cli.effective_to = to_date('9999-12-31','YYYY-MM-DD')
        and cli.deleted_flg = 'N'
	left join deaise.zale_dwh_dim_terminals_hist pos
		on tr.terminal = pos.terminal_id 
		and pos.effective_to = to_date('9999-12-31','YYYY-MM-DD')
        and pos.deleted_flg = 'N'
	order by client_id,trans_date 
)
select 
	t.trans_date event_dt,
	t.passport_num passport,
	concat(t.last_name,' ',t.first_name,' ', t.patronymic) fio,
	t.phone phone,
	3 event_type,
	date_trunc('day',
		(select min(max_update_dt) report_dt   
		 from deaise.zale_meta_loadinfo m
		 where src in ('file_transactions', 'file_terminals')
		)
	)::date report_dt
from event3 t
where 1=1
	and t.terminal_city <> next_city	
	and next_dt - trans_date < interval '1 hour'
order by t.trans_date