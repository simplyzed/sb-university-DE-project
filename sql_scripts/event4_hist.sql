INSERT INTO deaise.zale_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
with trans as (
	select --*
	client_id, a.account, tr.trans_date, trans_id, amt, oper_result
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
	where oper_type in ('PAYMENT','WITHDRAW')
	order by a.account, trans_date 
),
event4 as ( 
	select
		t1.client_id,
		--first reject
		t1.trans_id 	id1,
		t1.trans_date 	dt1,
		t1.amt 			amt1,
		--second reject
		t2.trans_id 	id2,
		t2.trans_date 	dt2,
		t2.amt 			amt2,
		--third reject
		t3.trans_id 	id3,
		t3.trans_date 	dt3,
		t3.amt 			amt3,
		--fourth success
		t4.trans_id 	id4,
		t4.trans_date 	dt4,
		t4.amt 			amt4
	from trans t1
	left join lateral
		(select *
			from trans tt 
			where 1=1
				and tt.account = t1.account
				and tt.oper_result = 'REJECT' 
				and tt.trans_date > t1.trans_date 
				and tt.trans_date <= t1.trans_date + interval '20 min'
				and tt.amt<t1.amt
		) t2 on true
	left join lateral
		(select *
			from trans tt 
			where 1=1
				and tt.account = t1.account
				and tt.oper_result = 'REJECT' 
				and tt.trans_date > t2.trans_date 
				and tt.trans_date <= t1.trans_date + interval '20 min'
				and tt.amt<t2.amt
		) t3 on true
	left join lateral
		(select *
			from trans tt 
			where 1=1
				and tt.account = t1.account
				and tt.oper_result = 'SUCCESS' 
				and tt.trans_date > t3.trans_date 
				and tt.trans_date <= t1.trans_date + interval '20 min'
				and tt.amt<t3.amt
			order by tt.trans_date asc
			limit 1
		) t4 on true
	where 1=1
		and t2.trans_id is not null 
		and t3.trans_id is not null 
		and t4.trans_id is not null 
		and t1.oper_result = 'REJECT'
	order by t4.trans_date
)
select 
	e.dt4 event_dt,
	cli.passport_num passport,
	concat(cli.last_name,' ',cli.first_name,' ', cli.patronymic) fio,
	cli.phone phone,
	4 event_type,
	date_trunc('day',
		(select min(max_update_dt) report_dt   
		 from deaise.zale_meta_loadinfo m
		 where src in ('file_transactions')
		)
	)::date report_dt
from event4 e
left join deaise.zale_dwh_dim_clients_hist cli 
	on e.client_id = cli.client_id 