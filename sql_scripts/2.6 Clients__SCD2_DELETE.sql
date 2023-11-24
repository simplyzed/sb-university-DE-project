INSERT INTO deaise.zale_dwh_dim_clients_hist (
  client_id,
	last_name,
	first_name,
	patronymic,
	date_of_birth,
	passport_num,
	passport_valid_to,
	phone,
	effective_from,
	effective_to,
	deleted_flg)
  SELECT tgt.client_id,
	tgt.last_name,
	tgt.first_name,
	tgt.patronymic,
	tgt.date_of_birth,
	tgt.passport_num,
	tgt.passport_valid_to,
	tgt.phone,
	now() effective_from,
	to_date('9999-12-31','YYYY-MM-DD') effective_to,
	'Y' deleted_flg 
  FROM deaise.zale_dwh_dim_clients_hist tgt
      LEFT JOIN deaise.zale_stg_clients stg
          on 1 = 1
          and stg.client_id = tgt.client_id
  WHERE stg.client_id is null
        and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
        and tgt.deleted_flg = 'N'
;
UPDATE deaise.zale_dwh_dim_clients_hist tgt
  SET effective_to = now() - interval '1 second'
WHERE tgt.client_id in (
    SELECT tgt.client_id
    FROM deaise.zale_dwh_dim_clients_hist tgt
    LEFT JOIN deaise.zale_stg_clients stg
          on 1=1
          and stg.client_id = tgt.client_id
    WHERE stg.client_id is null
        and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
        and tgt.deleted_flg = 'N'
  ) 
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N'
