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
  SELECT stg.client_id,
	stg.last_name,
	stg.first_name,
	stg.patronymic,
	stg.date_of_birth,
	stg.passport_num,
	stg.passport_valid_to,
	stg.phone,
	stg.update_dt effective_from,
	to_date('9999-12-31','YYYY-MM-DD') effective_to,
	'N' deleted_flg 
  FROM deaise.zale_stg_clients stg
      INNER JOIN deaise.zale_dwh_dim_clients_hist tgt
          on 1 = 1
          and stg.client_id = tgt.client_id
          and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  WHERE (stg.last_name <> tgt.last_name or (stg.last_name is null and tgt.last_name is not null) or (stg.last_name is not null and tgt.last_name is null)
		 or stg.first_name <> tgt.first_name or (stg.first_name is null and tgt.first_name is not null) or (stg.first_name is not null and tgt.first_name is null)
		 or stg.patronymic <> tgt.patronymic or (stg.patronymic is null and tgt.patronymic is not null) or (stg.patronymic is not null and tgt.patronymic is null)
		 or stg.date_of_birth <> tgt.date_of_birth or (stg.date_of_birth is null and tgt.date_of_birth is not null) or (stg.date_of_birth is not null and tgt.date_of_birth is null)
		 or stg.passport_num <> tgt.passport_num or (stg.passport_num is null and tgt.passport_num is not null) or (stg.passport_num is not null and tgt.passport_num is null)
		 or stg.passport_valid_to <> tgt.passport_valid_to or (stg.passport_valid_to is null and tgt.passport_valid_to is not null) or (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
		 or stg.phone <> tgt.phone or (stg.phone is null and tgt.phone is not null) or (stg.phone is not null and tgt.phone is null))
        or tgt.deleted_flg = 'Y'
;
UPDATE deaise.zale_dwh_dim_clients_hist tgt
  SET effective_to = tmp.update_dt - interval '1 second'
FROM (
  SELECT stg.client_id,
	stg.last_name,
	stg.first_name,
	stg.patronymic,
	stg.date_of_birth,
	stg.passport_num,
	stg.passport_valid_to,
	stg.phone,
	stg.update_dt
  FROM deaise.zale_stg_clients stg
    INNER JOIN deaise.zale_dwh_dim_clients_hist tgt
          on 1=1
          and stg.client_id = tgt.client_id
          and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  WHERE (1=0
         or stg.last_name <> tgt.last_name or (stg.last_name is null and tgt.last_name is not null) or (stg.last_name is not null and tgt.last_name is null)
		 or stg.first_name <> tgt.first_name or (stg.first_name is null and tgt.first_name is not null) or (stg.first_name is not null and tgt.first_name is null)
		 or stg.patronymic <> tgt.patronymic or (stg.patronymic is null and tgt.patronymic is not null) or (stg.patronymic is not null and tgt.patronymic is null)
		 or stg.date_of_birth <> tgt.date_of_birth or (stg.date_of_birth is null and tgt.date_of_birth is not null) or (stg.date_of_birth is not null and tgt.date_of_birth is null)
		 or stg.passport_num <> tgt.passport_num or (stg.passport_num is null and tgt.passport_num is not null) or (stg.passport_num is not null and tgt.passport_num is null)
		 or stg.passport_valid_to <> tgt.passport_valid_to or (stg.passport_valid_to is null and tgt.passport_valid_to is not null) or (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
		 or stg.phone <> tgt.phone or (stg.phone is null and tgt.phone is not null) or (stg.phone is not null and tgt.phone is null)
        )
        or tgt.deleted_flg = 'Y'
) tmp
WHERE 1=1
      and tgt.client_id = tmp.client_id
      and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
      and (tmp.last_name <> tgt.last_name or (tmp.last_name is null and tgt.last_name is not null) or (tmp.last_name is not null and tgt.last_name is null)
		 or tmp.first_name <> tgt.first_name or (tmp.first_name is null and tgt.first_name is not null) or (tmp.first_name is not null and tgt.first_name is null)
		 or tmp.patronymic <> tgt.patronymic or (tmp.patronymic is null and tgt.patronymic is not null) or (tmp.patronymic is not null and tgt.patronymic is null)
		 or tmp.date_of_birth <> tgt.date_of_birth or (tmp.date_of_birth is null and tgt.date_of_birth is not null) or (tmp.date_of_birth is not null and tgt.date_of_birth is null)
		 or tmp.passport_num <> tgt.passport_num or (tmp.passport_num is null and tgt.passport_num is not null) or (tmp.passport_num is not null and tgt.passport_num is null)
		 or tmp.passport_valid_to <> tgt.passport_valid_to or (tmp.passport_valid_to is null and tgt.passport_valid_to is not null) or (tmp.passport_valid_to is not null and tgt.passport_valid_to is null)
		 or tmp.phone <> tgt.phone or (tmp.phone is null and tgt.phone is not null) or (tmp.phone is not null and tgt.phone is null)
           or tgt.deleted_flg = 'Y')
