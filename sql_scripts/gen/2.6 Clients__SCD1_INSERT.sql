INSERT INTO deaise.zale_dwh_dim_clients (
  client_id,
	last_name,
	first_name,
	patronymic,
	date_of_birth,
	passport_num,
	passport_valid_to,
	phone,
	create_dt)
  SELECT stg.client_id,
	stg.last_name,
	stg.first_name,
	stg.patronymic,
	stg.date_of_birth,
	stg.passport_num,
	stg.passport_valid_to,
	stg.phone,
	stg.create_dt create_dt 
  FROM deaise.zale_stg_clients stg
      LEFT JOIN deaise.zale_dwh_dim_clients tgt
          on 1 = 1
          and stg.client_id = tgt.client_id
  WHERE tgt.client_id is null