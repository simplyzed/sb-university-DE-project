UPDATE deaise.zale_dwh_dim_clients tgt
SET last_name = tmp.last_name,
	first_name = tmp.first_name,
	patronymic = tmp.patronymic,
	date_of_birth = tmp.date_of_birth,
	passport_num = tmp.passport_num,
	passport_valid_to = tmp.passport_valid_to,
	phone = tmp.phone
FROM (
  SELECT stg.client_id,
		stg.last_name,
		stg.first_name,
		stg.patronymic,
		stg.date_of_birth,
		stg.passport_num,
		stg.passport_valid_to,
		stg.phone
  FROM deaise.zale_stg_clients stg
    INNER JOIN deaise.zale_dwh_dim_clients tgt
          on 1=1
          and stg.client_id = tgt.client_id
  WHERE (1=0
         or stg.last_name <> tgt.last_name or (stg.last_name is null and tgt.last_name is not null) or (stg.last_name is not null and tgt.last_name is null)
		 or stg.first_name <> tgt.first_name or (stg.first_name is null and tgt.first_name is not null) or (stg.first_name is not null and tgt.first_name is null)
		 or stg.patronymic <> tgt.patronymic or (stg.patronymic is null and tgt.patronymic is not null) or (stg.patronymic is not null and tgt.patronymic is null)
		 or stg.date_of_birth <> tgt.date_of_birth or (stg.date_of_birth is null and tgt.date_of_birth is not null) or (stg.date_of_birth is not null and tgt.date_of_birth is null)
		 or stg.passport_num <> tgt.passport_num or (stg.passport_num is null and tgt.passport_num is not null) or (stg.passport_num is not null and tgt.passport_num is null)
		 or stg.passport_valid_to <> tgt.passport_valid_to or (stg.passport_valid_to is null and tgt.passport_valid_to is not null) or (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
		 or stg.phone <> tgt.phone or (stg.phone is null and tgt.phone is not null) or (stg.phone is not null and tgt.phone is null)
        )
) tmp
WHERE tgt.client_id = tmp.client_id