INSERT INTO deaise.zale_dwh_fact_passport_blacklist (
  passport_num,
	entry_dt)
  SELECT stg.passport_num,
	stg.entry_dt 
  FROM deaise.zale_stg_passport_blacklist stg
      LEFT JOIN deaise.zale_dwh_fact_passport_blacklist tgt
          on 1 = 1
          and stg.passport_num = tgt.passport_num
  WHERE tgt.passport_num is null