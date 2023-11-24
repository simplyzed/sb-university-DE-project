UPDATE deaise.zale_dwh_fact_passport_blacklist tgt
SET entry_dt = tmp.entry_dt
FROM (
  SELECT stg.passport_num,
		stg.entry_dt
  FROM deaise.zale_stg_passport_blacklist stg
    INNER JOIN deaise.zale_dwh_fact_passport_blacklist tgt
          on 1=1
          and stg.passport_num = tgt.passport_num
  WHERE (1=0
         or stg.entry_dt <> tgt.entry_dt or (stg.entry_dt is null and tgt.entry_dt is not null) or (stg.entry_dt is not null and tgt.entry_dt is null)
        )
) tmp
WHERE tgt.passport_num = tmp.passport_num