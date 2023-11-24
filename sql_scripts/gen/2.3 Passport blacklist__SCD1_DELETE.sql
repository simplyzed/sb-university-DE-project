DELETE from deaise.zale_dwh_fact_passport_blacklist tgt
WHERE passport_num in (
  SELECT stg.passport_num FROM deaise.zale_dwh_fact_passport_blacklist tgt
      LEFT JOIN deaise.zale_stg_passport_blacklist stg
          on 1 = 1
          and stg.passport_num = tgt.passport_num
  WHERE stg.passport_num is null
)