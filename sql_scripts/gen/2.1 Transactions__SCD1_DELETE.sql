DELETE from deaise.zale_dwh_fact_transactions tgt
WHERE trans_id in (
  SELECT stg.trans_id FROM deaise.zale_dwh_fact_transactions tgt
      LEFT JOIN deaise.zale_stg_transactions stg
          on 1 = 1
          and stg.trans_id = tgt.trans_id
  WHERE stg.trans_id is null
)