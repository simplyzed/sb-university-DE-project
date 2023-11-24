DELETE from deaise.zale_dwh_dim_clients tgt
WHERE client_id in (
  SELECT stg.client_id FROM deaise.zale_dwh_dim_clients tgt
      LEFT JOIN deaise.zale_stg_clients stg
          on 1 = 1
          and stg.client_id = tgt.client_id
  WHERE stg.client_id is null
)