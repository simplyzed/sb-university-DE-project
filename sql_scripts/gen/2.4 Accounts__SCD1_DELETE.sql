DELETE from deaise.zale_dwh_dim_accounts tgt
WHERE account in (
  SELECT stg.account FROM deaise.zale_dwh_dim_accounts tgt
      LEFT JOIN deaise.zale_stg_accounts stg
          on 1 = 1
          and stg.account = tgt.account
  WHERE stg.account is null
)