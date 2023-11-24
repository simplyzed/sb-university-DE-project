INSERT INTO deaise.zale_dwh_dim_accounts (
  account,
	valid_to,
	client,
	create_dt)
  SELECT stg.account,
	stg.valid_to,
	stg.client,
	stg.create_dt create_dt 
  FROM deaise.zale_stg_accounts stg
      LEFT JOIN deaise.zale_dwh_dim_accounts tgt
          on 1 = 1
          and stg.account = tgt.account
  WHERE tgt.account is null