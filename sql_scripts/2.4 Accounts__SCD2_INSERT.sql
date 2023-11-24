INSERT INTO deaise.zale_dwh_dim_accounts_hist (
  account,
	valid_to,
	client,
	effective_from,
	effective_to,
	deleted_flg)
  SELECT stg.account,
	stg.valid_to,
	stg.client,
	stg.create_dt effective_from,
	to_date('9999-12-31','YYYY-MM-DD') effective_to,
	'N' deleted_flg 
  FROM deaise.zale_stg_accounts stg
      LEFT JOIN deaise.zale_dwh_dim_accounts_hist tgt
          on 1 = 1
          and stg.account = tgt.account
  WHERE tgt.account is null