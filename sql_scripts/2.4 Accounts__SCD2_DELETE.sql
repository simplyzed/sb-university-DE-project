INSERT INTO deaise.zale_dwh_dim_accounts_hist (
  account,
	valid_to,
	client,
	effective_from,
	effective_to,
	deleted_flg)
  SELECT tgt.account,
	tgt.valid_to,
	tgt.client,
	now() effective_from,
	to_date('9999-12-31','YYYY-MM-DD') effective_to,
	'Y' deleted_flg 
  FROM deaise.zale_dwh_dim_accounts_hist tgt
      LEFT JOIN deaise.zale_stg_accounts stg
          on 1 = 1
          and stg.account = tgt.account
  WHERE stg.account is null
        and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
        and tgt.deleted_flg = 'N'
;
UPDATE deaise.zale_dwh_dim_accounts_hist tgt
  SET effective_to = now() - interval '1 second'
WHERE tgt.account in (
    SELECT tgt.account
    FROM deaise.zale_dwh_dim_accounts_hist tgt
    LEFT JOIN deaise.zale_stg_accounts stg
          on 1=1
          and stg.account = tgt.account
    WHERE stg.account is null
        and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
        and tgt.deleted_flg = 'N'
  ) 
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N'
