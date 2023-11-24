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
	stg.update_dt effective_from,
	to_date('9999-12-31','YYYY-MM-DD') effective_to,
	'N' deleted_flg 
  FROM deaise.zale_stg_accounts stg
      INNER JOIN deaise.zale_dwh_dim_accounts_hist tgt
          on 1 = 1
          and stg.account = tgt.account
          and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  WHERE (stg.valid_to <> tgt.valid_to or (stg.valid_to is null and tgt.valid_to is not null) or (stg.valid_to is not null and tgt.valid_to is null)
		 or stg.client <> tgt.client or (stg.client is null and tgt.client is not null) or (stg.client is not null and tgt.client is null))
        or tgt.deleted_flg = 'Y'
;
UPDATE deaise.zale_dwh_dim_accounts_hist tgt
  SET effective_to = tmp.update_dt - interval '1 second'
FROM (
  SELECT stg.account,
	stg.valid_to,
	stg.client,
	stg.update_dt
  FROM deaise.zale_stg_accounts stg
    INNER JOIN deaise.zale_dwh_dim_accounts_hist tgt
          on 1=1
          and stg.account = tgt.account
          and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  WHERE (1=0
         or stg.valid_to <> tgt.valid_to or (stg.valid_to is null and tgt.valid_to is not null) or (stg.valid_to is not null and tgt.valid_to is null)
		 or stg.client <> tgt.client or (stg.client is null and tgt.client is not null) or (stg.client is not null and tgt.client is null)
        )
        or tgt.deleted_flg = 'Y'
) tmp
WHERE 1=1
      and tgt.account = tmp.account
      and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
      and (tmp.valid_to <> tgt.valid_to or (tmp.valid_to is null and tgt.valid_to is not null) or (tmp.valid_to is not null and tgt.valid_to is null)
		 or tmp.client <> tgt.client or (tmp.client is null and tgt.client is not null) or (tmp.client is not null and tgt.client is null)
           or tgt.deleted_flg = 'Y')
