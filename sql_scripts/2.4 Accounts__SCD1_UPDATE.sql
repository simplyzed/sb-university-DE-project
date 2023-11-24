UPDATE deaise.zale_dwh_dim_accounts tgt
SET valid_to = tmp.valid_to,
	client = tmp.client
FROM (
  SELECT stg.account,
		stg.valid_to,
		stg.client
  FROM deaise.zale_stg_accounts stg
    INNER JOIN deaise.zale_dwh_dim_accounts tgt
          on 1=1
          and stg.account = tgt.account
  WHERE (1=0
         or stg.valid_to <> tgt.valid_to or (stg.valid_to is null and tgt.valid_to is not null) or (stg.valid_to is not null and tgt.valid_to is null)
		 or stg.client <> tgt.client or (stg.client is null and tgt.client is not null) or (stg.client is not null and tgt.client is null)
        )
) tmp
WHERE tgt.account = tmp.account