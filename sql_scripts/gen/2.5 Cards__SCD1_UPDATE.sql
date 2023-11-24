UPDATE deaise.zale_dwh_dim_cards tgt
SET account = tmp.account
FROM (
  SELECT stg.card_num,
		stg.account
  FROM deaise.zale_stg_cards stg
    INNER JOIN deaise.zale_dwh_dim_cards tgt
          on 1=1
          and stg.card_num = tgt.card_num
  WHERE (1=0
         or stg.account <> tgt.account or (stg.account is null and tgt.account is not null) or (stg.account is not null and tgt.account is null)
        )
) tmp
WHERE tgt.card_num = tmp.card_num