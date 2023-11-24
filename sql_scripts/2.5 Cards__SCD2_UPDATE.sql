INSERT INTO deaise.zale_dwh_dim_cards_hist (
  card_num,
	account,
	effective_from,
	effective_to,
	deleted_flg)
  SELECT stg.card_num,
	stg.account,
	stg.update_dt effective_from,
	to_date('9999-12-31','YYYY-MM-DD') effective_to,
	'N' deleted_flg 
  FROM deaise.zale_stg_cards stg
      INNER JOIN deaise.zale_dwh_dim_cards_hist tgt
          on 1 = 1
          and stg.card_num = tgt.card_num
          and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  WHERE (stg.account <> tgt.account or (stg.account is null and tgt.account is not null) or (stg.account is not null and tgt.account is null))
        or tgt.deleted_flg = 'Y'
;
UPDATE deaise.zale_dwh_dim_cards_hist tgt
  SET effective_to = tmp.update_dt - interval '1 second'
FROM (
  SELECT stg.card_num,
	stg.account,
	stg.update_dt
  FROM deaise.zale_stg_cards stg
    INNER JOIN deaise.zale_dwh_dim_cards_hist tgt
          on 1=1
          and stg.card_num = tgt.card_num
          and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  WHERE (1=0
         or stg.account <> tgt.account or (stg.account is null and tgt.account is not null) or (stg.account is not null and tgt.account is null)
        )
        or tgt.deleted_flg = 'Y'
) tmp
WHERE 1=1
      and tgt.card_num = tmp.card_num
      and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
      and (tmp.account <> tgt.account or (tmp.account is null and tgt.account is not null) or (tmp.account is not null and tgt.account is null)
           or tgt.deleted_flg = 'Y')
