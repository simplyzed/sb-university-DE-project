INSERT INTO deaise.zale_dwh_dim_cards_hist (
  card_num,
	account,
	effective_from,
	effective_to,
	deleted_flg)
  SELECT tgt.card_num,
	tgt.account,
	now() effective_from,
	to_date('9999-12-31','YYYY-MM-DD') effective_to,
	'Y' deleted_flg 
  FROM deaise.zale_dwh_dim_cards_hist tgt
      LEFT JOIN deaise.zale_stg_cards stg
          on 1 = 1
          and stg.card_num = tgt.card_num
  WHERE stg.card_num is null
        and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
        and tgt.deleted_flg = 'N'
;
UPDATE deaise.zale_dwh_dim_cards_hist tgt
  SET effective_to = now() - interval '1 second'
WHERE tgt.card_num in (
    SELECT tgt.card_num
    FROM deaise.zale_dwh_dim_cards_hist tgt
    LEFT JOIN deaise.zale_stg_cards stg
          on 1=1
          and stg.card_num = tgt.card_num
    WHERE stg.card_num is null
        and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
        and tgt.deleted_flg = 'N'
  ) 
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N'
