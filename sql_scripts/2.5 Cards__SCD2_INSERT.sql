INSERT INTO deaise.zale_dwh_dim_cards_hist (
  card_num,
	account,
	effective_from,
	effective_to,
	deleted_flg)
  SELECT stg.card_num,
	stg.account,
	stg.create_dt effective_from,
	to_date('9999-12-31','YYYY-MM-DD') effective_to,
	'N' deleted_flg 
  FROM deaise.zale_stg_cards stg
      LEFT JOIN deaise.zale_dwh_dim_cards_hist tgt
          on 1 = 1
          and stg.card_num = tgt.card_num
  WHERE tgt.card_num is null