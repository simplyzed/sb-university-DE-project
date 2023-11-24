INSERT INTO deaise.zale_dwh_dim_cards (
  card_num,
	account,
	create_dt)
  SELECT stg.card_num,
	stg.account,
	stg.create_dt create_dt 
  FROM deaise.zale_stg_cards stg
      LEFT JOIN deaise.zale_dwh_dim_cards tgt
          on 1 = 1
          and stg.card_num = tgt.card_num
  WHERE tgt.card_num is null