DELETE from deaise.zale_dwh_dim_cards tgt
WHERE card_num in (
  SELECT stg.card_num FROM deaise.zale_dwh_dim_cards tgt
      LEFT JOIN deaise.zale_stg_cards stg
          on 1 = 1
          and stg.card_num = tgt.card_num
  WHERE stg.card_num is null
)