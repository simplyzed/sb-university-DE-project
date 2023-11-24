UPDATE deaise.zale_dwh_fact_transactions tgt
SET trans_date = tmp.trans_date,
	card_num = tmp.card_num,
	oper_type = tmp.oper_type,
	amt = tmp.amt,
	oper_result = tmp.oper_result,
	terminal = tmp.terminal
FROM (
  SELECT stg.trans_id,
		stg.trans_date,
		stg.card_num,
		stg.oper_type,
		stg.amt,
		stg.oper_result,
		stg.terminal
  FROM deaise.zale_stg_transactions stg
    INNER JOIN deaise.zale_dwh_fact_transactions tgt
          on 1=1
          and stg.trans_id = tgt.trans_id
  WHERE (1=0
         or stg.trans_date <> tgt.trans_date or (stg.trans_date is null and tgt.trans_date is not null) or (stg.trans_date is not null and tgt.trans_date is null)
		 or stg.card_num <> tgt.card_num or (stg.card_num is null and tgt.card_num is not null) or (stg.card_num is not null and tgt.card_num is null)
		 or stg.oper_type <> tgt.oper_type or (stg.oper_type is null and tgt.oper_type is not null) or (stg.oper_type is not null and tgt.oper_type is null)
		 or stg.amt <> tgt.amt or (stg.amt is null and tgt.amt is not null) or (stg.amt is not null and tgt.amt is null)
		 or stg.oper_result <> tgt.oper_result or (stg.oper_result is null and tgt.oper_result is not null) or (stg.oper_result is not null and tgt.oper_result is null)
		 or stg.terminal <> tgt.terminal or (stg.terminal is null and tgt.terminal is not null) or (stg.terminal is not null and tgt.terminal is null)
        )
) tmp
WHERE tgt.trans_id = tmp.trans_id