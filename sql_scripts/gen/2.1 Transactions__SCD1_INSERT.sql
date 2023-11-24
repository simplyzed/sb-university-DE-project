INSERT INTO deaise.zale_dwh_fact_transactions (
  trans_id,
	trans_date,
	card_num,
	oper_type,
	amt,
	oper_result,
	terminal)
  SELECT stg.trans_id,
	stg.trans_date,
	stg.card_num,
	stg.oper_type,
	stg.amt,
	stg.oper_result,
	stg.terminal 
  FROM deaise.zale_stg_transactions stg
      LEFT JOIN deaise.zale_dwh_fact_transactions tgt
          on 1 = 1
          and stg.trans_id = tgt.trans_id
  WHERE tgt.trans_id is null