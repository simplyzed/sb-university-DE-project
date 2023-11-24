INSERT INTO deaise.zale_dwh_dim_terminals_hist (
  terminal_id,
	terminal_type,
	terminal_city,
	terminal_address,
	effective_from,
	effective_to,
	deleted_flg)
  SELECT tgt.terminal_id,
	tgt.terminal_type,
	tgt.terminal_city,
	tgt.terminal_address,
	now() effective_from,
	to_date('9999-12-31','YYYY-MM-DD') effective_to,
	'Y' deleted_flg 
  FROM deaise.zale_dwh_dim_terminals_hist tgt
      LEFT JOIN deaise.zale_stg_terminals stg
          on 1 = 1
          and stg.terminal_id = tgt.terminal_id
  WHERE stg.terminal_id is null
        and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
        and tgt.deleted_flg = 'N'
;
UPDATE deaise.zale_dwh_dim_terminals_hist tgt
  SET effective_to = now() - interval '1 second'
WHERE tgt.terminal_id in (
    SELECT tgt.terminal_id
    FROM deaise.zale_dwh_dim_terminals_hist tgt
    LEFT JOIN deaise.zale_stg_terminals stg
          on 1=1
          and stg.terminal_id = tgt.terminal_id
    WHERE stg.terminal_id is null
        and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
        and tgt.deleted_flg = 'N'
  ) 
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N'
