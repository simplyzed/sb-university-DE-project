INSERT INTO deaise.zale_dwh_dim_terminals_hist (
  terminal_id,
	terminal_type,
	terminal_city,
	terminal_address,
	effective_from,
	effective_to,
	deleted_flg)
  SELECT stg.terminal_id,
	stg.terminal_type,
	stg.terminal_city,
	stg.terminal_address,
	stg.file_dt effective_from,
	to_date('9999-12-31','YYYY-MM-DD') effective_to,
	'N' deleted_flg 
  FROM deaise.zale_stg_terminals stg
      LEFT JOIN deaise.zale_dwh_dim_terminals_hist tgt
          on 1 = 1
          and stg.terminal_id = tgt.terminal_id
  WHERE tgt.terminal_id is null