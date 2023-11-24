INSERT INTO deaise.zale_dwh_dim_terminals (
  terminal_id,
	terminal_type,
	terminal_city,
	terminal_address,
	create_dt)
  SELECT stg.terminal_id,
	stg.terminal_type,
	stg.terminal_city,
	stg.terminal_address,
	stg.file_dt create_dt 
  FROM deaise.zale_stg_terminals stg
      LEFT JOIN deaise.zale_dwh_dim_terminals tgt
          on 1 = 1
          and stg.terminal_id = tgt.terminal_id
  WHERE tgt.terminal_id is null