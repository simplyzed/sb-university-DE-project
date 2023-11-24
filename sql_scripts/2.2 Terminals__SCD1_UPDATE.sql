UPDATE deaise.zale_dwh_dim_terminals tgt
SET terminal_type = tmp.terminal_type,
	terminal_city = tmp.terminal_city,
	terminal_address = tmp.terminal_address
FROM (
  SELECT stg.terminal_id,
		stg.terminal_type,
		stg.terminal_city,
		stg.terminal_address
  FROM deaise.zale_stg_terminals stg
    INNER JOIN deaise.zale_dwh_dim_terminals tgt
          on 1=1
          and stg.terminal_id = tgt.terminal_id
  WHERE (1=0
         or stg.terminal_type <> tgt.terminal_type or (stg.terminal_type is null and tgt.terminal_type is not null) or (stg.terminal_type is not null and tgt.terminal_type is null)
		 or stg.terminal_city <> tgt.terminal_city or (stg.terminal_city is null and tgt.terminal_city is not null) or (stg.terminal_city is not null and tgt.terminal_city is null)
		 or stg.terminal_address <> tgt.terminal_address or (stg.terminal_address is null and tgt.terminal_address is not null) or (stg.terminal_address is not null and tgt.terminal_address is null)
        )
) tmp
WHERE tgt.terminal_id = tmp.terminal_id