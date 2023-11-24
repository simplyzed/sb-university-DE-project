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
      INNER JOIN deaise.zale_dwh_dim_terminals_hist tgt
          on 1 = 1
          and stg.terminal_id = tgt.terminal_id
          and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  WHERE (stg.terminal_type <> tgt.terminal_type or (stg.terminal_type is null and tgt.terminal_type is not null) or (stg.terminal_type is not null and tgt.terminal_type is null)
		 or stg.terminal_city <> tgt.terminal_city or (stg.terminal_city is null and tgt.terminal_city is not null) or (stg.terminal_city is not null and tgt.terminal_city is null)
		 or stg.terminal_address <> tgt.terminal_address or (stg.terminal_address is null and tgt.terminal_address is not null) or (stg.terminal_address is not null and tgt.terminal_address is null))
        or tgt.deleted_flg = 'Y'
;
UPDATE deaise.zale_dwh_dim_terminals_hist tgt
  SET effective_to = tmp.file_dt - interval '1 second'
FROM (
  SELECT stg.terminal_id,
	stg.terminal_type,
	stg.terminal_city,
	stg.terminal_address,
	stg.file_dt
  FROM deaise.zale_stg_terminals stg
    INNER JOIN deaise.zale_dwh_dim_terminals_hist tgt
          on 1=1
          and stg.terminal_id = tgt.terminal_id
          and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  WHERE (1=0
         or stg.terminal_type <> tgt.terminal_type or (stg.terminal_type is null and tgt.terminal_type is not null) or (stg.terminal_type is not null and tgt.terminal_type is null)
		 or stg.terminal_city <> tgt.terminal_city or (stg.terminal_city is null and tgt.terminal_city is not null) or (stg.terminal_city is not null and tgt.terminal_city is null)
		 or stg.terminal_address <> tgt.terminal_address or (stg.terminal_address is null and tgt.terminal_address is not null) or (stg.terminal_address is not null and tgt.terminal_address is null)
        )
        or tgt.deleted_flg = 'Y'
) tmp
WHERE 1=1
      and tgt.terminal_id = tmp.terminal_id
      and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
      and (tmp.terminal_type <> tgt.terminal_type or (tmp.terminal_type is null and tgt.terminal_type is not null) or (tmp.terminal_type is not null and tgt.terminal_type is null)
		 or tmp.terminal_city <> tgt.terminal_city or (tmp.terminal_city is null and tgt.terminal_city is not null) or (tmp.terminal_city is not null and tgt.terminal_city is null)
		 or tmp.terminal_address <> tgt.terminal_address or (tmp.terminal_address is null and tgt.terminal_address is not null) or (tmp.terminal_address is not null and tgt.terminal_address is null)
           or tgt.deleted_flg = 'Y')
