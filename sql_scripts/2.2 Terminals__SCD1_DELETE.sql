DELETE from deaise.zale_dwh_dim_terminals tgt
WHERE terminal_id in (
  SELECT stg.terminal_id FROM deaise.zale_dwh_dim_terminals tgt
      LEFT JOIN deaise.zale_stg_terminals stg
          on 1 = 1
          and stg.terminal_id = tgt.terminal_id
  WHERE stg.terminal_id is null
)