#import os
from pathlib import Path
from psycopg2.extras import execute_batch
from .config import Config
import time
from datetime import datetime
import pandas as pd
import os
import enum
 
class SCD_Type(enum.Enum):
    fact_scd0 = 0
    dim_scd1 = 1
    dim_scd2 = 2

class Query_config:

    def __init__(self, scd_type: SCD_Type):
        self.scd_type  = scd_type
        self.insert_columns_dds_add = ''
        self.insert_columns_stg_add = ''
        self.update_dds_set_add = ''
        self.update_stg_select_add = ''
        # self.insert_columns_dds_add = 'create_dt'
        # self.insert_columns_stg_add = 'stg.create_dt'
        # self.update_dds_set_add = 'update_dt = tmp.update_dt'
        # self.update_stg_select_add = 'stg.update_dt'
        self.stg_create_dt = 'create_dt'
        self.stg_update_dt = 'update_dt'
        self.dds_scd1_create_dt = 'create_dt'
        self.dds_scd1_update_dt = 'update_dt'
        self.dds_scd2_start_dt = 'effective_from'
        self.dds_scd2_end_dt = 'effective_to'
        self.dds_scd2_deleted_flg = 'deleted_flg'
        self.dds_scd2_delete_end_dt_value = 'now()'
        if self.scd_type == SCD_Type.dim_scd2 :
            self.scd_str = 'SCD2'
        else:
            self.scd_str = 'SCD1'
        # if self.scd_type == SCD_Type.fact_scd0 :
        #     self.insert_columns_dds_add = ''
        #     self.insert_columns_stg_add = ''
        #     self.update_dds_set_add = ''
        #     self.update_stg_select_add = ''



def sql_save( config: Config, description: str, suffix: str, save_txt):
    if suffix != '':
        fname = description + "__" + suffix 
    else:
        fname = description

    fname = config.gen_dir + '/' + fname + '.sql'

    with open( fname, 'w' ) as f:
        f.write(save_txt)

def sql_load( config: Config, description: str, read_only_suffix = '') -> dict:
    if read_only_suffix != '':
        search_for = description + "__" + read_only_suffix 
    else:
        search_for = description
    filenames = config.get_sql_files(search_for)
    sql_text_dict = {}
    for f in filenames:
        if '__' in f:
            suff = f.split('__')[-1]
            suff = suff.split('.')[0]
        else:
            suff = 'SQL'

        with open( f ) as ff:
            sql_text = ff.read()
            sql_text_dict[suff] = sql_text
    return sql_text_dict

def sql_get_from_dict(sql_dict:dict, key: str, description: str) -> str:
    if len(sql_dict) == 0:
        raise Exception (f'No sql files found for {description}')
    if key in sql_dict:
        return sql_dict[key]
    else:
        if len(sql_dict) == 1:
            return list(sql_dict.values())[0]
    raise Exception (f'Sql file {description}__{key} not found. Others found:', list(sql_dict.keys()))

def sql_file_exec( config: Config, description: str, cursor_alias: str, exec_only_suffix = ''):

    cursor = config.get_cursor(cursor_alias)
    print(description, datetime.now().isoformat(sep=' ', timespec='seconds'))
    queries_dict = sql_load(config, description, exec_only_suffix)
    for key,value in queries_dict.items():
        #start_time = time.time()
        if config.show_sql_and_exit :
            print(f"--- Skip {key}")
            #print(value)
            return
        print(f"--- Exec {key}")
        cursor.execute(value)
        #print(f"--->  {key} + fetch time: ", (time.time() - start_time ) )           

def dwh_meta_upd(config, src: str, src_time: datetime, cursor_dwh_alias = 'dwh'):
        cursor_dwh = config.get_cursor(cursor_dwh_alias)
        dt_sql = (
            f"UPDATE deaise.zale_meta_loadinfo "
            f"set max_update_dt = to_timestamp('{src_time.isoformat(sep=' ', timespec='seconds')}','YYYY-MM-DD HH24:MI:SS') "
            f"where src = '{src}'"
        )
        cursor_dwh.execute (dt_sql)

def get_table_columns( cursor, table_name: str, exclude_list = []) -> list:
    l = table_name.split(".")
    if len(l)!=2 :
        raise Exception("dwh table name must contain schema name")
    sql_col = (
        f" SELECT column_name FROM information_schema.columns"
        f" WHERE table_schema = '{l[0]}' and table_name = '{l[1]}'"
    )
    cursor.execute(sql_col)
    tuple_list = cursor.fetchall()
    return [item[0] for item in tuple_list if item[0] not in exclude_list]


def simple_src_to_dwh (config : Config,
                       description: str, 
                       src_table_name: str, 
                       dwh_table_name: str, 
                       cursor_src_alias = 'src', 
                       cursor_dwh_alias = 'dwh'):

    cursor_src = config.get_cursor(cursor_src_alias)
    cursor_dwh = config.get_cursor(cursor_dwh_alias)

    start_time = time.time()
    print(description, datetime.now().isoformat(sep=' ', timespec='seconds'))
    
    if config.use_static_sql_only:
        queries = sql_load (config, description)
        src_select = sql_get_from_dict (queries, 'SELECT', description)
        dwh_insert = sql_get_from_dict (queries, 'INSERT', description)
    else:
        if config.debug :
            print("--- get columns")   
        columns = get_table_columns(cursor_dwh, dwh_table_name)
        #columns.remove("update_dt")
        columns_dwh = ",\n\t".join( columns )
        insert_tmpl = ",".join([" %s"] * len(columns)) # join %s * n of columns
        #columns [ columns.index("update_dt") ] = "null" # update
        columns_src = ",\n\t".join( columns )

        src_select = f"SELECT {columns_src} FROM {src_table_name}"
        dwh_insert = f"INSERT INTO {dwh_table_name} ({columns_dwh}) VALUES ({insert_tmpl})"

        if config.generate_sql:
            sql_save(config, description, 'SELECT', src_select)
            sql_save(config, description, 'INSERT', dwh_insert)

    if config.show_sql_and_exit :
        print(src_select)
        print(dwh_insert)
        return

    print("--- SELECT from source:", src_table_name)   
    if config.debug :
        print("---: ", src_select)           
    cursor_src.execute(src_select) 
    if config.debug :
        print("--->  SELECT + fetch time: ", (time.time() - start_time ) )   
    
    start_time = time.time()
    print("--- INSERT into:", dwh_table_name)   
    if config.debug :
        print("---: ", dwh_insert)           
    for row in cursor_src.fetchall():
        cursor_dwh.execute(dwh_insert, row)
    if config.debug :
        print("--->  INSERT time: ", (time.time() - start_time) )   
    if config.show_stg_insert_count :
        chk_count = f'SELECT count(*) from {dwh_table_name}'
        cursor_dwh.execute( chk_count )
        tuple_list = cursor_dwh.fetchall()
        print("--->  Count(*):", tuple_list[0][0])
        

def file_to_dwh (config : Config,
                description: str, 
                src_file_name: str, 
                src_file_type: str,
                dwh_src_links: dict,
                dwh_table_name: str,
                read_file_params = {},
                add_col = '',
                add_col_value = '', 
                cursor_dwh_alias = 'dwh'):
    
    cursor_dwh = config.get_cursor(cursor_dwh_alias)
    start_time = time.time()
    print(description, datetime.now().isoformat(sep=' ', timespec='seconds'))

    if not (src_file_type == 'excel' or src_file_type == 'csv'):
        raise Exception("src_file_type must be 'excel' or 'csv'")

    if config.use_static_sql_only:
        queries = sql_load (config, description)
        dwh_insert = sql_get_from_dict (queries, 'INSERT', description)
    else:
        dwh_table_col_list = [key for key,value  in dwh_src_links.items()]
        dwh_table_cols = ",\n\t".join ( dwh_table_col_list )
        insert_tmpl = ",".join([" %s"] * len(dwh_table_col_list)) # join %s * n of columns
        dwh_insert = f"INSERT INTO {dwh_table_name} ({dwh_table_cols}) VALUES ({insert_tmpl})"
        if config.generate_sql:
            sql_save(config, description, 'INSERT', dwh_insert)

    if config.show_sql_and_exit :
        print(dwh_insert)
        return
    
    print(f"--- Read {src_file_type}: {src_file_name}")
    if src_file_type == 'csv':
        df = pd.read_csv( src_file_name, sep=';',**read_file_params )
    elif src_file_type == 'excel':
        df = pd.read_excel ( src_file_name, **read_file_params )
    
    if add_col != '':
        df[add_col] = add_col_value

    file_col_list = [value for key,value  in dwh_src_links.items()]
    df = df[file_col_list]
    if config.debug :
        print("--->  read time: ", (time.time() - start_time ) )   

    start_time = time.time()
    print("--- INSERT into:", dwh_table_name)   
    if config.debug :
        print("---: ", dwh_insert)
    execute_batch(cursor_dwh, dwh_insert, df.values.tolist(),page_size=1000)
    #cursor_dwh.executemany( dwh_insert, df.values.tolist() )
    if config.debug :
        print("--->  INSERT time: ", (time.time() - start_time) )   
    if config.show_stg_insert_count :
        chk_count = f'SELECT count(*) from {dwh_table_name}'
        cursor_dwh.execute( chk_count )
        tuple_list = cursor_dwh.fetchall()
        print("--->  Count(*):", tuple_list[0][0])

    if not config.do_not_move_to_archive:
        #move file to archive
        _,fname = os.path.split(src_file_name)
        os.replace(src_file_name, config.archive + '/' + fname)


def dds_load(config : Config,
                description: str, 
                qc: Query_config, 
                stg_table: str,
                dds_table: str,
                id_column: str,
                compare_by_columns: list,
                cursor_dwh_alias = 'dwh'):
    
    cursor_dwh = config.get_cursor(cursor_dwh_alias)
    start_time = time.time()
    print(description, datetime.now().isoformat(sep=' ', timespec='seconds'))

    if config.use_static_sql_only:
        queries = sql_load (config, description)
        insert_sql = sql_get_from_dict (queries, qc.scd_str + '_INSERT', description)
        update_sql = sql_get_from_dict (queries, qc.scd_str + '_UPDATE', description)
        delete_sql = sql_get_from_dict (queries, qc.scd_str + '_DELETE', description)
    else:
        ####################### GENERATE SQL begin
        if config.debug :
            print("--- get columns")
        exclude_cols = [qc.dds_scd1_create_dt, 
                        qc.dds_scd1_update_dt, 
                        qc.dds_scd2_start_dt, 
                        qc.dds_scd2_end_dt, 
                        qc.dds_scd2_deleted_flg 
                       ]
        columns = get_table_columns(cursor_dwh, dds_table, exclude_cols)
        columns_noid = columns.copy()
        
        #INSERT new
        add_dds=[]
        add_stg=[]
        if qc.scd_type == SCD_Type.dim_scd2:
            add_dds = [ qc.dds_scd2_start_dt, 
                        qc.dds_scd2_end_dt,
                        qc.dds_scd2_deleted_flg
            ]
            add_stg = [ "stg." + qc.stg_create_dt +" "+ qc.dds_scd2_start_dt, 
                        "to_date('9999-12-31','YYYY-MM-DD') "+ qc.dds_scd2_end_dt,
                        "'N' "+qc.dds_scd2_deleted_flg
            ]
        elif qc.scd_type == SCD_Type.dim_scd1:
            add_dds = [ qc.dds_scd1_create_dt ]
            #if space in column name then it is evaluation column, do not add "stg."
            if " " not in qc.stg_create_dt:
                add_stg = [ "stg." + qc.stg_create_dt +" "+ qc.dds_scd1_create_dt ]
            else:
                add_stg = [ qc.stg_create_dt ]

        columns_dds = ",\n\t".join( columns + add_dds)
        #if space in column name then it is evaluation column, do not add "stg."
        columns_stg = ",\n\t".join( [(s if " " in s else "stg."+s) for s in (columns + add_stg)] ) 

        on_clause_cols = [f"stg.{col} = tgt.{col}" for col in compare_by_columns ]
        on_clause = "\n\tand ".join( on_clause_cols )
        
        #?? to del?
        if qc.insert_columns_dds_add != '':
            columns_dds += f",\n\t{qc.insert_columns_dds_add.lstrip(',')}"
        # if qc.insert_columns_stg_add != '':
            columns_stg += f",\n\t{qc.insert_columns_stg_add.lstrip(',')}"

        insert_sql = (
            f"INSERT INTO {dds_table} (\n  {columns_dds})\n"
            f"  SELECT {columns_stg} \n  FROM {stg_table} stg\n"
            f"      LEFT JOIN {dds_table} tgt\n"
            f"          on 1 = 1\n"
            f"          and {on_clause}\n"
            f"  WHERE tgt.{id_column} is null"
        )
        

        #UPDATE
        if qc.scd_type != SCD_Type.dim_scd2:
            columns_noid.remove(id_column)
            tmpl = "stg.{col} <> tgt.{col} or (stg.{col} is null and tgt.{col} is not null) or (stg.{col} is not null and tgt.{col} is null)"
            inner_where_clause = "\n\t\t or ".join( [tmpl.format(col=col) for col in columns_noid] )
            dds_set = ",\n\t".join( [f"{col} = tmp.{col}" for col in columns_noid] )
            #on_clause is the same
            stg_select = ",\n\t\t".join( [f"stg.{col}" for col in columns] )

            if qc.update_dds_set_add != '':
                dds_set += f",\n\t{qc.update_dds_set_add.lstrip(',')}"
                stg_select += f",\n\t{qc.update_stg_select_add.lstrip(',')}"

            update_sql = (
                f"UPDATE {dds_table} tgt\n"
                f"SET {dds_set}\n"
                f"FROM (\n"
                f"  SELECT {stg_select}\n"
                f"  FROM {stg_table} stg\n"
                f"    INNER JOIN {dds_table} tgt\n"
                f"          on 1=1\n"
                f"          and {on_clause}\n"
                f"  WHERE (1=0\n"
                f"         or {inner_where_clause}\n"
                f"        )\n"
                f") tmp\n"
                f"WHERE tgt.{id_column} = tmp.{id_column}"
            )
        if qc.scd_type == SCD_Type.dim_scd2 :
            columns_noid.remove(id_column)
            tmpl = "stg.{col} <> tgt.{col} or (stg.{col} is null and tgt.{col} is not null) or (stg.{col} is not null and tgt.{col} is null)"
            inner_where_clause = "\n\t\t or ".join( [tmpl.format(col=col) for col in columns_noid] )
            tmp_where_clause = inner_where_clause.replace("stg.","tmp.")
            #on_clause      - from insert
            #columns_stg    - from insert

            add_and = f"tgt.{qc.dds_scd2_end_dt} = to_date('9999-12-31','YYYY-MM-DD')"
            #on_clause_and = on_clause + add_and
            add_dds = [ qc.dds_scd2_start_dt, 
                        qc.dds_scd2_end_dt,
                        qc.dds_scd2_deleted_flg
            ]
            add_stg = [ "stg." + qc.stg_update_dt +" "+ qc.dds_scd2_start_dt, 
                        "to_date('9999-12-31','YYYY-MM-DD') "+ qc.dds_scd2_end_dt,
                        "'N' "+qc.dds_scd2_deleted_flg
            ]
            columns_dds = ",\n\t".join( columns + add_dds)
            #if space in column name then it is evaluation column, do not add "stg."
            columns_stg = ",\n\t".join( [(s if " " in s else "stg."+s) for s in (columns + add_stg)] ) 
            columns_stg_upd = ",\n\t".join( [(s if " " in s else "stg."+s) for s in (columns + [qc.stg_update_dt])] ) 


            update_step_scd2_insert_sql = (
                f"INSERT INTO {dds_table} (\n  {columns_dds})\n"
                f"  SELECT {columns_stg} \n  FROM {stg_table} stg\n"
                f"      INNER JOIN {dds_table} tgt\n"
                f"          on 1 = 1\n"
                f"          and {on_clause}\n"
                f"          and {add_and}\n"
                f"  WHERE ({inner_where_clause})\n"
                f"        or tgt.{qc.dds_scd2_deleted_flg} = 'Y'\n"
            )
            update_step_scd2_update_sql = (
                f"UPDATE {dds_table} tgt\n"
                f"  SET {qc.dds_scd2_end_dt} = tmp.{qc.stg_update_dt} - interval '1 second'\n"
                f"FROM (\n"
                f"  SELECT {columns_stg_upd}\n"
                f"  FROM {stg_table} stg\n"
                f"    INNER JOIN {dds_table} tgt\n"
                f"          on 1=1\n"
                f"          and {on_clause}\n"
                f"          and {add_and}\n"
                f"  WHERE (1=0\n"
                f"         or {inner_where_clause}\n"
                f"        )\n"
                f"        or tgt.{qc.dds_scd2_deleted_flg} = 'Y'\n"
                f") tmp\n"
                f"WHERE 1=1\n"
                f"      and tgt.{id_column} = tmp.{id_column}\n"
                f"      and {add_and}\n"
                f"      and ({tmp_where_clause}\n"
                f"           or tgt.{qc.dds_scd2_deleted_flg} = 'Y')\n"
            )
            update_sql = update_step_scd2_insert_sql + ";\n" + update_step_scd2_update_sql


        #DELETE
        if qc.scd_type != SCD_Type.dim_scd2:
            delete_sql = (
                f"DELETE from {dds_table} tgt\n"
                f"WHERE {id_column} in (\n"
                f"  SELECT stg.{id_column} FROM {dds_table} tgt\n"
                f"      LEFT JOIN {stg_table} stg\n"
                f"          on 1 = 1\n"
                f"          and {on_clause}\n"
                f"  WHERE stg.{id_column} is null\n"
                f")"
            )
        else: 
            add_tgt = [ qc.dds_scd2_delete_end_dt_value + " " + qc.dds_scd2_start_dt, 
                        "to_date('9999-12-31','YYYY-MM-DD') "+ qc.dds_scd2_end_dt,
                        "'Y' "+qc.dds_scd2_deleted_flg
            ]
            columns_tgt = ",\n\t".join( [(s if " " in s else "tgt."+s) for s in (columns + add_tgt)] ) 

            delete_step_scd2_insert_sql = (
                f"INSERT INTO {dds_table} (\n  {columns_dds})\n"
                f"  SELECT {columns_tgt} \n  FROM {dds_table} tgt\n"
                f"      LEFT JOIN {stg_table} stg\n"
                f"          on 1 = 1\n"
                f"          and {on_clause}\n"
                f"  WHERE stg.{id_column} is null\n"
                f"        and tgt.{qc.dds_scd2_end_dt} = to_date('9999-12-31','YYYY-MM-DD')\n"
                f"        and tgt.{qc.dds_scd2_deleted_flg} = 'N'\n"
            ) 
            delete_step_scd2_update_sql = (
                f"UPDATE {dds_table} tgt\n"
                f"  SET {qc.dds_scd2_end_dt} = {qc.dds_scd2_delete_end_dt_value} - interval '1 second'\n"
                f"WHERE tgt.{id_column} in (\n"
                f"    SELECT tgt.{id_column}\n"
                f"    FROM {dds_table} tgt\n"
                f"    LEFT JOIN {stg_table} stg\n"
                f"          on 1=1\n"
                f"          and {on_clause}\n"
                f"    WHERE stg.{id_column} is null\n"
                f"        and tgt.{qc.dds_scd2_end_dt} = to_date('9999-12-31','YYYY-MM-DD')\n"
                f"        and tgt.{qc.dds_scd2_deleted_flg} = 'N'\n"
                f"  ) \n"
                f"  and tgt.{qc.dds_scd2_end_dt} = to_date('9999-12-31','YYYY-MM-DD')\n"
                f"  and tgt.{qc.dds_scd2_deleted_flg} = 'N'\n"
            )
            delete_sql = delete_step_scd2_insert_sql + ";\n" + delete_step_scd2_update_sql

        
        
        
        if config.generate_sql:
            sql_save(config, description, qc.scd_str + '_INSERT', insert_sql)
            sql_save(config, description, qc.scd_str + '_UPDATE', update_sql)
            sql_save(config, description, qc.scd_str + '_DELETE', delete_sql)
        ####################### GENERATE SQL end

    if config.show_sql_and_exit :
        print(insert_sql)
        print()
        print(update_sql)
        print()
        print(delete_sql)
        print()
        return
    
    ####################### EXECUTE SQL
    # INSERT EXECUTION
    print("--- INSERT new into:", dds_table)   
    if config.show_stg_insert_count :
        chk_count = f'SELECT count(*) from {dds_table}'
        cursor_dwh.execute( chk_count )
        tuple_list = cursor_dwh.fetchall()
        print("--->  Count(*):", tuple_list[0][0])
        c1 = tuple_list[0][0]
    if config.debug :
        print("---: ", insert_sql)
    cursor_dwh.execute(insert_sql)

    if config.debug :
        print("--->  INSERT time: ", (time.time() - start_time) )   

    if config.show_stg_insert_count :
        chk_count = f'SELECT count(*) from {dds_table}'
        cursor_dwh.execute( chk_count )
        tuple_list = cursor_dwh.fetchall()
        print("--->  Count(*):", tuple_list[0][0])
        c2 = tuple_list[0][0]
        print("--->  inserted:", {c2-c1})

    if qc.scd_type == SCD_Type.fact_scd0: #fact
        return

    # UPDATE EXECUTION
    print("--- UPDATE :", dds_table)   
    start_time = time.time()
    
    if config.debug :
        print("---: ", update_sql)
    cursor_dwh.execute(update_sql)
    
    if config.debug :
        print("--->  UPDATE time: ", (time.time() - start_time) )   

    # DELETE EXECUTION
    print("--- DELETE :", dds_table)   
    start_time = time.time()
    
    if config.debug :
        print("---: ", delete_sql)
    cursor_dwh.execute(delete_sql)
    
    if config.debug :
        print("--->  DELETE time: ", (time.time() - start_time) )   


