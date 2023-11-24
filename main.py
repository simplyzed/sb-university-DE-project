#!/usr/bin/python3

import psycopg2
from py_scripts import sql_utils as sql
from py_scripts.config import Config
from datetime import datetime

global etl


def step_1(scd1_or_scd2_type: sql.SCD_Type):
    print ("1.1 wipe stage tables")
    etl.get_cursor('dwh').execute('DELETE FROM deaise.zale_stg_accounts')
    etl.get_cursor('dwh').execute('DELETE FROM deaise.zale_stg_cards')
    etl.get_cursor('dwh').execute('DELETE FROM deaise.zale_stg_clients')
    

    print ("1.2 fill stage tables from src")
    sql.simple_src_to_dwh(etl, '1.2.1 stage accounts',
                            'info.accounts', 
                            'deaise.zale_stg_accounts',
                            )
    sql.simple_src_to_dwh(etl, '1.2.2 stage cards',
                            'info.cards', 
                            'deaise.zale_stg_cards',
                            )
    sql.simple_src_to_dwh(etl, '1.2.3 stage clients',
                            'info.clients', 
                            'deaise.zale_stg_clients',
                            )
    sql.dwh_meta_upd(etl, 'src_db', datetime.now() )


    print ("1.3 fill stage tables from files")
    #! for gen mode - there should files
    dwh_to_file_cols = { 'trans_id': 'transaction_id',
                        'trans_date': 'transaction_date',
                        'card_num': 'card_num',
                        'oper_type': 'oper_type',
                        'amt': 'amount',
                        'oper_result': 'oper_result',
                        'terminal': 'terminal'
                        }
    read_file_params = {'decimal':','}
    for f in etl.get_files('transactions*.txt'):
        d = f.replace('.txt','')[-8:]
        d1 = datetime.strptime(d, '%d%m%Y')
        etl.get_cursor('dwh').execute('DELETE FROM deaise.zale_stg_transactions')
        sql.file_to_dwh (etl, '1.3.1 stage transactions files',
                        f,'csv', dwh_to_file_cols,
                        'deaise.zale_stg_transactions',
                        read_file_params
                        )
        sql.dwh_meta_upd(etl, 'file_transactions', d1 )
        query_params = sql.Query_config(sql.SCD_Type.fact_scd0)
        sql.dds_load(etl,'2.1 Transactions', query_params,
                    'deaise.zale_stg_transactions',
                    'deaise.zale_dwh_fact_transactions',
                    'trans_id',
                    ['trans_id']
                )


    dwh_to_file_cols = {'terminal_id': 'terminal_id',
                        'terminal_type': 'terminal_type',
                        'terminal_city': 'terminal_city',
                        'terminal_address': 'terminal_address',
                        'file_dt': 'file_dt'
                        }
    read_file_params = {'sheet_name':'terminals'}
    for f in  etl.get_files('terminals*.xlsx'):
        etl.get_cursor('dwh').execute('DELETE FROM deaise.zale_stg_terminals')
        d = f.replace('.xlsx','')[-8:]
        d1 = datetime.strptime(d, '%d%m%Y')
        dt = d1.date()
        sql.file_to_dwh(etl, '1.3.2 stage terminals files',
                        f,'excel', dwh_to_file_cols,
                        'deaise.zale_stg_terminals',
                        read_file_params,
                        add_col='file_dt', add_col_value=dt
                        )
        sql.dwh_meta_upd(etl, 'file_terminals', d1 )
        query_params = sql.Query_config(scd1_or_scd2_type)
        query_params.stg_create_dt = 'file_dt'
        query_params.stg_update_dt = 'file_dt'
        suffix = "" if scd1_or_scd2_type == sql.SCD_Type.dim_scd1 else "_hist"
        sql.dds_load(etl,'2.2 Terminals', query_params,
                    'deaise.zale_stg_terminals',
                    'deaise.zale_dwh_dim_terminals'+suffix,
                    'terminal_id',
                    ['terminal_id']
                    # insert_columns_stg_add='stg.file_dt',
                    # update_stg_select_add ='stg.file_dt as update_dt'
                )


    dwh_to_file_cols = {'passport_num': 'passport',
                        'entry_dt': 'date'
                        }
    read_file_params = {'sheet_name':'blacklist'}
    for f in etl.get_files('passport_blacklist*.xlsx'):
        etl.get_cursor('dwh').execute('DELETE FROM deaise.zale_stg_passport_blacklist')
        d = f.replace('.xlsx','')[-8:]
        d1 = datetime.strptime(d, '%d%m%Y')
        sql.file_to_dwh(etl, '1.3.3 stage passport_blacklist files',
                        f,'excel', dwh_to_file_cols,
                        'deaise.zale_stg_passport_blacklist',
                        read_file_params
                        )
        sql.dwh_meta_upd(etl, 'file_passport_blacklist', d1 )
        query_params = sql.Query_config(sql.SCD_Type.fact_scd0)
        sql.dds_load(etl,'2.3 Passport blacklist', query_params,
                    'deaise.zale_stg_passport_blacklist',
                    'deaise.zale_dwh_fact_passport_blacklist',
                    'passport_num',
                    ['passport_num']
                )
        
#
#
##########################
#
#
def step_2(scd1_or_scd2_type: sql.SCD_Type):
    query_params = sql.Query_config(scd1_or_scd2_type)
    suffix = "" if scd1_or_scd2_type == sql.SCD_Type.dim_scd1 else "_hist"
    sql.dds_load(etl,'2.4 Accounts', query_params,
                'deaise.zale_stg_accounts',
                'deaise.zale_dwh_dim_accounts' + suffix,
                'account',
                ['account']
            )
    sql.dds_load(etl,'2.5 Cards', query_params,
                'deaise.zale_stg_cards',
                'deaise.zale_dwh_dim_cards' + suffix,
                'card_num',
                ['card_num']
            )
    sql.dds_load(etl,'2.6 Clients', query_params,
                'deaise.zale_stg_clients',
                'deaise.zale_dwh_dim_clients' + suffix,
                'client_id',
                ['client_id']
            )

def step_3(scd1_or_scd2_type: sql.SCD_Type):
    print ("3.1 wipe report table")
    etl.get_cursor('dwh').execute('DELETE FROM deaise.zale_rep_fraud')
    print ("3.2 fill report table")
    suffix = "" if scd1_or_scd2_type == sql.SCD_Type.dim_scd1 else "_hist"
    sql.sql_file_exec(etl, 'event1' + suffix, 'dwh')
    sql.sql_file_exec(etl, 'event2' + suffix, 'dwh')
    sql.sql_file_exec(etl, 'event3' + suffix, 'dwh')
    sql.sql_file_exec(etl, 'event4' + suffix, 'dwh')
    

#
#
########################## main #######################################
#
#

print('0. Init config and connections')
etl = Config()

conn_src = psycopg2.connect(**etl.get_cred('src'))
conn_dwh = psycopg2.connect(**etl.get_cred('dwh'))

conn_src.autocommit = False
conn_dwh.autocommit = False

etl.add_cursor( 'src',conn_src.cursor() )
etl.add_cursor( 'dwh',conn_dwh.cursor() )

print(datetime.now().isoformat(sep=' ', timespec='seconds'))
print ("1 stage layer")
#step_1(sql.SCD_Type.dim_scd1)
step_1(sql.SCD_Type.dim_scd2)

print ("2 DDS layer")
#step_2(sql.SCD_Type.dim_scd1)
step_2(sql.SCD_Type.dim_scd2)

print ("3 Report events")
#step_2(sql.SCD_Type.dim_scd1)
step_3(sql.SCD_Type.dim_scd2)

#SELECT * FROM deaise.zale_rep_fraud

print(datetime.now().isoformat(sep=' ', timespec='seconds'))
print('98. Commit')
conn_dwh.commit()
print('99. Close connections')
etl.get_cursor('src').close()
etl.get_cursor('dwh').close()
conn_src.close()
conn_dwh.close()
print(datetime.now().isoformat(sep=' ', timespec='seconds'))
