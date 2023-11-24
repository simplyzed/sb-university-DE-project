drop table if exists deaise.zale_stg_accounts ;
drop table if exists deaise.zale_stg_cards ;
drop table if exists deaise.zale_stg_clients ;
drop table if exists deaise.zale_stg_transactions ;
drop table if exists deaise.zale_stg_terminals ;
drop table if exists deaise.zale_stg_passport_blacklist ;
drop table if exists deaise.zale_dwh_fact_transactions ;
drop table if exists deaise.zale_dwh_fact_passport_blacklist ;
drop table if exists deaise.zale_dwh_dim_terminals ;
drop table if exists deaise.zale_dwh_dim_accounts ;
drop table if exists deaise.zale_dwh_dim_cards ;
drop table if exists deaise.zale_dwh_dim_clients ;
drop table if exists deaise.zale_dwh_dim_terminals_hist ;
drop table if exists deaise.zale_dwh_dim_accounts_hist ;
drop table if exists deaise.zale_dwh_dim_cards_hist ;
drop table if exists deaise.zale_dwh_dim_clients_hist ;
drop table if exists deaise.zale_meta_loadinfo ;
drop table if exists deaise.zale_rep_fraud ;

-------------------- stage

create table deaise.zale_stg_accounts (
	account char(20),
	valid_to date,
	client varchar(10),
	create_dt timestamp(0),
	update_dt timestamp(0) 
);

create table deaise.zale_stg_cards (
	card_num char(20) ,
	account char(20) ,
	create_dt timestamp(0) ,
	update_dt timestamp(0) 
);

create table deaise.zale_stg_clients (
	client_id varchar(10) ,
	last_name varchar(20) ,
	first_name varchar(20) ,
	patronymic varchar(20) ,
	date_of_birth date ,
	passport_num varchar(15) ,
	passport_valid_to date ,
	phone char(16) ,
	create_dt timestamp(0) ,
	update_dt timestamp(0) 
);

create table deaise.zale_stg_transactions (
	trans_id	char(11),
	trans_date	timestamp(0),
	card_num	char(20),
	oper_type	varchar(8),
	amt	decimal(12,2),
	oper_result varchar(7),
	terminal char(5)
);

create table deaise.zale_stg_terminals (
	terminal_id char(5),
	terminal_type char(3),
	terminal_city varchar(30),
	terminal_address varchar(200),
	file_dt date
);

create table deaise.zale_stg_passport_blacklist (
	passport_num varchar(15),
	entry_dt date
);

-------------------- dds

create table deaise.zale_dwh_fact_transactions (
	trans_id	char(11),
	trans_date	timestamp(0),
	card_num	char(20),
	oper_type	varchar(8),
	amt	decimal(12,2),
	oper_result varchar(7),
	terminal char(5)
);

create table deaise.zale_dwh_fact_passport_blacklist (
	passport_num varchar(15),
	entry_dt date
);

----------- DIM tables scd1
create table deaise.zale_dwh_dim_terminals (
	terminal_id char(5),
	terminal_type char(3),
	terminal_city varchar(30),
	terminal_address varchar(200),
	create_dt timestamp(0),
	update_dt timestamp(0) 
);


create table deaise.zale_dwh_dim_accounts (
	account char(20),
	valid_to date,
	client varchar(10),
	create_dt timestamp(0),
	update_dt timestamp(0) 
);

create table deaise.zale_dwh_dim_cards (
	card_num char(20) ,
	account char(20) ,
	create_dt timestamp(0) ,
	update_dt timestamp(0) 
);

create table deaise.zale_dwh_dim_clients (
	client_id varchar(10) ,
	last_name varchar(20) ,
	first_name varchar(20) ,
	patronymic varchar(20) ,
	date_of_birth date ,
	passport_num varchar(15) ,
	passport_valid_to date ,
	phone char(16) ,
	create_dt timestamp(0) ,
	update_dt timestamp(0) 
);



----------- DIM tables scd2

create table deaise.zale_dwh_dim_terminals_hist (
	terminal_id char(5),
	terminal_type char(3),
	terminal_city varchar(30),
	terminal_address varchar(200),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg	char(1)
);


create table deaise.zale_dwh_dim_accounts_hist (
	account char(20),
	valid_to date,
	client varchar(10),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg	char(1)
);

create table deaise.zale_dwh_dim_cards_hist (
	card_num char(20) ,
	account char(20) ,
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg	char(1)
);

create table deaise.zale_dwh_dim_clients_hist (
	client_id varchar(10) ,
	last_name varchar(20) ,
	first_name varchar(20) ,
	patronymic varchar(20) ,
	date_of_birth date ,
	passport_num varchar(15) ,
	passport_valid_to date ,
	phone char(16) ,
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg	char(1)
);

---------- meta

create table deaise.zale_meta_loadinfo (
    src varchar(30),
    max_update_dt timestamp(0)
);
delete from deaise.zale_meta_loadinfo;
insert into deaise.zale_meta_loadinfo( src, max_update_dt )
values('file_terminals', to_timestamp('1900-01-01','YYYY-MM-DD') );
insert into deaise.zale_meta_loadinfo( src, max_update_dt )
values('file_transactions', to_timestamp('1900-01-01','YYYY-MM-DD') );
insert into deaise.zale_meta_loadinfo( src, max_update_dt )
values('file_passport_blacklist', to_timestamp('1900-01-01','YYYY-MM-DD') );
insert into deaise.zale_meta_loadinfo( src, max_update_dt )
values('src_db', to_timestamp('1900-01-01','YYYY-MM-DD') );



-------------------- report

create table deaise.zale_rep_fraud (
	event_dt timestamp(0),
	passport varchar(15),
	fio varchar(60),
	phone char(16),
	event_type	varchar(20),
	report_dt date
);


