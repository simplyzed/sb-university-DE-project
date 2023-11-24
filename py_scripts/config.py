import os
import fnmatch
import json

class Config:

    def __init__(self):
        self.confdata = []
        with open('config.json') as config_file:
            self.confdata = json.load(config_file)
        self.cursors = {}
        self.data_dir = os.getcwd()+"/data"
        self.sql_dir  = os.getcwd()+"/sql_scripts"
        self.gen_dir  = os.getcwd()+"/sql_scripts/gen"
        self.archive  = os.getcwd()+"/archive"
        if not os.path.exists(self.archive):
            os.mkdir(self.archive)
        if os.path.exists(self.sql_dir):
            self.sql_files = os.listdir(self.sql_dir)
        else:
            self.sql_files = []
        self.files = os.listdir(self.data_dir)
        self.files.sort()

        print("Data files dir:",self.data_dir )
        if self.__debug():
            print("! debug config flag on" )
        if self.__do_not_move_to_archive():
            print("! do_not_move_to_archive config flag on" )
        if self.__show_sql_and_exit():
            print("! show_sql_and_exit config flag on" )
        if self.__show_stg_insert_count():
            print("! show_stg_insert_count config flag on" )
        if self.__generate_sql():
            print("! generate_sql config flag on" )
            if not os.path.exists(self.sql_dir):
                os.mkdir(self.sql_dir)
            if not os.path.exists(self.gen_dir):
                os.mkdir(self.gen_dir)
        if self.__use_static_sql_only():
            print("! use_static_sql_only config flag on" )
            if self.__generate_sql():
                raise Exception("cannot 'use_static_sql_only' config flag with 'generate_sql' config flag")

    
    def get_cred(self,alias) -> dict:
        return self.confdata['cred'][alias]
    
    def get_files(self,file_mask) -> list:
        fl = fnmatch.filter(self.files, file_mask)
        return [ self.data_dir+"/"+f for f in fl ]

    def get_sql_files(self,file_prefix) -> list:
        fl = fnmatch.filter(self.sql_files, file_prefix+"*")
        return [ self.sql_dir+"/"+f for f in fl ]

    def __check_bool(self,name):
        if name in self.confdata:
            #if self.confdata['debug'] is bool:
            if self.confdata[name] == False or self.confdata[name] == True:
                return self.confdata[name]
            else:
                print(f"{name} must bool value")
                print("current: ",self.confdata[name], type(self.confdata[name])) 
        return False

    def __debug(self) -> bool:
        self.debug = self.__check_bool('debug')
        return self.debug

    def __do_not_move_to_archive(self) -> bool:
        self.do_not_move_to_archive = self.__check_bool('do_not_move_to_archive')
        return self.do_not_move_to_archive

    def __show_sql_and_exit(self) -> bool:
        self.show_sql_and_exit = self.__check_bool('show_sql_and_exit')
        return self.show_sql_and_exit
    
    def __show_stg_insert_count(self) -> bool:
        self.show_stg_insert_count =  self.__check_bool('show_stg_insert_count')
        return self.show_stg_insert_count
    
    def __generate_sql(self) -> bool:
        self.generate_sql =  self.__check_bool('generate_sql')
        return self.generate_sql

    def __use_static_sql_only(self) -> bool:
        self.use_static_sql_only =  self.__check_bool('use_static_sql_only')
        return self.use_static_sql_only
    
    def add_cursor(self, alias: str, cursor):
        self.cursors.update ({alias : cursor})

    def get_cursor(self, alias: str):
        return  self.cursors.get(alias)
    




