#!usr/bin/env python
#-*- coding:utf-8 _*-  
""" 
@author:ZhangHui
@file: opDBforPostgre.py 
@time: 2017/12/17 
"""
import psycopg2
class DBoperation(object):
    def __init__(self,database,user,password,host,port):
        try:
            self.conObj = psycopg2.connect(database=database,user=user,password=password,host=host,port=port)
            self.con = self.conObj.cursor()
        except psycopg2.OperationalError as e:
            raise Exception('Database connect error',e)
    def exec(self,CMD,data):
        try:
            self.con.execute(CMD,data)
        except psycopg2.OperationalError as e:
            raise Exception('Database connect error', e)
    def commit(self):
        self.conObj.commit()
    def exec_getreturn(self,sqlstr,data):
        try:
            self.con.execute(sqlstr,data)
            rows = self.con.fetchall()
            return rows
        except psycopg2.OperationalError as e:
            raise Exception('Database connect error', e)
    def exec_getreturn2(self,sqlstr):
        try:
            self.con.execute(sqlstr)
            rows = self.con.fetchall()
            return rows
        except psycopg2.OperationalError as e:
            raise Exception('Database connect error', e)
    def exec_commit(self,sqlstr):
        try:
            self.con.execute(sqlstr)
            self.conObj.commit()
        except psycopg2.OperationalError as e:
            raise Exception('Database connect error', e)
    def exec_commit2(self,sqlstr,data):
        try:
            self.con.execute(sqlstr,data)
            self.conObj.commit()
        except psycopg2.OperationalError as e:
            raise Exception('Database connect error', e)
    def tableCreate(self,tableName,*CMD):
        # CMD = '''select 1 from information_schema.tables where table_schema = 'public' and table_name = '{}' '''.format(tableName)
        CMD = '''
        CREATE TABLE "public"."{}" (
            "id" serial PRIMARY KEY, 
            "name" varchar(254),
            "director" varchar(254),
            "actor" varchar(254),
            "type" varchar(254),
            "country" varchar(254),
            "onboardtime" varchar(254),
            "time" varchar(254),
            "score_in" varchar(254),
            "score_db" varchar(254),
            "movie" varchar(254),
            "link" varchar(254)
        );
        '''.format(tableName)
        self.exec_commit(CMD)
    def getColumns(self,tableName):
        CMD = '''
        select * from information_schema.columns
where table_schema='public' and table_name='{}';
        '''.format(tableName)
        self.con.execute(CMD)
        rows = self.con.fetchall()
        return [row[3] for row in rows]
    def insert(self,sqlstr,data):
        self.exec_commit2(sqlstr,data)
        print('insert success')
def getInsertSQL(Detail):
    title = point(list(Detail.keys()),isTitle=True)
    data = point(list(Detail.values()),isTitle=False)
    insertSQL = "INSERT INTO \"public\".\"movies\" ({}) VALUES ({})"
    return insertSQL.format(title,data)
def point(data,isTitle=True):
    result = ''
    for index in range(len(data)):
        if index == 0:
            result = '\''+str(data[index]).replace('\'','')+'\''
        else:
            result = result+','+'\''+str(data[index]).replace('\'','')+'\''
    if isTitle:
        result = result.replace('\'', '')
    return result

