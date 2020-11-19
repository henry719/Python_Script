#!/usr/bin/env python
# -*- coding: utf-8 -*-
#author:zhaochangda
#create_date:2020-10-21

import os
import sys
import re
import time
import pymysql
import linecache
reload(sys)
sys.setdefaultencoding('utf-8')

target_directory = '/Users/gm/work/git/warehouse/warehouse-ng/warehouse-ng/tl'
target_table_dir = []
targetTableName = "targetTableName"
zxURL = "zxURL"
gmURL = "gmURL"
sourceTableName = "sourceTableName"
start_date = "start_date"
end_date = "end_date"
userName = "userName"
passWord = "passWord"
author = "作者:"
create_date = "--*更新时间"
dbname = "dbname"
comment = "--*功能:"
columns = "columns"


final_list = []
cluster_list = []
table_list = []
for dir in os.listdir(target_directory):
    parent_dir = os.path.join(target_directory, dir)
    if os.path.isdir(parent_dir):
        keyword_dict = {}
        table_name = parent_dir.split("/")[-1]
        keyword_dict['table_name'] = table_name
        properties = os.path.join(parent_dir + '/workflow/job.properties')
        if os.path.exists(properties):
            if properties == '/Users/gm/work/git/warehouse/warehouse-ng/warehouse-ng/tl/tl_pl_insurance_insuranceorder/workflow/job.properties':
                with open(properties, 'r') as f:
                    for line in f.readlines():
                        if zxURL in line:
                            keyword_dict['database_name'] = line.split("/")[-1].split("?")[0].replace('\n', '')
                            keyword_dict['ip'] = line.split("//")[-1].split(":")[0].replace('\n', '')
                            keyword_dict['port'] = line.split(":")[-1].split("/")[0].replace('\n', '')
                            keyword_dict['name'] = line.split(":")[1].replace('\n', '')
                        if gmURL in line:
                            keyword_dict['database_name'] = line.split("/")[-1].split("?")[0].replace('\n', '')
                            keyword_dict['ip'] = line.split("//")[-1].split(":")[0].replace('\n', '')
                            keyword_dict['port'] = line.split(":")[-1].split("/")[0].replace('\n', '')
                            keyword_dict['name'] = line.split(":")[1].replace('\n', '')
                        if sourceTableName in line:
                            keyword_dict['source_table'] = line.split("=")[-1].replace('\n', '')
                        if start_date in line:
                            keyword_dict['start_time'] = line.split("=")[-1].split("T")[0].replace('\n', '')
                        if end_date in line and columns not in line:
                            keyword_dict['end_time'] = line.split("=")[-1].split("T")[0].replace('\n', '')
                        if userName in line:
                            keyword_dict['user_name'] = line.split("=")[-1].replace('\n', '')
                        if passWord in line:
                            keyword_dict['password'] = line.split("=")[-1].replace('\n', '')
                continue
            with open(properties, 'r') as f:
                for line in f.readlines():
                    if zxURL in line:
                        keyword_dict['database_name'] = line.split("/")[-1].split("?")[0].replace('\n', '')
                        keyword_dict['ip'] = line.split("//")[-1].split(":")[0].replace('\n', '')
                        keyword_dict['port'] = line.split(":")[-1].split("/")[0].replace('\n', '')
                        keyword_dict['name'] = line.split(":")[1].replace('\n', '')
                    if gmURL in line:
                        keyword_dict['database_name'] = line.split("/")[-1].split("?")[0].replace('\n', '')
                        keyword_dict['ip'] = line.split("//")[-1].split(":")[0].replace('\n', '')
                        keyword_dict['port'] = line.split(":")[-1].split("/")[0].replace('\n', '')
                        keyword_dict['name'] = line.split(":")[1].replace('\n', '')
                    if sourceTableName in line:
                        keyword_dict['source_table'] = line.split("=")[-1].replace('\n', '')
                    if start_date in line:
                        keyword_dict['start_time'] = line.split("=")[-1].split("T")[0].replace('\n', '')
                    if end_date in line:
                        keyword_dict['end_time'] = line.split("=")[-1].split("T")[0].replace('\n', '')
                    if userName in line:
                        keyword_dict['user_name'] = line.split("=")[-1].replace('\n', '')
                    if passWord in line:
                        keyword_dict['password'] = line.split("=")[-1].replace('\n', '')

        create = os.path.join(parent_dir + '/etl/create_' + table_name + '.sql')
        if os.path.exists(create):
            with open(create, 'r') as f:
                for line in f.readlines():
                    if author in line:
                        keyword_dict['author'] = line.split(":")[-1].lstrip().replace('\n', '')
                    if create_date in line:
                        keyword_dict['create_time'] = line.split(" ")[1].lstrip().split(" ")[0].replace('\n', '')
                    if comment in line:
                        keyword_dict['comment'] = line[3:].split("(")[-1].split(")")[0].replace('\n', '')
                    #    keyword_dict['comment'] = re.findall(r'[(](.*?)[)]', line, 0)
    final_list.append(keyword_dict)



for i in final_list:
    table_name = i['table_name']
    name = i.setdefault('name', 'hive')
    database_name = i.setdefault('database_name', 'tl')
    source_table = i.setdefault('source_table', '')
    comment = i.setdefault('comment', '')
    ip = i.setdefault('ip', 'bj-gm-prod-cos-datacenter001')
    port = i.setdefault('port', '3306')
    user_name = i.setdefault('user_name', 'hive')
    password = i.setdefault('password', 'null')
    author = i.setdefault('author', '')
    create_time = i.setdefault('create_time', '')
    start_time = i.setdefault('start_time', '')
    end_time = i.setdefault('end_time', '')
    cluster_list.append(name + ' ' + ip + ' ' + port + ' ' + user_name + ' ' + password + ' ' + database_name)
    table_list.append(source_table + ' ' + comment + ' ' + database_name + ' ' + ip + ' ' + user_name + ' ' + port + ' ' + password + ' ' + author + ' ' + create_time + ' ' + start_time + ' ' + end_time+ ' ' + table_name)
    data = (table_name, database_name, comment, source_table, ip, user_name, password, author, create_time, start_time, end_time)
    #    print(data)
 #   cur.execute(insert_sql, data)





print("----校验并更新data_ieport_database----")
#校验postgresql库是否存在,如果不存在则插入
check_postgresql_sql = "select t1.name,t2.address from data_ieport_database t1 left join data_ieport_cluster t2 on t1.cl_id = t2.id where t2.name = 'postgresql';"
insert_database_sql = "insert into data_ieport_database(cl_id,name,is_inuse,create_time,update_time) " \
                      "VALUES (%s,%s,%s,%s,%s);"
check_postgresql_sql1 = "select t1.id,t1.address from data_ieport_cluster t1 where t1.name = 'postgresql';"
cur.execute(check_postgresql_sql)
check_data = cur.fetchall()
cur.execute(check_postgresql_sql1)
check_data1 = cur.fetchall()
for i in cluster_list:
    if i.split(' ')[0] == 'postgresql':
        a = (i.split(' ')[-1], i.split(' ')[1])
        if a in check_data:
            continue;
        else:
            for i in check_data1:
                if a[1] == (i[1]):
                    data = (i[0], i[1], 'true', time.strftime("%Y-%m-%d"), time.strftime("%Y-%m-%d"))
                    #                    print(data)
                    cur.execute(insert_database_sql, data)

