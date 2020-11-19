#!/usr/bin/env python
# -*- coding: utf-8 -*-
#author:zhaochangda
#create_date:2020-10-21

import os
import re
import time
import pymysql
import linecache

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


#链接数据库
conn = pymysql.Connection(host='172.16.30.130',
                          port=3306,
                          user='data',
                          passwd='8lysZM6#m$UiHlf6',
                          db='data_exchange',
                          charset='utf8mb4')
cur = conn.cursor()

#truncate table
#truncate_sql = "truncate table warehouse_tl_monitor;"
#cur.execute(truncate_sql)

#insert into table warehouse_tl_monitor
insert_sql = "insert into warehouse_tl_monitor(table_name,database_name,table_comment,source_table,ip,user_name,password,author,create_time,start_time,end_time) " \
             "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

for i in final_list:
    table_name = i['table_name']
    name = i.setdefault('name', 'warehouse-ng')
    database_name = i.setdefault('database_name', 'tl')
    source_table = i.setdefault('source_table', '')
    comment = i.setdefault('comment', '')
    ip = i.setdefault('ip', 'hdfs://bj-gmei-hdfs/user/hive/warehouse-ng/tl')
    port = i.setdefault('port', 'null')
    user_name = i.setdefault('user_name', 'null')
    password = i.setdefault('password', 'null')
    author = i.setdefault('author', '')
    create_time = i.setdefault('create_time', '')
    start_time = i.setdefault('start_time', '')
    end_time = i.setdefault('end_time', '')
    data = (table_name, database_name, comment, source_table, ip, user_name, password, author, create_time, start_time, end_time)
    #cur.execute(insert_sql, data)
    cluster_list.append(name + ' ' + ip + ' ' + port + ' ' + user_name + ' ' + password)

#insert into table data_ieport_cluster
comm_cluster = []
insert_cluster_sql = "insert into data_ieport_cluster(name,address,port,user,password,permission,project,is_inuse,create_time,update_time) " \
                     "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
select_cluster_sql = "select address,port,user from data_ieport_cluster;"
cur.execute(select_cluster_sql)
data_ieport_cluster = cur.fetchall()
#增量更新插入
cluster_list = list(set(cluster_list))
for i in cluster_list:
    for j in data_ieport_cluster:
        j = j[0] + j[1] + j[2]
        if i.split(' ')[1] + i.split(' ')[2] + i.split(' ')[3] == j:
            comm_cluster.append(i)
for i in list(set(cluster_list) - set(comm_cluster)):
    i = (i.split(' ')[0], i.split(' ')[1], i.split(' ')[2], i.split(' ')[3], i.split(' ')[4], 'r', 'gengmei', 'true', time.strftime("%Y-%m-%d"), time.strftime("%Y-%m-%d"))
    print(i)
    cur.execute(insert_cluster_sql, i)




conn.commit()


select_sql = "select * from data_ieport_cluster;"
cur.execute(select_sql)

data = cur.fetchall()
for line in data:
    print(line)



cur.close()
conn.close()