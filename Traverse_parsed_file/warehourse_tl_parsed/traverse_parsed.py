#!/usr/bin/env python
# -*- coding: utf-8 -*-
#author:zhaochangda
#create_date:2020-10-28

import sys
import os
import re
import time
import pymysql
import linecache
import subprocess

reload(sys)
sys.setdefaultencoding('utf-8')

target_directory = '/opt/warehouse-ng/tl'
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

os.system("cd /opt/warehouse-ng;git checkout data-exchange;git pull")


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
                        keyword_dict['end_time'] = '2022-12-31'
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

#truncate table warehouse_tl_monitor
truncate_sql = "truncate table warehouse_tl_monitor;"
cur.execute(truncate_sql)
#insert into table warehouse_tl_monitor
insert_sql = "insert into warehouse_tl_monitor(table_name,database_name,table_comment,source_table,ip,user_name,password,author,create_time,start_time,end_time) " \
             "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

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
    cur.execute(insert_sql, data)

print("----校验并更新data_ieport_cluster----")
#insert into table data_ieport_cluster
insert_cluster_sql = "insert into data_ieport_cluster(name,address,port,user,password,permission,project,is_inuse,create_time,update_time) " \
                     "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

comm_cluster = []

select_cluster_sql = "select address,port,user from data_ieport_cluster;"
cur.execute(select_cluster_sql)
data_ieport_cluster = cur.fetchall()

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
            for k in check_data1:
                if a[1] == (k[1]):
                    data = (k[0], a[0], 'true', time.strftime("%Y-%m-%d"), time.strftime("%Y-%m-%d"))
                    print(data)
                    cur.execute(insert_database_sql, data)
conn.commit()

print("----校验并更新data_ieport_history----")
#校验data_table_history并更新数据
check_history_sql = "select t1.table_name,t2.name as database_name,t3.address as ip,t1.target_table_name from data_table_history t1 left join data_ieport_database t2 on t1.db_id = t2.id left join  data_ieport_cluster t3 on t1.cl_id = t3.id"
cur.execute(check_history_sql)
history_list = cur.fetchall()

select_id_sql = "select t1.id as cl_id,t2.id as db_id,t1.address,t1.port,t1.user,t2.name from data_ieport_cluster t1 left join data_ieport_database t2 on t1.id = t2.cl_id;"
cur.execute(select_id_sql)
select_id = cur.fetchall()

insert_history_sql = "insert into data_table_history(cl_id,db_id,table_name,table_comment,is_inuse,is_bit,create_time,update_time,start_time,end_time,first_owner,second_owner,target_table_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"



tmp_history_list = [i[0] for i in history_list]
tmp_table_list = [i.split(' ')[0] for i in table_list if i.split(' ')[3] != '172.16.32.15']
#tmp_table_list = [i.split(' ')[-1] for i in table_list]
diff = [i for i in tmp_table_list if i not in tmp_history_list]
for i in diff:
    for j in table_list:
        if i == j.split(' ')[0]:
            for k in select_id:
                if j.split(' ')[3] == k[2] and j.split(' ')[5] == k[3] and j.split(' ')[4] == k[4] and j.split(' ')[2] == k[5]:
                    data = (k[0], k[1], j.split(' ')[0], j.split(' ')[1], 'true', 'N', j.split(' ')[-4], j.split(' ')[-4], j.split(' ')[-3], j.split(' ')[-2], j.split(' ')[-5], 'unknown', j.split(' ')[-1])
                    print(data)
                    cur.execute(insert_history_sql, data)
conn.commit()

cur.close()
conn.close()