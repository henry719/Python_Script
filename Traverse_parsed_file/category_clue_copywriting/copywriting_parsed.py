#!/usr/bin/env python
# -*- coding: utf-8 -*-
#author:zhaochangda
#create_date:2020-11-09
"""
   手动更新category_clue.txt文件内容 然后执行该脚本更新线索文案相关内容
"""
import os
import time
import subprocess

def commend_parse(input_file, output_file):
    input_file = open(input_file, 'r+')
    output_file = open(output_file, 'w')
    subp1 = subprocess.Popen("cat", shell=True, stdin=input_file, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    subp2 = subprocess.Popen("python", shell=True, stdin=subp1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    content = subp2.stdout.read()
    print(content)
    output_file.write(content)
    output_file.close()

def load_data(input_file):
    hdfs = "/opt/hadoop-2.6.0-cdh5.16.1/bin/hdfs dfs -put " + input_file + " hdfs://bj-gmei-hdfs/user/hive/warehouse/offline.db/dim_category_copywriting/"
    print(hdfs)
    subp3 = subprocess.Popen(hdfs, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    cat_hdfs = "/opt/hadoop-2.6.0-cdh5.16.1/bin/hdfs dfs -ls hdfs://bj-gmei-hdfs/user/hive/warehouse/offline.db/dim_category_copywriting/"

    load_commend = "set role admin; load data inpath 'hdfs://bj-gmei-hdfs/user/hive/warehouse/offline.db/dim_category_copywriting/category.txt' into table offline.dim_category_copywriting;"
    beeline = "/opt/hive-1.1.0-cdh5.16.1/bin/beeline -u jdbc:hive2://bj-gm-prod-cos-datacenter006:10000/online -n data -e " + '{1}{0}{1}'.format(load_commend,'"')
    print(beeline)
    subp4 = subprocess.Popen(beeline, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    out = subp4.stdout.read()
    err = subp4.stderr.read()
    print(out, err)



if __name__ == '__main__':
    input_path = '/Users/gm/Documents/demo/Traverse_parsed_file/category_clue_copywriting/category_clue.txt'
    file_name = 'category' + time.strftime("%Y-%m-%d") + '.txt'
    output_path = os.path.join('/Users/gm/Documents/demo/Traverse_parsed_file/category_clue_copywriting/', file_name)
    if not os.path.exists(output_path):
        os.system(r"touch {}".format(output_path))
    commend_parse(input_path, output_path)
    load_data(output_path)

















"""
    content = list(content.split('\n'))
    for line in content:
        if len(line) == 0:
            content.remove(line)
        else:
            code = '{1}{0}{1},'.format(line.split(' ')[0], "'")
            pk = '{1}{0}{1},'.format(line.split(' ')[1], "'")
            home_page_category_id = '{1}{0}{1},'.format(line.split(' ')[2], "'")
            welfare_page_category_id = '{1}{0}{1},'.format(line.split(' ')[3], "'")
            name = '{1}{0}{1},'.format(line.split(' ')[4], "'")
            memo = '{1}{0}{1},'.format(line.split(' ')[5], "'")
            start_day = '{1}{0}{1},'.format(line.split(' ')[6], "'")
            oid = line.split(' ')[7]
            line = 'INSERT INTO TABLE OFFLINE.DIM_CATEGORY_COPYWRITING VALUES(' + \
                   code + pk + home_page_category_id + welfare_page_category_id + name + memo + start_day + oid + \
                   ');'
            #subp3 = subprocess.Popen("hive -e", shell=True, stdin=line, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
    """

