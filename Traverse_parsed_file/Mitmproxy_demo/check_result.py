#!/usr/bin/env python
# -*- coding: utf-8 -*-
#author:zhaochangda

from login_wiki import Parser_html
import pandas as pd
import subprocess
import os
import json
import time



def truncate_mitmproxy_file():
    """每次抓包重新写入文件"""
    mitmproxy = os.path.join(os.getcwd(), 'mitmproxy.txt')
    with open(mitmproxy, 'r+') as f:
        f.truncate()
        f.close()
    print("清空mitmproxy抓包txt文件 %s" % (time.ctime(time.time())))

def mitmproxy_parse():
    """执行抓包过程"""
    print("开始抓包 %s" % (time.ctime(time.time())))
    child = subprocess.Popen("mitmdump -s /Users/gm/Documents/demo/Project2/Mitmproxy_demo/mitmproxy_info.py", shell=True, stdout=subprocess.PIPE)
    print(child.stdout.read())
    print("current MimtProxy system process is " + str(child.pid))

def compare(wiki_dict):
    wiki_action_name = []
    #取出埋点文档和mitmproxy埋点相匹配的action列表
    mitmproxy_list = []
    #没有抓到的埋点列表
    non_existent_action = []
    existent_action = []
    #缺少参数的埋点
    non_existent_key_action = []
    #参数不匹配的埋点
    non_matched_key = []
    #类型错误
    type_true = []
    type_error_f = []

    for value in wiki_dict.values():
        for action in value.keys():
            wiki_action_name.append(action)

    wiki_action_name = list(set(wiki_action_name))
    print('-----------------------------------------------------')
    with open(os.path.join(os.getcwd(), 'mitmproxy.txt'), 'r') as f:
        for line in f.readlines():
            params = json.loads(line)
            for i in list(params['params'].keys()):
                if i == 'referrer_link':
                    del params['params']['referrer_link']
            for i in wiki_action_name:
                if i == params['action']:
                    mitmproxy_list.append(params)
    #重复上报埋点去重
    for i in range(0, len(mitmproxy_list) - 1):
        for j in range(len(mitmproxy_list), i + 1, -1):
            if mitmproxy_list[i]['action'] == mitmproxy_list[j-1]['action'] and mitmproxy_list[i]['create_at_millis'] == mitmproxy_list[j-1]['create_at_millis']:
                mitmproxy_list.pop(i)
    for i in mitmproxy_list:
        print(i)

    for k, v in wiki_dict.items():
        print(k, v)

    for (key, values) in wiki_dict.items():
        for key_param, values_param in values.items():
            for line in mitmproxy_list:
                if key_param == line['action']:
                    #文档中存在但在抓包中没有的params
                    non_existent_key = values_param.keys() - dict(line['params']).keys()
                    wiki = values_param
                    mitm = line['params']
                    diff_vals = [(k, wiki[k], mitm[k]) for k in wiki.keys() & mitm if wiki[k] != mitm[k]]
                    if key_param == 'on_click_card':
                        if wiki['page_name'] == mitm['page_name'] and wiki['card_type'] == mitm['card_type']:
                            existent_action.append((key, key_param))
                            for key_name in [key for key, values in wiki.items() if len(values) > 0]:
                                try:
                                    if wiki[key_name] == mitm[key_name]:
                                        type_true.append((key, key_param, key_name, line['create_at_millis']))
                                    else:
                                        type_error_f.append((key, key_param, key_name, line['create_at_millis'], wiki[key_name], mitm[key_name]))
                                except:
                                    non_existent_key_action.append(' '.join([key, key_param, ','.join(non_existent_key)]))
                            if len(wiki.keys() - mitm.keys()) > 0:
                                non_matched_key.append((key, key_param, diff_vals))
                                non_existent_key_action.append(' '.join([key, key_param, ','.join(non_existent_key)]))
                        else:
                            non_existent_action.append((key, key_param))

                    elif key_param == 'page_view':
                        if wiki['page_name'] == mitm['page_name']:
                            existent_action.append((key, key_param))
                            for key_name in [key for key, values in wiki.items() if len(values) > 0]:
                                try:
                                    if wiki[key_name] == mitm[key_name]:
                                        type_true.append((key, key_param, key_name, line['create_at_millis']))
                                    else:
                                        type_error_f.append((key, key_param, key_name, line['create_at_millis'], wiki[key_name], mitm[key_name]))
                                except:
                                    non_existent_key_action.append(' '.join([key, key_param, ','.join(non_existent_key)]))
                        else:
                            non_existent_action.append((key, key_param))
                    elif key_param == 'popup_view':
                        if wiki['popup_name'] == mitm['popup_name']:
                            existent_action.append((key, key_param))
                            for key_name in [key for key, values in wiki.items() if len(values) > 0]:
                                try:
                                    if wiki[key_name] == mitm[key_name]:
                                        type_true.append((key, key_param, key_name, line['create_at_millis']))
                                    else:
                                        type_error_f.append((key, key_param, key_name, line['create_at_millis'], wiki[key_name], mitm[key_name]))
                                except:
                                    non_existent_key_action.append(' '.join([key, key_param, ','.join(non_existent_key)]))
                            if len(wiki.keys() - mitm.keys()) > 0:
                                non_matched_key.append((key, key_param, diff_vals))
                                non_existent_key_action.append(' '.join([key, key_param, ','.join(non_existent_key)]))
                        else:
                            non_existent_action.append((key, key_param))
                    elif key_param == 'on_click_button':
                        if wiki['button_name'] == mitm['button_name']:
                            existent_action.append((key, key_param))
                            for key_name in [key for key, values in wiki.items() if len(values) > 0]:
                                try:
                                    if wiki[key_name] == mitm[key_name]:
                                        type_true.append((key, key_param, key_name, line['create_at_millis']))
                                    else:
                                        type_error_f.append((key, key_param, key_name, line['create_at_millis'], wiki[key_name], mitm[key_name]))
                                except:
                                    non_existent_key_action.append(' '.join([key, key_param, ','.join(non_existent_key)]))
                            for key_name in [key for key, values in wiki.items() if len(values) == 0]:
                                try:
                                    if wiki[key_name] == mitm[key_name]:
                                        type_true.append((key, key_param, key_name, line['create_at_millis']))
                                    else:
                                        type_error_f.append((key, key_param, key_name, line['create_at_millis'], wiki[key_name], mitm[key_name]))
                                except:
                                    non_existent_key_action.append(' '.join([key, key_param, ','.join(non_existent_key)]))
                        else:
                            non_existent_action.append((key, key_param))
                    elif key_param == 'is_open_push':
                        existent_action.append((key, key_param))
                    else:
                        non_existent_action.append((key, key_param))
                else:
                    non_existent_action.append((key, key_param))

    print('---------------------参数值错误----------------------')
    for i in type_true:
        for j in type_error_f:
            if i[0] == j[0] and i[1] == j[1] and i[2] == j[2] and i[3] == j[3]:
                type_error_f.remove(j)
    type_error_f = list(set(type_error_f))
    dff = pd.DataFrame(type_error_f, columns=['name', 'action', 'type', 'time', 'wiki_p', 'mitm_p'])

    df = pd.DataFrame(type_true, columns=['name', 'action', 'type', 'time'])
    df_count = df.groupby(['name', 'action', 'time'], as_index=False)['time'].agg({'count': 'count'})
    df_count_max = df_count[['name', 'action', 'count']].groupby(by=['name', 'action'], as_index=False).max()
    df_merge = pd.merge(df_count_max, df_count, on=['name', 'action', 'count'], how='left')
    df_merge_final = pd.merge(df_merge, dff, on=['name', 'action', 'time'], how='left').dropna(subset=['type'])

    for i in df_merge_final.values.tolist():
        #print(i)
        del i[2:4]
        print(i)
    print('-----------------------缺少参数-----------------------')
    non_existent_key_action = list(set(non_existent_key_action))
    for i in non_existent_key_action:
        print(i)
    print('------------------------缺少埋点----------------------')
    non_existent_action = list(set(non_existent_action) - set(existent_action))
    print(non_existent_action)




if __name__ == '__main__':

    #truncate_mitmproxy_file()
    #mitmproxy_parse()

  
    parser_wiki = Parser_html('http://wiki.wanmeizhensuo.com/pages/viewpage.action?pageId=28916420',
                              'http://wiki.wanmeizhensuo.com/pages/viewpage.action?pageId=43552629',
                              'zhaochangda',
                              'Gengmei1'
                              )
    wiki = parser_wiki.wiki_html_parse()
    compare(wiki)
