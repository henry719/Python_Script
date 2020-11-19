#!/usr/bin/env python
# -*- coding: utf-8 -*-
#author:zhaochangda

import os
import json
from mitmproxy import ctx


def save_to_file(text):
    with open(os.path.join('/Users/gm/Documents/demo/Project2/Mitmproxy_demo', 'mitmproxy.txt'), 'a+') as f:
        f.write(json.dumps(text, ensure_ascii=False) + '\n')
        f.close()

#测试 http://log.test.igengmei.com/log/collect
#线上 https://log.igengmei.com/log/collect

def request(flow):
    redundant_action = ['report_status', 'device_opened', 'is_open_push', 'on_app_session_over', 'main_tabBarView_controller_willAppear']
    if flow.request.url == 'https://log.igengmei.com/log/collect':
        print(len(json.loads(flow.request.text)))
        for params in json.loads(flow.request.text):
            ctx.log.warn(str(params))
            print(params['type'])
            print(params['params'])
            if params['type'] not in redundant_action:
                data = {
                    'action': params['type'],
                    'params': params['params'],
                    'create_at_millis': params['create_at_millis']
                }
                save_to_file(data)