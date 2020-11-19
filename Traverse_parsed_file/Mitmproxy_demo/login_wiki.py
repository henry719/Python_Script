#!/usr/bin/env python
# -*- coding: utf-8 -*-
#author:zhaochangda

import requests
from bs4 import BeautifulSoup

class Parser_html:

    def __init__(self, target_url, requirement_document_url, username, password):
        self.target_url = target_url
        self.req_doc_url = requirement_document_url
        self.username = username
        self.password = password

    def wiki_html_login(self):
        """通过username password发送post表单模拟登陆获取session_id,然后get目标页面"""

        login_url = 'http://wiki.wanmeizhensuo.com/login.action?logout=true'
        login_data = {
            'os_username': self.username,
            'os_password': self.password,
            'login': '登录',
            'os_destination': ''
        }
        loginreqsession = requests.session()

        logincontent = loginreqsession.post(login_url, login_data).content.decode('utf-8')

        html = loginreqsession.get(self.target_url)

        soup = BeautifulSoup(html.content, "lxml")
        final_html = soup.find(name='div', class_='wiki-content')
        #print(loginreqsession.cookies.get_dict())
        #print(final_html)
        return final_html

    def wiki_html_session(self):
        """通过获取cookie爬取页面 要保持wiki是登录状态 一旦session_id失效需要重新替换"""

        headers = {'User-Agent': "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Mobile Safari/537.36",
                   'Host': 'wiki.wanmeizhensuo.com',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                   'Referer': 'http://wiki.wanmeizhensuo.com/plugins/servlet/mobile?contentId=36545693',
                   'Accept-Encoding': 'gzip, deflate',
                   'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                   'Cookie': 'seraph.confluence=36635364%3A7eadb016970e3f8c4854ab8e19b489ff6619ea60; JSESSIONID=7CC9BF8A7F82BF276F2B2D69D2F695C1; confluence.mobile.desktop.switch=true',
                   'Upgrade-Insecure-Requests': '1',
                   'Connection': 'keep-alive'
                   }
        html = requests.get(self.target_url, headers=headers)
        soup = BeautifulSoup(html.content, "lxml")
        final_html = soup.find(name='div', class_='wiki-content')
        print(final_html)
        return final_html

    def wiki_html_parse(self):
        html = self.wiki_html_login()
        maidian_info_dict = {}
        count = 0
        flag = 0
        for div in html:
            print(div)
            if div.name == 'p' and div.a.get('href') == self.req_doc_url:
                #print(div.a.get('href'))
                flag = count
            count += 1
        count = 0
        for div in html:
            if div.name == 'div':
                if count == flag + 1:
                    for tag in div.contents[0].children:
                        #获取埋点文档参数拼接成字典
                        if tag.name == 'tbody':
                                for i in range(0, len(tag.contents)):
                                    try:
                                        maidian_info_dict[tag.contents[i].find_all('td')[0].string] = \
                                        {tag.contents[i].find_all('td')[1].string:
                                             {params.get_text().split('--')[0].replace('"', '').split(':')[0]:
                                                  params.get_text().split('--')[0].replace('"', '').strip().split(':')[1].replace(',', '')
                                              for params in tag.contents[i].find_all('td')[2].contents}}
                                    except:
                                        continue

            count += 1
        return maidian_info_dict


obj1 = Parser_html('http://wiki.wanmeizhensuo.com/pages/viewpage.action?pageId=28916420',
                   'http://wiki.wanmeizhensuo.com/pages/viewpage.action?pageId=36565773',
                   'zhaochangda',
                   'Gengmei1'
                   )
ss = obj1.wiki_html_login()
aa = obj1.wiki_html_parse()
print(aa)
#print(ss)

"""
http://wiki.wanmeizhensuo.com/pages/viewpage.action?pageId=36557362
http://wiki.wanmeizhensuo.com/pages/viewpage.action?pageId=36555989
http://wiki.wanmeizhensuo.com/pages/viewpage.action?pageId=36565773
http://wiki.wanmeizhensuo.com/pages/viewpage.action?pageId=43552629
"""