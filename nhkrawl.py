#!/usr/bin/env python3
import json
import sqlite3
import urllib.request

import os
import requests
from time import sleep
from dateutil.parser import parse
from entrypoint2 import entrypoint
from bs4 import BeautifulSoup, Tag


@entrypoint
def main(target_dir):
    sql = sqlite3.connect('nhkrawl.sqlite')
    sql.execute('CREATE TABLE IF NOT EXISTS links(title TEXT, link TEXT UNIQUE);')
    sql.commit()

    n = 1
    hasnext = True
    while hasnext:
        to_crawl = []
        new_url = 'https://www3.nhk.or.jp/news/json16/new_{:03d}.json'.format(n)
        new_json = requests.get(new_url)
        json_data = json.loads(new_json.text)
        if 'channel' not in json_data.keys():
            continue
        if 'hasNext' not in json_data['channel'].keys() or not json_data['channel']['hasNext']:
            hasnext = False
        for item in json_data['channel']['item']:
            to_crawl.append((item['title'], item['link']))
            for rel in item['relationNews']:
                to_crawl.append((rel['title'], rel['link']))

        print('parsed listing {:03d}'.format(n))

        n += 1

        for title, link in to_crawl:
            # skip links we already visited
            c = sql.execute('SELECT * FROM links WHERE link = ?;', (link,))
            if len(c.fetchall()) > 0:
                print('skipping already saved article: {}, {}'.format(title, link))
                continue

            sleep(1)

            url = 'https://www3.nhk.or.jp/news/' + link
            req = urllib.request.urlopen(url, timeout=10)
            html = req.read().decode('utf-8')
            soup = BeautifulSoup(html, 'lxml')
            text = '\n'.join(node.text for node in (
                    soup.findAll('div', {'id': 'news_textbody'}) +
                    soup.findAll('div', {'id': 'news_textmore'})))
            for node in soup.findAll('div', {'class': 'news_add'}):
                text += '\n'.join([ch.text for ch in node.findAll(True, recursive=False)])

            # get publish date
            date = None
            try:
                for time in soup.findAll('time'):
                    if time.parent.find('span', {'class': 'contentTitle'}):
                        date = parse(time.attrs['datetime'])
            except Exception as ex:
                print('failed parsing date: ' + repr(ex))
                pass

            try:
                filename = (date.strftime('%Y-%m-%d') + '_' if date else '') + title + '.txt'
                with open(os.path.join(target_dir, filename), 'w') as file:
                    file.write(title + '\n')
                    file.write(text)
            except Exception as ex:
                print('failed writing file {}: {}'.format(filename, repr(ex)))
                continue

            sql.execute('INSERT INTO links(title, link) VALUES(?,?);', (title, link))
            sql.commit()

            print('successfully saved ' + filename)

