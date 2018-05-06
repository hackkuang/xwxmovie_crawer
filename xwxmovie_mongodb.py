import json
import os
import re
from multiprocessing import Pool

import requests
import urllib3
from bs4 import BeautifulSoup
from pymongo import MongoClient

client = MongoClient()
db = client.xxmovie


def get_total_page():
    url = 'http://xwxmovie.cn/'
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'lxml')
    total = soup.select(
        '#wrap > div.wp-pagenavi > span.pages')[0].text.split('，')[-1]
    return int(re.sub(r'\D', '', total))


def get_posts(page):
    url_index = 'http://xwxmovie.cn/page/{0}'.format(page)
    html = requests.get(url_index).text
    soup = BeautifulSoup(html, 'lxml')
    titles = soup.select('.post h2 a')
    urls = soup.select('.post > div.pinbin-image > a')
    imgs = soup.select('.post > div.pinbin-image > a > img')
    posts = []
    for title, url, img in zip(titles, urls, imgs):
        post = {
            'title': re.sub(r'[\/:*?"<>→]', '', title.text),
            'url_iner': url['href'],
            'img': img['src']
        }
        posts.append(post)
    return posts


def get_down_url(post):
    domain = 'http://xwxmovie.cn/wp-admin/admin-ajax.php'
    headers = {'Referer': post['url_iner']}
    data = {
        'action': 'ic_ajax',
        'code': 'woola'
    }
    r = requests.post(domain, data=data, headers=headers)
    result = r.text
    data = json.loads(result)
    if data and 'list' in data.keys():
        if data.get('list') and type(data.get('list')) == dict:
            for v in data.get('list').values():
                down_url = v.get('url')
                down_note = v.get('note')
                if down_note:
                    down_note = v.get('note').split(': ')[-1]
        else:
            return None, None
    return down_url, down_note


def down_img(post):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    if post.get('img'):
        r = requests.get(post['img'].replace(',', ''), verify=False).content
    else:
        r = None
    img_path = '{0}\\img\\{1}.jpg'.format(os.getcwd(), post.get('title'))
    if r and not os.path.exists(img_path):
        with open(img_path, 'wb') as w:
            w.write(r)


def get_movie_detail(post):
    html = requests.get(post['url_iner']).text
    soup = BeautifulSoup(html, 'lxml')
    infos = soup.select('.post > .pinbin-copy > p')
    if infos:
        infos = infos[0].text
    movdirector = soup.select('.post > .pinbin-copy > p movdirector')
    if movdirector:
        movdirector = soup.select(
            '.post > .pinbin-copy > p movdirector')[0].text
    movwriter = soup.select('.post > .pinbin-copy > p movwriter')
    if movwriter:
        movwriter = movwriter[0].text
    movactor = soup.select('.post > .pinbin-copy > p movactor')
    if movactor:
        movactor = movactor[0].text
    movtype = soup.select('.post > .pinbin-copy > p movtype')
    if movtype:
        movtype = movtype[0].text
    movcountry = soup.select('.post > .pinbin-copy > p movcountry')
    if movcountry:
        movcountry = movcountry[0].text
    movlanguage = soup.select('.post > .pinbin-copy > p movlanguage')
    if movlanguage:
        movlanguage = movlanguage[0].text
    movdate = soup.select('.post > .pinbin-copy > p movdate')
    if movdate:
        movdate = movdate[0].text
    movruntime = soup.select('.post > .pinbin-copy > p movruntime')
    if movruntime:
        movruntime = movruntime[0].text
    movalia = soup.select('.post > .pinbin-copy > p movalias')
    if movalia:
        movalia = soup.select('.post > .pinbin-copy > p movalias')[0].text
    movimdb = soup.select('.post > .pinbin-copy > p movimdb')
    if movimdb:
        movimdb = movimdb[0].text
    movsummary = soup.select('.post > .pinbin-copy > p movsummary')
    if movsummary:
        movsummary = movsummary[0].text
    down_url, down_note = get_down_url(post)
    return {
        'title': post.get('title'),
        'img': post.get('img'),
        'movdirector': movdirector,
        'time': infos,
        'movwriter': movwriter,
        'movactor': movactor,
        'movtype': movtype,
        'movcountry': movcountry,
        'movlanguage': movlanguage,
        'movdate': movdate,
        'movruntime': movruntime,
        'movalias': movalia,
        'movimdb': movimdb,
        'movsummary': movsummary,
        'down_url': down_url,
        'down_note': down_note,
        'movie_url': post['url_iner']
    }


def main(page):
    posts_list = get_posts(page)
    if posts_list:
        for post in posts_list:
            down_img(post)
            movie_info = get_movie_detail(post)
            if db.xwxmovie_20180502.insert(movie_info):
                print('存入MongoDB成功', movie_info.get('movie_url'))


if __name__ == '__main__':
    total_page = get_total_page()
    print('共有' + str(total_page*20) + '部电影')
    pool = Pool()
    pool.map(main, range(1, total_page+1))
    pool.close()
    pool.join()
