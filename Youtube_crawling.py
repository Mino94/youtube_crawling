## Airflow 관련 모듈 ##
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
from airflow.models.connection import Connection

## 스크래핑 관련 모듈 ##
import requests
from bs4 import BeautifulSoup
from pprint import pprint
from urllib import parse
import hashlib
import pandas as pd
from pandas import DataFrame
import time
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from pprint import pprint
from pymongo import MongoClient
from collections import Counter

## Connection 관련 모듈 ##
from pymongo import MongoClient
from boto3.session import Session, Config
import psycopg2
import magic
import psycopg2

import uuid
import tempfile
import re
import datetime
from datetime import timedelta
import random
import urllib.request


@dag(schedule_interval=None, start_date=days_ago(2), tags=["KMH", "KEYWORD", "YOUTUBE"], params={'keyword': ''})
def SC_KEYWORD_YOUTUBE_REPLY_20211019():
    @task()
    def extract():

        # 감정분석 api 변수 저장
        # emo_url = 'http://10.70.201.254:80/model-engine/social_senti_keyword/'
        emo_url = '#'

        # MONGO DB 연결 설정
        db_connections = Connection.get_connection_from_secrets(conn_id="crawling_poc")
        connection = 'mongodb' + Connection.get_uri(db_connections)[5:]
        mongo_conn = MongoClient(connection)

        # schema name
        database = mongo_conn['news']

        # table name
        collections = database['youtube_test']

        # psql
        psql_connections = Connection.get_connection_from_secrets(conn_id="MANAGESNSDATA_KEYWORD_DB")
        temp = Connection.get_uri(psql_connections)
        host = temp.split('@')[1].split(':')[0]
        port = temp.split('@')[1].split(':')[1].split('/')[0]
        user = temp.split('@')[0].split(':')[1].replace('/', '')
        passwd = temp.split('@')[0].split(':')[2].replace("%21", "!")
        db_name = 'airflow'
        db = psycopg2.connect(host=host, port=port, user=user, password=passwd, dbname=db_name)
        sql = f"""SELECT keyword from keyword_status where daum = '1' or daum = '2';"""
        try:
            with db.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                db.close()
                result = {'keyword': [x[0] for x in rows]}
        finally:
            pass

        ## Webdirver option 설정
        options = webdriver.ChromeOptions()
        options.add_argument('headless')  # 크롬 띄우는 창 없애기
        options.add_argument('window-size=1920x1080')  # 크롬드라이버 창크기
        options.add_argument("disable-gpu")  # 그래픽 성능 낮춰서 크롤링 성능 쪼금 높이기
        options.add_argument(
            "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36")  # 네트워크 설정
        options.add_argument("lang=ko_KR")  # 사이트 주언어

        driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
        today = datetime.datetime.now()

        keyword = "오징어게임"

        url = f"https://www.youtube.com/results?search_query={keyword}"
        base_url = "https://www.youtube.com"

        driver.get(url)
        time.sleep(0.1)

        driver.find_element_by_css_selector("#container > ytd-toggle-button-renderer > a").click()
        time.sleep(2)
        driver.find_element_by_css_selector(
            "#collapse-content > ytd-search-filter-group-renderer:nth-child(1) > ytd-search-filter-renderer:nth-child(4)> #endpoint").click()
        time.sleep(1)

        # 스크롤
        no_of_pagedowns = 20
        elem = driver.find_element_by_tag_name("body")
        while no_of_pagedowns:
            elem.send_keys(Keys.PAGE_DOWN)
            time.sleep(0.5)
            no_of_pagedowns -= 1
        driver.implicitly_wait(20)
        num = 0

        soup = BeautifulSoup(driver.page_source, "html.parser")
        box_list = soup.select('div#contents a#video-title')

        # if box_list == None:
        #     print("동영상이 없습니다.")
        #     continue

        duration = soup.find_all('span',
                                 {'id': 'text', 'class': 'style-scope ytd-thumbnail-overlay-time-status-renderer'})

        for box, d in zip(box_list, duration):
            daily_emo = []

            print("각 사이트 접속")
            link = parse.urljoin(url, box.get('href'))
            title = box.text.replace('\n', '')
            print(link)
            # 각 동영상 url 접속
            driver.get(link)
            time.sleep(5)
            post_html = driver.page_source
            insoup = BeautifulSoup(post_html, "html.parser")
            thumbUrl = insoup.find('link', {'itemprop': "thumbnailUrl"}).attrs['href']

            name = insoup.find('link', {'itemprop': "name"}).attrs['content']
            likes_num = insoup.find('yt-formatted-string',
                                    {'id': 'text', 'class': 'style-scope ytd-toggle-button-renderer style-text',
                                     'aria-label': re.compile('좋아요')}).attrs['aria-label'].split(" ")[1][:-1].replace(
                ',',
                '')
            unlikes_num = insoup.find('yt-formatted-string',
                                      {'id': 'text', 'class': 'style-scope ytd-toggle-button-renderer style-text',
                                       'aria-label': re.compile('싫어요')}).attrs['aria-label'].split(" ")[1][:-1].replace(
                ',', '')
            # 조회수
            view_num = insoup.find('meta', {'itemprop': "interactionCount"}).attrs['content']
            # 본문
            body = insoup.find('meta', {'itemprop': "description"}).attrs['content'].replace('\n', '').replace('\r', '')
            # 게시일
            datePublished = insoup.find('meta', {'itemprop': "datePublished"}).attrs['content']
            tmp_date = time.mktime(datetime.datetime.strptime(datePublished, '%Y-%m-%d').timetuple())
            date = datetime.datetime.fromtimestamp(tmp_date)
            date_hour = date.strftime('%Y.%m.%d %H')

            genre = insoup.find('meta', {'itemprop': "genre"}).attrs['content']

            video_len = d.text.replace('\n', '').strip()

            """
            구독자 형식 예시
                백명  435명
                천명  1.3천명
                만명  2.3만명
                10만 10.3만명
                백만 200만명
            """
            try:
                subscriber_count = \
                    insoup.find('yt-formatted-string', {'id': "owner-sub-count"}).attrs['aria-label'].split(' ')[
                        1]
                print(subscriber_count)
                if subscriber_count[-2] == "만":
                    subs_count = int(float(subscriber_count[:-2]) * 10000)
                elif subscriber_count[-1] == "만":
                    subs_count = int(float(subscriber_count[:-1]) * 10000)
                elif subscriber_count[-2] == "천":
                    subs_count = int(float(subscriber_count[:-2]) * 1000)
                elif subscriber_count[-1] == "천":
                    subs_count = int(float(subscriber_count[:-1]) * 1000)
                else:
                    subs_count = int(subscriber_count[:3])
            except Exception as e:
                print("구독자수 모름")
                subs_count = 0

            print(subs_count)

            # md5 작성자, 동영상 시간, 게시일
            temp_id = str(name + link)
            temp_id = temp_id.encode('utf-8')
            h = hashlib.new('md5')
            h.update(temp_id)
            _id = h.hexdigest()

            # 감정분석 추가
            try:
                emo_response = requests.post(emo_url, json={'text': [body]}, timeout=5)
                emotions = emo_response.json()
                source_emo = emotions['pred_labels'][0]
                key_rk = emotions['keyword'].get("Noun_keywords")[0]
                keyword_rk = dict(Counter(key_rk))
                key_ak = emotions['keyword'].get("Not_Noun_keywords")[0]
                keyword_ak = dict(Counter(key_ak))
                daily_emo.append(emotions)
            except:
                source_emo = None
                keyword_rk = {}
                keyword_ak = {}

            data = {
                "id": _id,
                "keyword": keyword,
                "link": link,
                "title": title,
                "image": thumbUrl,
                "videolength": video_len,
                "name": name,
                "subscribercount": subs_count,
                "body": body,
                "date": date,
                "dt_hour": date_hour,
                "count_ak": keyword_ak,
                "count_rk": keyword_rk,
                "emotions": source_emo,
                "viewcount": int(view_num),
                "likecount": int(likes_num),
                "unlikescount": int(unlikes_num),
                "genre": genre,
                "portal": "youtube"
            }

            print(data)

            try:
                collections.insert_one(data)
                print("insert 완료!!!!!!!!!!!!!!!!")
            except Exception as e:
                print(e)
                continue
            print("-" * 20)
            # 썸네일 이미지 S3저장
            print("image 저장")
            session = Session(aws_access_key_id=ECS_ACCESS_KEY, aws_secret_access_key=ECS_SECRET_KEY,
                              region_name=ECS_REGION_OP)
            s3 = session.resource('s3', endpoint_url=ECS_ENDPOINT_OP, verify=False,
                                  config=Config(signature_version='s3'))

            with tempfile.NamedTemporaryFile(mode="wb") as temp_upload_image:
                opener = urllib.request.build_opener()
                opener.addheaders = [('User-Agent',
                                      'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36')]
                urllib.request.install_opener(opener)

                try:
                    image_url = thumbUrl
                    urllib.request.urlretrieve(image_url, temp_upload_image.name)
                    file_name = temp_upload_image.name

                    upload_file_name = file_name
                    upload_file_key = str(uuid.uuid4())

                    s3.Bucket(ECS_ACCESS_KEY).upload_file(upload_file_name, upload_file_key,
                                                          ExtraArgs={'ACL': 'public-read',
                                                                     'ContentType': magic.from_file(
                                                                         upload_file_name,
                                                                         mime=True)})
                    public_access_url = 'https://s3.wehago.com/' + ECS_BUCKET_NAME + '/' + upload_file_key  # 외부에서 접근 가능 한 경로 생성
                except Exception as ex:
                    print('error', ex)
                    public_access_url = 'https://s3.wehago.com/svc_managesns/49e0c3f6-6e9e-43ab-8c75-fe1bab48e779'
            print("image 저장 완료")

        # psql 수집 후 업데이트
        target = "youtube"
        status = 1
        psql_connections = Connection.get_connection_from_secrets(conn_id="MANAGESNSDATA_KEYWORD_DB")
        temp = Connection.get_uri(psql_connections)
        host = temp.split('@')[1].split(':')[0]
        port = temp.split('@')[1].split(':')[1].split('/')[0]
        user = temp.split('@')[0].split(':')[1].replace('/', '')
        passwd = temp.split('@')[0].split(':')[2].replace("%21", "!")
        db_name = 'airflow'
        sql_put = f"""UPDATE keyword_status SET {target} = '{status}' where keyword = '{keyword}';"""
        db = psycopg2.connect(host=host, port=port, user=user, password=passwd, dbname=db_name)

        try:
            with db.cursor() as cursor:
                cursor.execute(sql_put)
                db.commit()
                db.close()

        except Exception as e:
            return e

        driver.close()
        driver.quit()

    extract()


SC_KEYWORD_YOUTUBE_REPLY_20211019 = SC_KEYWORD_YOUTUBE_REPLY_20211019()