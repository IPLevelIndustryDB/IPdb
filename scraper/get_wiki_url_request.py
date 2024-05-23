import time
import sys
import io
import base64
from urllib.parse import urlparse
from urllib import parse

import requests
import unidecode
import regex
import re
# from selenium.common.exceptions import NoSuchWindowException, WebDriverException
from bs4 import BeautifulSoup
import random
from sql_operation.db_operate import create_connection, update_cluster_buff
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from tldextract import extract
from scraper.scraper_utils import USER_AGENTS, dismiss_keyword, non_english_dismiss, translate_to_english
from params import CHROMEDRIVER_EXE
from params import chrome_path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf8")

mutex = threading.Lock()
# proxy = {"http": "socks5://127.0.0.1:1089", "https": "socks5://127.0.0.1:1089"}


def check(keyword, text, type_="strict"):
    keywords = regex.split(r"[\s,.]+", keyword)
    keywords = [k.lower() for k in keywords]

    if type_ != "strict":
        text = text.lower()
        for k in keywords:
            if k not in text:
                return False
        return True
    else:
        text = unidecode.unidecode(text) + " " + text
        text_list = regex.split(r"[\s,.]+", text)
        text_list = [t.lower() for t in text_list]
        for k in keywords:
            if k not in text_list:
                return False
        return True


def perform_wiki_search(query, keyword=None):
    # search_engine_urls = ["https://www.google.com/search?q="]
    search_engine_urls = ["https://www.bing.com/search?q="]
    # https://www.google.com/search?q="vmware"+site:wikipedia.org
    # https://www.bing.com/search?q="baidu"+site:wikipedia.org
    # thread_local = threading.local()
    # options = webdriver.ChromeOptions()
    # options.add_argument('headless')  # 无界面模式
    # options.add_argument(f'user-agent={random.choice(USER_AGENTS)}')
    # options.add_argument("--no-sandbox")
    # options.add_argument('--proxy-server=socks5://127.0.0.1:8889')
    # # options.binary_location = chrome_path
    # service = Service(executable_path=CHROMEDRIVER_EXE)
    # try:
    #     driver = get_driver(thread_local, options=options, service=service)
    # except Exception as e:
    #     print(f"获取driver失败: {str(e)}")
    #     close_driver(thread_local)  # 关闭当前的WebDriver实例
    #     return None, None

    for search_engine_url in search_engine_urls:
        retry = 0
        query_url = search_engine_url + query
        urls = []
        while retry < 1:
            try:
                # driver.get(query_url)
                # # 等待直到ol标签加载完成
                # WebDriverWait(driver, 10).until(wait_for_element)
                # time.sleep(1)
                session = requests.Session()
                # session.proxies = proxy
                session.headers = {"User-Agent": random.choice(USER_AGENTS)}
                res = session.get(query_url)
                soup = BeautifulSoup(res.text, 'html.parser')
                # soup = BeautifulSoup(driver.page_source, 'html.parser')
                results_list = soup.find('ol', id='b_results')
                results_items = results_list.find_all('li', class_='b_algo', recursive=False)

                if not results_items:
                    # print(f"没有li标签")
                    retry += 1
                    continue

                for item in results_items:
                    result_div = item.find('div', class_='b_tpcn')
                    if result_div:
                        # 在找到的div标签内寻找class为'tilk'的a标签以获取链接
                        anchor = result_div.find('a', class_='tilk')
                        if anchor and 'href' in anchor.attrs:
                            link = anchor['href']
                            if link.startswith("https://www.bing.com/ck/a"):
                                link = parse.parse_qs(urlparse(link).query)['u'][0][2:]
                                padding = len(link) % 4
                                if padding:
                                    link += '=' * (4 - padding)
                                try:
                                    link = base64.b64decode(link).decode("utf-8")
                                except:
                                    print(f"解码失败:{link}")
                                    continue
                            host = urlparse(link).netloc
                            if host.endswith("wikipedia.org"):
                                urls.append(link)
                    else:
                        print(f"找不到div  url:{query_url}")
                        retry += 1
                        continue
                    if len(urls) >= 5:
                        break
                if len(urls) == 0:
                    retry += 1
                else:
                    break
            # except NoSuchWindowException as e:
            #     print(f"无法找到窗口:{str(e)}")
            #     retry += 1
            #     continue
            #
            # except WebDriverException as e:
            #     if "not connected to DevTools" in str(e):
            #         print(f"无法连接到DevTools:{str(e)}")
            #     retry += 1
            #     continue

            except Exception as e:
                print(f"访问 {query_url} 时发生错误:{str(e)}")
                retry += 1
                continue

        key_info = []
        for domain in urls:
            try:
                # driver.get(domain)
                # soup = BeautifulSoup(driver.page_source, 'html.parser')
                session = requests.Session()
                # session.proxies = proxy
                session.headers = {"User-Agent": random.choice(USER_AGENTS)}
                res = session.get(query_url)
                soup = BeautifulSoup(res.text, 'html.parser')
                max_cnt = 5
                cnt = 0
                p_list = soup.findAll("p")
                if len(p_list) == 0:
                    continue
                p = p_list[cnt].text
                if domain.split("://")[1].startswith("zh.") or domain.split("://")[1].startswith("ja."):
                    while not check(keyword, p, "loose") and cnt < len(p_list) - 1:
                        cnt += 1
                        p = p_list[cnt].text
                    if not check(keyword, p, "loose") or "may refer to" in p:
                        continue
                else:
                    while not check(keyword, p) and cnt < len(p_list) - 1:
                        cnt += 1
                        p = p_list[cnt].text
                    if not check(keyword, p) or "may refer to" in p:
                        continue
                if cnt < max_cnt:
                    key_info.append((p, domain.split("://")[1] + "?p=" + str(cnt), cnt))
            except Exception as e:
                print(f"获取内页时发生错误" + str(e))
                continue
        if len(key_info) == 0:
            continue
        key_info = sorted(key_info, key=lambda x: x[2])
        for wiki in key_info:
            if wiki[1].startswith("simple") or wiki[1].startswith("en"):
                # close_driver(thread_local)
                return wiki[0], wiki[1] + "&q=" + keyword
        # close_driver(thread_local)
        return key_info[0][0], key_info[0][1] + "&q=" + keyword
    # close_driver(thread_local)
    # print(f"无法找到wiki链接:{query}")
    return None, None


def get_wiki(cluster_id, org_name):
    try:
        # domain keyword 开头统一加上"*."
        if org_name.startswith("*."):
            while org_name.startswith("*."):
                org_name = org_name[2:]
            keyword = extract(org_name).domain
        else:
            keywords = re.sub(r'[(（].*[)）]', ' ', org_name)
            keywords = regex.split(r"[\s,.]+", keywords)
            keywords = [k.lower() for k in keywords if k.lower() not in dismiss_keyword and len(k) > 1]
            keyword = " ".join(keywords)
            for dismiss in non_english_dismiss:
                keyword = keyword.replace(dismiss, "")
            if len(keyword) < 1:
                keyword = org_name
        # search english wiki first
        query_list = [f'site:en.wikipedia.org+"{keyword}"+{org_name}', f'site:wikipedia.org+"{keyword}"+{org_name}']
        for query in query_list:
            key_info, key_info_url = perform_wiki_search(query, keyword)
            if key_info and key_info_url:
                if key_info_url.startswith("simple") or key_info_url.startswith("en"):
                    key_info_en = key_info
                else:
                    key_info_en = translate_to_english(key_info)
                return cluster_id, key_info, key_info_en
    except Exception as e:
        print(f"处理{org_name}时发生错误:{str(e)}")
        return cluster_id, None, None
    return cluster_id, None, None


def get_wiki_info(time_stamp):
    conn = create_connection()
    cursor = conn.cursor()
    offset = 0
    while True:
        cursor.execute(f"SELECT cluster_id, wiki_keyword FROM cluster_{time_stamp} WHERE wiki_keyword is not NULL and wiki_text is null limit 10000 offset {offset}")
        results_to_get = cursor.fetchall()
        offset += 10000
        if len(results_to_get) == 0:
            break
        buff_size = 100
        cluster_id_list = []
        values_list = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(get_wiki, to_get[0], to_get[1]) for to_get in results_to_get]
            progress_bar = tqdm(total=len(futures), desc="processing")
            for future in as_completed(futures):
                progress_bar.update(1)
                cluster_id, key_info, key_info_en = future.result()
                if key_info is None:
                    continue
                cluster_id_list.append(cluster_id)
                values_list.append({"wiki_text": key_info, "wiki_text_en": key_info_en})
                # print(f"cluster_id:{cluster_id} wiki_text:{key_info} wiki_text_en:{key_info_en}")
                if len(cluster_id_list) >= buff_size:
                    update_cluster_buff(cluster_id_list, values_list, time_stamp, conn)
                    conn.commit()
                    cluster_id_list = []
                    values_list = []
            if len(cluster_id_list) != 0:
                update_cluster_buff(cluster_id_list, values_list, time_stamp, conn)
                conn.commit()
            progress_bar.close()
    conn.close()
