import random
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from nltk import word_tokenize
from tqdm import tqdm
from bs4.element import Comment

from sql_operation.db_operate import create_connection, update_webpage
from scraper.scraper_utils import USER_AGENTS, translate_to_english

KEYWORDS_EN = ["about", "service", "solution", "coverage", "history", "career", "product", "contact"]
KEYWORDS_EN_EXT = ["us", "our", "we", "who"]
KEYWORDS_ZH = ["关于", "概况", "联系", "介绍", "我们", "产品", "职责"]
logfile = "./log.txt"
log_mux = threading.Lock()
mux = threading.Lock()


def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if element.parent.get('style') is not None and 'display:none' in element.parent.get('style').replace(" ", ""):
        return False
    if isinstance(element, Comment):
        return False
    return True


def text_from_html(soup):
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)
    return u". ".join(t.strip() for t in visible_texts)


def check(text):
    words = word_tokenize(text)
    words = [word.lower() for word in words]
    if any(keyword.lower() in words for keyword in KEYWORDS_EN) or any(keyword.lower() in words for keyword in KEYWORDS_EN_EXT):
        return True
    if any(keyword in text for keyword in KEYWORDS_ZH):
        return True
    return False


def worker(item, time_stamp):
    url = "http://" + item
    url_origin = item
    domain_with_scheme = "{}://{}".format(urlparse(url).scheme, urlparse(url).netloc)
    res = {"main_page_text": "", "inner_page_text": "", "title": "", "meta_des": ""}
    stack = [domain_with_scheme]
    links = [domain_with_scheme]
    while len(stack) > 0:
        current_url = stack.pop()
        try:
            user_agent = random.choice(USER_AGENTS)
            response = requests.get(url, headers={'User-Agent': user_agent}, timeout=30)
            if response.status_code != 200:
                continue
            soup = BeautifulSoup(response.text, 'html.parser')
            title = soup.title.string if soup.title else ""
            # # response status != 200
            # response = requests.head(current_url, timeout=10)
            if response.status_code != 200:
                continue
            if title == "403 Forbidden" or title == "404 Not Found":
                continue

            text = text_from_html(soup.body)
            text = re.sub(r"\s+", " ", text)
            meta_description = soup.find("meta", {"name": "description"})
            if meta_description:
                meta_description = meta_description.get('content')

            if current_url == domain_with_scheme:
                res["main_page_text"] = text
            else:
                res["inner_page_text"] += ". " + text
            if title:
                res["title"] += ". " + title
            if meta_description:
                res["meta_des"] += ". " + meta_description
            # 获取内页链接
            for a in soup.find_all('a', href=True):
                if len(links) >= 5:
                    break
                if not a['href'].startswith('http'):
                    full_link = urljoin("{}://{}".format(urlparse(current_url).scheme, urlparse(current_url).netloc), a['href'])
                else:
                    full_link = a['href']
                if full_link not in links and (check(a.text) or any(keyword.lower() in a['href'].lower() for keyword in KEYWORDS_EN)):
                    links.append(full_link)
                    stack.append(full_link)
        except Exception as e:
            # print(f"获取失败: {str(e)}" + current_url)
            continue

    # translate
    # if res["main_page"] != "" or res["inner_pages"] != "":
    #     res["main_page"] = translate_to_english(res["main_page"])
    #     res["inner_pages"] = translate_to_english(res["inner_pages"])
    #     res["title"] = translate_to_english(res["title"])
    #     res["meta_description"] = translate_to_english(res["meta_description"])
    res["main_page_text"] = res["main_page_text"][:65535]
    res["inner_page_text"] = res["inner_page_text"][:65535]
    res["title"] = res["title"][:65535]
    res["meta_des"] = res["meta_des"][:65535]
    return url_origin, res


def get_webpage(time_stamp):
    offset = 0
    limit = 500
    conn = create_connection()
    cursor = conn.cursor()
    while True:
        cursor.execute(f"select url from webpage_{time_stamp} where main_page_text is null limit %s offset %s",
                       (limit, offset))
        offset += limit
        res_to_get = cursor.fetchall()
        url_list = []
        update_values = []
        if len(res_to_get) == 0:
            break
        cnt = 0
        with ThreadPoolExecutor(max_workers=100) as executor:
            # 创建 future 对象列表，待获取列表
            futures = [executor.submit(worker, to_get[0], time_stamp) for to_get in res_to_get]
            # 创建进度条
            progress_bar = tqdm(total=len(futures), desc="处理进度")
            for future in as_completed(futures):
                # 进度条每次更新一个
                progress_bar.update(1)
                url_origin, res = future.result()
                if res["main_page_text"] == "" and res["inner_page_text"] == "" and res["title"] == "" and res["meta_des"] == "":
                    continue
                cnt += 1
                url_list.append(url_origin)
                update_values.append(res)
            progress_bar.close()
        if len(url_list) == 0:
            continue
        update_webpage(url_list, update_values, time_stamp, conn)
        conn.commit()
        print(f"success rate: {cnt}/{len(res_to_get)}, {cnt}")
    conn.close()
