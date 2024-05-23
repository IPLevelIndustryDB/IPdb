import google.generativeai as genai
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from tqdm import tqdm
from sql_operation.db_operate import create_connection, update_cluster_buff
from prompt.description_gemini import gemini_cate_des
from nltk.tokenize import word_tokenize
from params import GOOGLE_API_KEY

mux = threading.Lock()
categories = list(gemini_cate_des.keys())
with open("./prompt/gemini_user_prompt.txt", "r", encoding="utf-8") as f:
    prompt = f.read()
with open("./prompt/prompt_all_cate.txt", "r", encoding="utf-8") as f:
    prompt_all_cate = f.read()


def worker(item_, time_stamp):
    cluster_id = item_[0]
    org_name = item_[1]
    des = org_name + ": " + "related information: " + item_[2]
    cate_stack = []
    c1 = []
    c2 = []
    retry = 10
    category_ = ""
    while retry > 0:
        try:
            model = genai.GenerativeModel('gemini-1.0-pro')
            chat = model.start_chat(history=[])
            chat_ = chat.send_message(prompt_all_cate + " " + des).text

            ca = re.findall(r' *[”"“\*]*[cC]ategories[”"”\*]*.*(?::|are|are:) *([\[{].*[]}])', chat_.replace("\n", ""))
            if len(ca) > 0:
                category_ = ca[0]
                break
        except Exception as e:
            print(str(e))
            time.sleep(30)
            retry -= 1
    if category_ == "":
        category_ = "[UnKnown]"
    data_c1 = category_.split(",")

    for c in data_c1:
        for j in range(len(categories) - 1, 0, -1):
            if categories[j].lower().replace(" ", "") in c.lower().replace(" ", ""):
                c1.append(categories[j])
                cate_stack.append(categories[j])
                break
    while len(cate_stack) > 0:
        cate = cate_stack[0]
        cate_def = gemini_cate_des[cate]
        if cate == "UnKnown" or cate == "Other":
            cate_stack.pop(0)
            continue
        retry = 10
        while retry > 0:
            try:
                user_prompt = prompt.replace("<<definition>>", cate_def).replace("<<category>>", cate) + des
                model = genai.GenerativeModel('gemini-1.5-flash')
                chat = model.start_chat(history=[])
                chat_ = chat.send_message(user_prompt).text

                chat_tokens = word_tokenize(chat_.lower())
                if "yes" in chat_tokens and "no" not in chat_tokens:
                    c2.append(cate)
                cate_stack.pop(0)
                break
            except Exception as e:
                print(str(e))
                time.sleep(30)
                retry -= 1
    return cluster_id, {"c1": [categories.index(c) for c in c1], "c2": [categories.index(c) for c in c2]}


def gemini_label(time_stamp):

    genai.configure(api_key=GOOGLE_API_KEY, transport='rest')

    conn = create_connection()
    cursor = conn.cursor()
    cluster_table = f"cluster_{time_stamp}"
    cursor.execute(
        f"select cluster_id, wiki_keyword, wiki_text from {cluster_table} where wiki_text is not null and wiki_text != '' and gemini_label is null and cluster_id in (select cluster_id from url_cert_{time_stamp} where cluster_id is not null)")
    items = cursor.fetchall()
    cluster_id_list = []
    update_values = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        # 创建 future 对象列表，待获取列表
        futures = [executor.submit(worker, item, time_stamp) for item in items]
        # 创建进度条
        progress_bar = tqdm(total=len(futures), desc="processing progress")
        for future in as_completed(futures):
            # 进度条每次更新一个
            progress_bar.update(1)
            cluster_id, labels = future.result()
            if cluster_id is None:
                continue
            cluster_id_list.append(cluster_id)
            update_values.append({"gemini_label": str(labels)})
            if len(cluster_id_list) > 0:
                update_cluster_buff(cluster_id_list,update_values,time_stamp,conn)
                conn.commit()
                cluster_id_list = []
                update_values = []
        if len(cluster_id_list) > 0:
            update_cluster_buff(cluster_id_list, update_values, time_stamp, conn)
            conn.commit()
        progress_bar.close()
        conn.close()
