import re
import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from tqdm import tqdm
from openai import OpenAI
from prompt.description_gpt import gpt_cate_des
from nltk.tokenize import word_tokenize
from sql_operation.db_operate import create_connection, update_cluster_buff
from params import GPT_API_KEY


categories = list(gpt_cate_des.keys())
with open("./prompt/gpt_user_prompt.txt", "r", encoding="utf-8") as f:
    prompt = f.read()
with open("./prompt/gpt_system_prompt.txt", "r", encoding="utf-8") as f:
    system_prompt = f.read()
with open("./prompt/prompt_all_cate.txt", "r", encoding="utf-8") as f:
    prompt_all_cate = f.read()
mux = threading.Lock()


def worker(item_, time_stamp):

    cluster_id = item_[0]
    org_name = item_[1]
    des = org_name + ": " + "related information: " + item_[2]
    cate_stack = []
    c1 = []
    c2 = []
    #  first round of classification
    try:
        client = OpenAI(
            api_key=GPT_API_KEY,
        )
        response = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": system_prompt

                },
                {
                    "role": "user",
                    "content": prompt_all_cate + des
                },
            ],
            model="gpt-4o-mini"
        )
        chat_ = response.choices[0].message.content
        ca = re.findall(r' *[”"“\*]*[cC]ategories[”"”\*]*.*(?::|are|are:) *([\[{].*[]}])', chat_.replace("\n", ""))
        if len(ca) > 0:
            category_ = ca[0]
        else:
            category_ = "[UnKnown]"
        data_c1 = category_.split(",")
        for c in data_c1:
            for j in range(len(categories) - 1, 0, -1):
                if categories[j].lower().replace(" ", "") in c.lower().replace(" ", ""):
                    c1.append(categories[j])
                    cate_stack.append(categories[j])
                    break
    except Exception as e:
        print("error: " + e.__str__())
        return None, None

    #  second round, one by one
    while len(cate_stack) > 0:
        cate = cate_stack.pop(0)
        cate_def = gpt_cate_des[cate]
        if cate == "UnKnown" or cate == "Other":
            continue
        try:
            user_prompt = prompt.replace("<<definition>>", cate_def).replace("<<category>>", cate) + des
            # print(user_prompt)
            client = OpenAI(
                api_key=GPT_API_KEY,
            )

            response = client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": system_prompt

                    },
                    {
                        "role": "user",
                        "content": user_prompt
                    },
                ],
                model="gpt-4o-mini"
            )
            chat_ = response.choices[0].message.content
            chat_tokens = word_tokenize(chat_.lower())
            if "yes" in chat_tokens and "no" not in chat_tokens:
                c2.append(cate)

        except Exception as e:
            print("error: " + e.__str__())
            return None, None

    return cluster_id, {"c1": [categories.index(c) for c in c1], "c2": [categories.index(c) for c in c2]}


def gpt_label(time_stamp):
    conn = create_connection()
    cursor = conn.cursor()
    cluster_table = f"cluster_{time_stamp}"
    cursor.execute(f"select cluster_id, wiki_keyword, wiki_text from {cluster_table} where wiki_text is not null and wiki_text != '' and gpt_label is null and cluster_id in (select cluster_id from url_cert_{time_stamp} where cluster_id is not null)")
    items = cursor.fetchall()
    cluster_id_list = []
    update_values = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        # 创建 future 对象列表，待获取列表
        futures = [executor.submit(worker, item, time_stamp) for item in items]
        # 创建进度条
        progress_bar = tqdm(total=len(futures), desc="处理进度")
        for future in as_completed(futures):
            # 进度条每次更新一个
            progress_bar.update(1)
            cluster_id, labels = future.result()
            if cluster_id is None:
                continue
            cluster_id_list.append(cluster_id)
            update_values.append({"gpt_label": str(labels)})
            if len(cluster_id_list) > 100:
                update_cluster_buff(cluster_id_list,update_values,time_stamp,conn)
                conn.commit()
                cluster_id_list = []
                update_values = []
        if len(cluster_id_list) > 0:
            update_cluster_buff(cluster_id_list, update_values, time_stamp, conn)
            conn.commit()
        progress_bar.close()
        conn.close()
