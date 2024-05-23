import time

import requests
import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from tqdm import tqdm
from prompt.description_ernie import ernie_cate_des, ernie_cate_name_map
from sql_operation.db_operate import create_connection, update_cluster_buff

from transformers import AutoTokenizer, AutoModel
from params import glm_tokenizer_path as tokenizer_path, glm_model_path as model_path

mux = threading.Lock()
categories = list(ernie_cate_name_map.keys())
categories_en = list(ernie_cate_des.keys())
with open("./prompt/glm_user_prompt.txt", "r", encoding="utf-8") as f:
    prompt = f.read()
with open("./prompt/glm_prompt_all_cate.txt", "r", encoding="utf-8") as f:
    prompt_all_cate = f.read()


def glm_label(time_stamp):
    conn = create_connection()
    cursor = conn.cursor()
    cluster_table = f"cluster_{time_stamp}"
    cursor.execute(f"select cluster_id, wiki_keyword, wiki_text_en, wiki_text from {cluster_table} where wiki_text is not null and wiki_text != '' and glm_label is null and cluster_id in (select cluster_id from url_cert_{time_stamp} where cluster_id is not null)")
    items = cursor.fetchall()
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_path, trust_remote_code=True)
    model = AutoModel.from_pretrained(model_path, trust_remote_code=True).cuda()
    model = model.eval()
    progress_bar = tqdm(total=len(items), desc="processing progress")

    def worker(item_):
        cluster_id = item_[0]
        org_name = item_[1]
        if item[2] is not None and "[EN-TRANSLATE-ERROR]" not in item[2] and "[EN-TransError]" not in item[2]:
            des = org_name + ": " + "相关信息: " + item_[2]
        else:
            des = org_name + ": " + "相关信息: " + item_[3]
        cate_stack = []
        c1 = []
        c2 = []
        #  first round all categories
        try:
            response, history = model.chat(tokenizer, prompt_all_cate + des, history=[], top_p=1, temperature=0.1, max_new_tokens=512)
            chat_ = response
            with open("log.txt", "a", encoding="utf-8") as f:
                f.write(des + chat_ + "\n")
            ca = re.findall(r' *[““”"“\*]*类别[”"””\*]*.*(?::：是)? *([\[{【].*[]}】])', chat_.replace("\n", ""))
            if len(ca) > 0:
                category_ = ca[0]
            else:
                category_ = "[UnKnown]"
        except Exception as e:
            print("error: " + e.__str__())
            return None, None
        data_c1 = re.split(r'[,，、]', category_)
        for c in data_c1:
            for j in range(len(categories)):
                if categories[j].replace(" ", "") in c.replace(" ", ""):
                    c1.append(ernie_cate_name_map[categories[j]])
                    cate_stack.append(categories[j])
                    break

        if len(cate_stack) == 0:
            return None, None

        #  second round of classification
        while len(cate_stack) > 0:
            cate_cn = cate_stack.pop(0)
            cate = ernie_cate_name_map[cate_cn]
            cate_def = ernie_cate_des[cate]
            if cate == "UnKnown" or cate == "Other":
                continue
            try:
                user_prompt = prompt.replace("<<类别定义>>", cate_def).replace("<<类别>>", cate_cn) + des
                response, _ = model.chat(tokenizer, user_prompt, history=[i for i in history], top_p=1, temperature=0.1)
                chat_ = response

                if "yes" in chat_.lower() and "no" not in chat_.lower():
                    c2.append(cate)
                with open("log.txt", "a", encoding="utf-8") as f:
                    f.write(chat_ + "\n")
            except Exception as e:
                print("error: " + e.__str__())
                return None, None
        if len(c2) == 0:
            return None, None
        return cluster_id, {"c1": [categories_en.index(c) for c in c1], "c2": [categories_en.index(c) for c in c2]}

    cluster_id_list = []
    update_values = []
    for item in items:
        cluster_id, labels = worker(item)
        progress_bar.update(1)
        if cluster_id is None:
            continue
        cluster_id_list.append(cluster_id)
        update_values.append({"glm_label": str(labels)})
        if len(cluster_id_list) >= 100:
            update_cluster_buff(cluster_id_list, update_values, time_stamp, conn)
            conn.commit()
            cluster_id_list = []
            update_values = []
    if len(cluster_id_list) > 0:
        update_cluster_buff(cluster_id_list, update_values, time_stamp, conn)
    conn.commit()
    conn.close()
    progress_bar.close()
