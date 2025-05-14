import re
import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from tqdm import tqdm
from typing import List, Optional
import fire
from llama import Llama, Dialog
import os

from prompt.description_llama import llama_cate_des
from nltk.tokenize import word_tokenize
from sql_operation.db_operate import create_connection, update_cluster_buff
from params import llama_ckpt_dir as ckpt_dir, llama_tokenizer_path as tokenizer_path

categories = list(llama_cate_des.keys())
with open("./prompt/gpt_user_prompt.txt", "r", encoding="utf-8") as f:
    prompt = f.read()
with open("./prompt/llama_system_prompt.txt", "r", encoding="utf-8") as f:
    system_prompt = f.read()
with open("./prompt/prompt_all_cate.txt", "r", encoding="utf-8") as f:
    prompt_all_cate = f.read()
mux = threading.Lock()

max_seq_len = 4096
max_batch_size = 6
temperature: float = 0.3
top_p: float = 0.95


def llama_label(time_stamp):
    os.environ.setdefault("RANK", "0")
    os.environ.setdefault("WORLD_SIZE", "1")
    os.environ.setdefault("MASTER_ADDR", "localhost")
    os.environ.setdefault("MASTER_PORT", "29500")

    conn = create_connection()
    cursor = conn.cursor()
    cluster_table = f"cluster_{time_stamp}"
    cursor.execute(f"select cluster_id, wiki_keyword, wiki_text_en, wiki_text from {cluster_table} where wiki_text is not null and wiki_text != '' and llama_label is null and cluster_id in (select cluster_id from url_cert_{time_stamp} where cluster_id is not null)")
    items = cursor.fetchall()

    try:
        generator = Llama.build(ckpt_dir=ckpt_dir, tokenizer_path=tokenizer_path, max_seq_len=max_seq_len,
                                max_batch_size=max_batch_size)
    except Exception as e:
        print("error: " + e.__str__())
        return

    def worker(item_):
        cluster_id = item_[0]
        org_name = item_[1]
        if item[2] is not None and "[EN-TRANSLATE-ERROR]" not in item[2] and "[EN-TransError]" not in item[2]:
            des = org_name + ": " + "related information: " + item_[2]
        else:
            des = org_name + ": " + "related information: " + item_[3]
        cate_stack = []
        c1 = []
        c2 = []
        #  first round of classification
        try:
            dialogs: List[Dialog] = [[
                {"role": "system", "content": system_prompt + ''' response should always contain an answer in format {”categories”:[<category1>, <category2>, ...]}'''},
                {"role": "user", "content": prompt_all_cate + des},
            ]]
            results = generator.chat_completion(
                dialogs,  # type: ignore
                max_gen_len=None,
                temperature=temperature,
                top_p=top_p,
            )
            chat_ = results[0]['generation']['content']
            ca = re.findall(r' *[”"“\*]*[cC]ategories[”"”\*]*.*(?::|are|are:) *([\[{].*[]}])', chat_.replace("\n", ""))
            if len(ca) > 0:
                category_ = ca[0]
            else:
                category_ = "[UnKnown]"
            data_c1 = category_.split(",")
            for c in data_c1:
                for j in range(len(categories) - 1, -1, -1):
                    if categories[j].lower().replace(" ", "") in c.lower().replace(" ", ""):
                        c1.append(categories[j])
                        cate_stack.append(categories[j])
                        break
        except Exception as e:
            print("error: " + e.__str__())
            return None, None

        #  second round, one by one
        dialogs: List[Dialog] = []
        cates = []
        while len(cate_stack) > 0:
            cate = cate_stack.pop(0)
            cate_def = llama_cate_des[cate]
            if cate == "UnKnown" or cate == "Other":
                continue
            user_prompt = prompt.replace("<<definition>>", cate_def).replace("<<category>>", cate) + des
            dialogs.append([
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ])
            cates.append(cate)

        if len(dialogs) == 0 or len(dialogs) > max_batch_size:
            return None, None
        results = generator.chat_completion(
            dialogs,  # type: ignore
            max_gen_len=None,
            temperature=temperature,
            top_p=top_p,
        )
        for cate, result in zip(cates, results):
            chat_ = result['generation']['content']
            chat_tokens = word_tokenize(chat_.lower())
            if "yes" in chat_tokens and "no" not in chat_tokens:
                c2.append(cate)
        # with open("log.txt", "a", encoding="utf-8") as f:
        #     f.write(f"{wiki_url}: {c1}, {c2}\n")
        if len(c2) == 0:
            return None, None
        return cluster_id, {"c1": [categories.index(c) for c in c1], "c2": [categories.index(c) for c in c2]}

    progress_bar = tqdm(total=len(items), desc="处理进度")
    cluster_id_list = []
    update_values = []
    for item in items:
        cluster_id, labels = worker(item)
        progress_bar.update(1)
        if cluster_id is None:
            continue
        cluster_id_list.append(cluster_id)
        update_values.append({"llama_label": str(labels)})
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
