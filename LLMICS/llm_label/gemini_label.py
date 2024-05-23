import google.generativeai as genai
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from tqdm import tqdm
from prompt.description_gemini import gemini_cate_des
from nltk.tokenize import word_tokenize
from params import GOOGLE_API_KEY

mux = threading.Lock()
categories = list(gemini_cate_des.keys())
res = {}

with open("./prompt/gemini_user_prompt.txt", "r", encoding="utf-8") as f:
    prompt = f.read()
with open("./prompt/prompt_all_cate.txt", "r", encoding="utf-8") as f:
    prompt_all_cate = f.read()


def worker(item_):
    org_name = item_[0]
    des = org_name + ": " + "related information: " + item_[1]
    cate_stack = []
    c1 = []
    c2 = []
    retry = 10
    category_ = ""
    while retry > 0:
        try:
            #  you can replace the model with the one you want to use
            model = genai.GenerativeModel('gemini-pro')
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
        for j in range(len(categories)):
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
                model = genai.GenerativeModel('gemini-pro')
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

    mux.acquire()
    res[org_name] = [categories.index(c) for c in c2]
    mux.release()


def gemini_label(items):
    genai.configure(api_key=GOOGLE_API_KEY)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(worker, item) for item in items]
        progress_bar = tqdm(total=len(futures), desc="Processing")
        for _ in as_completed(futures):
            progress_bar.update(1)
        progress_bar.close()

    return res
