import re
import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from tqdm import tqdm
from openai import OpenAI
from prompt.description_gpt import gpt_cate_des
from nltk.tokenize import word_tokenize
from params import GPT_API_KEY


categories = list(gpt_cate_des.keys())
with open("./prompt/gpt_user_prompt.txt", "r", encoding="utf-8") as f:
    prompt = f.read()
with open("./prompt/gpt_system_prompt.txt", "r", encoding="utf-8") as f:
    system_prompt = f.read()
with open("./prompt/prompt_all_cate.txt", "r", encoding="utf-8") as f:
    prompt_all_cate = f.read()
mux = threading.Lock()
res = {}


def worker(item_):

    org_name = item_[0]
    des = org_name + ": " + "related information: " + item_[1]
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
            # you can change the model here
            model="gpt-3.5-turbo-0125"
        )
        chat_ = response.choices[0].message.content
        ca = re.findall(r' *[”"“\*]*[cC]ategories[”"”\*]*.*(?::|are|are:) *([\[{].*[]}])', chat_.replace("\n", ""))
        if len(ca) > 0:
            category_ = ca[0]
        else:
            category_ = "[UnKnown]"
        data_c1 = category_.split(",")
        for c in data_c1:
            for j in range(len(categories)):
                if categories[j].lower().replace(" ", "") in c.lower().replace(" ", ""):
                    c1.append(categories[j])
                    cate_stack.append(categories[j])
                    break
    except Exception as e:
        print("error: " + e.__str__())
        return

    #  second round, one by one
    while len(cate_stack) > 0:
        cate = cate_stack.pop(0)
        cate_def = gpt_cate_des[cate]
        if cate == "UnKnown" or cate == "Other":
            continue
        try:
            user_prompt = prompt.replace("<<definition>>", cate_def).replace("<<category>>", cate) + des
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
                model="gpt-3.5-turbo-0125"
            )
            chat_ = response.choices[0].message.content
            chat_tokens = word_tokenize(chat_.lower())
            if "yes" in chat_tokens and "no" not in chat_tokens:
                c2.append(cate)

        except Exception as e:
            print("error: " + e.__str__())
            return
    mux.acquire()
    res[org_name] = [categories.index(c) for c in c2]
    mux.release()


def gpt_label(items):

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(worker, item) for item in items]
        progress_bar = tqdm(total=len(futures), desc="processing")
        for _ in as_completed(futures):
            progress_bar.update(1)
        progress_bar.close()

    return res
