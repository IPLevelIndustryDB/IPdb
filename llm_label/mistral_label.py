import re
import threading
from tqdm import tqdm
from prompt.description_gpt import gpt_cate_des
from sql_operation.db_operate import create_connection, update_cluster_buff
from nltk.tokenize import word_tokenize

from transformers import AutoTokenizer, AutoModelForCausalLM
from params import mistral_tokenizer_path as tokenizer_path, mistral_model_path as model_path
mux = threading.Lock()
categories = list(gpt_cate_des.keys())
with open("./prompt/gemini_user_prompt.txt", "r", encoding="utf-8") as f:
    prompt = f.read()
with open("./prompt/prompt_all_cate.txt", "r", encoding="utf-8") as f:
    prompt_all_cate = f.read()


def mistral_label(time_stamp):
    conn = create_connection()
    cursor = conn.cursor()
    cluster_table = f"cluster_{time_stamp}"
    cursor.execute(f"select cluster_id, wiki_keyword, wiki_text_en, wiki_text from {cluster_table} where wiki_text is not null and wiki_text != '' and mistral_label is null and cluster_id in (select cluster_id from url_cert_{time_stamp} where cluster_id is not null)")
    items = cursor.fetchall()
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_path, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(model_path, trust_remote_code=True).cuda()
    progress_bar = tqdm(total=len(items), desc="处理进度")

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
        #  first round all categories
        try:
            messages = [
                {
                    "role": "user",
                    "content": prompt_all_cate + des + "Noting that you should not give category that do not in the list" + str(categories)
                }
            ]
            encodeds = tokenizer.apply_chat_template(messages, return_tensors="pt")
            model_inputs = encodeds.to("cuda")
            generated_responses = model.generate(model_inputs, do_sample=True, max_length=4096, pad_token_id=tokenizer.eos_token_id)
            chat_ = tokenizer.batch_decode(generated_responses)[0]
            chat_ = re.findall(r'\[/INST][\s\S]*</s>', chat_)[0]
            ca = re.findall(r' *[”"“\*]*[cC]ategories[”"”\*]*.*(?::|are|are:) *([\[{].*[]}])', chat_.replace("\n", ""))
            if len(ca) > 0:
                category_ = ca[0]
            else:
                category_ = "[UnKnown]"
        except Exception as e:
            print("error: " + e.__str__())
            return None, None
        data_c1 = category_.split(",")
        for c in data_c1:
            for j in range(len(categories) - 1, -1, -1):
                if categories[j].lower().replace(" ", "") in c.lower().replace(" ", ""):
                    c1.append(categories[j])
                    cate_stack.append(categories[j])
                    break
        if len(c1) == 0:
            print("no category found")
            return None, None
        #  second round of classification
        while len(cate_stack) > 0:
            cate = cate_stack.pop(0)
            cate_def = gpt_cate_des[cate]
            if cate == "UnKnown" or cate == "Other":
                continue
            try:
                user_prompt = prompt.replace("<<definition>>", cate_def).replace("<<category>>", cate) + des
                messages = [
                    {
                        "role": "user",
                        "content": user_prompt
                    }
                ]
                encodeds = tokenizer.apply_chat_template(messages, return_tensors="pt")
                model_inputs = encodeds.to("cuda")
                generated_responses = model.generate(model_inputs, do_sample=True, max_length=4096, pad_token_id=tokenizer.eos_token_id)
                chat_ = tokenizer.batch_decode(generated_responses)[0]
                chat_ = re.findall(r'\[/INST][\s\S]*</s>', chat_)[0]
                chat_tokens = word_tokenize(chat_.lower())
                if "yes" in chat_tokens and "no" not in chat_tokens:
                    c2.append(cate)
            except Exception as e:
                print("error: " + e.__str__())
                return None, None
        return cluster_id, {"c1": [categories.index(c) for c in c1], "c2": [categories.index(c) for c in c2]}

    cluster_id_list = []
    update_values = []
    for item in items:
        cluster_id, labels = worker(item)
        progress_bar.update(1)
        if cluster_id is None:
            continue
        cluster_id_list.append(cluster_id)
        update_values.append({"mistral_label": str(labels)})
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
