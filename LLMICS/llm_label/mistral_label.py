import re
import threading
from tqdm import tqdm
from prompt.description_gpt import gpt_cate_des
from nltk.tokenize import word_tokenize

from transformers import AutoTokenizer, AutoModelForCausalLM
from params import mistral_tokenizer_path as tokenizer_path, mistral_model_path as model_path
mux = threading.Lock()
categories = list(gpt_cate_des.keys())
with open("./prompt/gemini_user_prompt.txt", "r", encoding="utf-8") as f:
    prompt = f.read()
with open("./prompt/prompt_all_cate.txt", "r", encoding="utf-8") as f:
    prompt_all_cate = f.read()


def mistral_label(items):
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_path, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(model_path, trust_remote_code=True).cuda()
    progress_bar = tqdm(total=len(items), desc="processing")
    res = {}

    def worker(item_):
        org_name = item_[0]
        cate_stack = []
        c1 = []
        c2 = []
        des = org_name + ": " + "related information: " + item_[1]
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
            return
        data_c1 = category_.split(",")
        for c in data_c1:
            for j in range(len(categories)):
                if categories[j].lower().replace(" ", "") in c.lower().replace(" ", ""):
                    c1.append(categories[j])
                    cate_stack.append(categories[j])
                    break
        if len(c1) == 0:
            print("no category found")
            return
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
                return
        res[org_name] = [categories.index(c) for c in c2]

    for item in items:
        worker(item)
        progress_bar.update(1)
    progress_bar.close()
    return res
