import re
import threading

from tqdm import tqdm
from transformers import AutoTokenizer, AutoModel

from prompt.description_glm import glm_cate_des, glm_cate_name_map
from params import glm_tokenizer_path as tokenizer_path, glm_model_path as model_path

mux = threading.Lock()
categories = list(glm_cate_name_map.keys())
categories_en = list(glm_cate_des.keys())
with open("./prompt/glm_user_prompt.txt", "r", encoding="utf-8") as f:
    prompt = f.read()
with open("./prompt/glm_prompt_all_cate.txt", "r", encoding="utf-8") as f:
    prompt_all_cate = f.read()


def glm_label(items):
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_path, trust_remote_code=True)
    model = AutoModel.from_pretrained(model_path, trust_remote_code=True).cuda()
    model = model.eval()
    progress_bar = tqdm(total=len(items), desc="processing")
    res = {}

    def worker(item_):
        org_name = item_[0]
        cate_stack = []
        c1 = []
        c2 = []
        des = org_name + ": " + "Related information: " + item_[1]
        #  first round all categories
        try:
            response, history = model.chat(tokenizer, prompt_all_cate + des, history=[], top_p=1, temperature=0.1)
            chat_ = response
            ca = re.findall(r' *[““”"“\*]*类别[”"””\*]*.*(?::：是)? *([\[{【].*[]}】])', chat_.replace("\n", ""))
            if len(ca) > 0:
                category_ = ca[0]
            else:
                category_ = "[UnKnown]"
        except Exception as e:
            print("error: " + e.__str__())
            return
        data_c1 = re.split(r'[,，、]', category_)
        for c in data_c1:
            for j in range(len(categories)):
                if categories[j].replace(" ", "") in c.replace(" ", ""):
                    c1.append(glm_cate_name_map[categories[j]])
                    cate_stack.append(categories[j])
                    break

        if len(cate_stack) == 0:
            return

        #  second round of classification
        while len(cate_stack) > 0:
            cate_cn = cate_stack.pop(0)
            cate = glm_cate_name_map[cate_cn]
            cate_def = glm_cate_des[cate]
            if cate == "UnKnown" or cate == "Other":
                continue
            try:
                user_prompt = prompt.replace("<<类别定义>>", cate_def).replace("<<类别>>", cate_cn) + des
                response, _ = model.chat(tokenizer, user_prompt, history=[i for i in history], top_p=1, temperature=0.1)
                chat_ = response

                if "yes" in chat_.lower() and "no" not in chat_.lower():
                    c2.append(cate)
            except Exception as e:
                print("error: " + e.__str__())
                return
        if len(c2) == 0:
            return
        res[org_name] = [categories_en.index(c) for c in c2]

    for item in items:
        worker(item)
        progress_bar.update(1)
    progress_bar.close()
    return res
