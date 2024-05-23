import sys

import torch

from llm_label.gemini_label import gemini_label
from llm_label.glm_label import glm_label
from llm_label.gpt_label import gpt_label
from llm_label.llama_label import llama_label
from llm_label.mistral_label import mistral_label
from prompt.description_gemini import gemini_cate_des
import csv

categories = list(gemini_cate_des.keys())

if __name__ == '__main__':
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    with open(input_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    gemini_label_res = gemini_label(lines)
    glm_label_res = glm_label(lines)
    gpt_label_res = gpt_label(lines)
    llama_label_res = llama_label(lines)
    mistral_label_res = mistral_label(lines)

    cate_num = 23
    x_predict = []
    org_list = []
    # combine
    for line in lines:
        tmp = [0 for _ in range(cate_num * 5)]
        gemini_tag = gemini_label_res[line] if line in gemini_label_res else []
        glm_tag = glm_label_res[line] if line in glm_label_res else []
        gpt_tag = gpt_label_res[line] if line in gpt_label_res else []
        llama_tag = llama_label_res[line] if line in llama_label_res else []
        mistral_tag = mistral_label_res[line] if line in mistral_label_res else []
        for tag in gpt_tag:
            if tag >= cate_num:
                continue
            tmp[tag] += 1
        for tag in gemini_tag:
            if tag >= cate_num:
                continue
            tmp[tag + cate_num] += 1
        for tag in glm_tag:
            if tag >= cate_num:
                continue
            tmp[tag + 2 * cate_num] += 1
        for tag in llama_tag:
            if tag >= cate_num:
                continue
            tmp[tag + 3 * cate_num] += 1
        for tag in mistral_tag:
            if tag >= cate_num:
                continue
            tmp[tag + 4 * cate_num] += 1
        x_predict.append(tmp)
        org_list.append(line[0])

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    clf = torch.load("llm_label/llmics.pkl")
    x_predict = torch.tensor(x_predict)
    pred = clf(x_predict.float().to(device)).detach().cpu().numpy()
    combined_res = {}
    for i in range(len(org_list)):
        combined_res[org_list[i]] = [j for j in range(cate_num) if pred[i][j] > 0.5]

    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(["org_name", "gemini", "glm", "gpt", "llama", "mistral", "llmics"])
        for k, v in combined_res.items():
            gemini_label_industry = [categories[i] for i in gemini_label_res[k]] if k in gemini_label_res else []
            glm_label_industry = [categories[i] for i in glm_label_res[k]] if k in glm_label_res else []
            gpt_label_industry = [categories[i] for i in gpt_label_res[k]] if k in gpt_label_res else []
            llama_label_industry = [categories[i] for i in llama_label_res[k]] if k in llama_label_res else []
            mistral_label_industry = [categories[i] for i in mistral_label_res[k]] if k in mistral_label_res else []
            llmics_industry = [categories[i] for i in combined_res[k]]
            csv_writer.writerow([k, gemini_label_industry, glm_label_industry, gpt_label_industry, llama_label_industry,
                                 mistral_label_industry, llmics_industry])

