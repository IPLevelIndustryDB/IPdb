from sql_operation.db_operate import create_connection, update_cluster_buff
import torch
from prompt.description_gemini import gemini_cate_des
gemini_cate_des_keys = list(gemini_cate_des.keys())


def combine_label(time_stamp, model_path):
    conn = create_connection()
    cursor = conn.cursor()
    cluster_table = f"cluster_{time_stamp}"
    cursor.execute(f"select cluster_id, gpt_label, gemini_label, glm_label, llama_label, mistral_label from {cluster_table} where wiki_text is not null and (groundtruth is null or groundtruth = '') and (semi_groundtruth is null or semi_groundtruth = '')")
    rows = cursor.fetchall()
    cate_num = 23
    data_to_predict = []
    cluster_id_list = []
    cnt = 0
    for row in rows:
        cluster_id = row[0]
        gpt_label = eval(row[1]) if row[1] is not None and row[1] != "" else {"c1": [], "c2": []}
        gemini_label = eval(row[2]) if row[2] is not None and row[2] != "" else {"c1": [], "c2": []}
        glm_label = eval(row[3]) if row[3] is not None and row[3] != "" else {"c1": [], "c2": []}
        llama_label = eval(row[4]) if row[4] is not None and row[4] != "" else {"c1": [], "c2": []}
        mistral_label = eval(row[5]) if row[5] is not None and row[5] != "" else {"c1": [], "c2": []}
        tmp = [0 for _ in range(cate_num * 5)]
        for tag in gpt_label['c2']:
            if tag >= cate_num:
                continue
            tmp[tag] += 1
        for tag in gemini_label['c2']:
            if tag >= cate_num:
                continue
            tmp[tag + cate_num] += 1
        for tag in glm_label['c2']:
            if tag >= cate_num:
                continue
            tmp[tag + 2 * cate_num] += 1
        for tag in llama_label['c2']:
            if tag >= cate_num:
                continue
            tmp[tag + 3 * cate_num] += 1
        for tag in mistral_label['c2']:
            if tag >= cate_num:
                continue
            tmp[tag + 4 * cate_num] += 1
        if sum(tmp) == 0:
            continue
        cnt += 1
        data_to_predict.append(tmp)
        cluster_id_list.append(cluster_id)
    print(cnt)
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    model = torch.nn.Sequential(
        torch.nn.Linear(cate_num * 5, cate_num),
        torch.nn.ReLU(),
        torch.nn.Linear(cate_num, cate_num),
        torch.nn.Sigmoid()
    )
    model.load_state_dict(torch.load(model_path, weights_only=True))
    model.to(device)
    model.eval()
    x = torch.tensor(data_to_predict, dtype=torch.float32).to(device)
    y = model(x)
    y = y.cpu().detach().numpy()
    semi_groundtruth_list = []
    cluster_id_to_update = []
    for i in range(len(cluster_id_list)):
        tmp = []
        for j in range(cate_num):
            if y[i][j] > 0.5:
                tmp.append(j)
        if len(tmp) > 0:
            cluster_id_to_update.append(cluster_id_list[i])
            semi_groundtruth_list.append({"semi_groundtruth": str(tmp)})
    update_cluster_buff(cluster_id_to_update, semi_groundtruth_list, time_stamp, conn)
    conn.commit()
    cursor.execute(f"update {cluster_table} set semi_groundtruth = groundtruth where groundtruth is not null and groundtruth != ''")
    conn.commit()
    conn.close()


def update_semi_groundtruth(time_stamp):
    conn = create_connection()
    cursor = conn.cursor()
    cluster_table = f"cluster_{time_stamp}"
    cate_list = []
    for cate in gemini_cate_des_keys:
        cate = cate.replace("&", "")
        cate = cate.replace("  ", " ")
        cate = cate.replace(" ", "_")
        cate_list.append(cate)
    cursor.execute(f"select cluster_id, {','.join(cate_list)} from {cluster_table} where wiki_text is not null and (groundtruth is null or groundtruth = '')")
    rows = cursor.fetchall()
    data_to_update = []
    for row in rows:
        cluster_id = row[0]
        tmp = []
        for i in range(1, len(row)):
            if row[i] == 1:
                tmp.append(i - 1)
        if len(tmp) > 0:
            data_to_update.append((str(tmp), cluster_id))
    cursor.executemany(f"update {cluster_table} set semi_groundtruth = %s where cluster_id = %s", data_to_update)
    cursor.execute(f"update {cluster_table} set semi_groundtruth = groundtruth where groundtruth is not null and groundtruth != ''")
    conn.commit()
    conn.close()
