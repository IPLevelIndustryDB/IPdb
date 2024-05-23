from sql_operation.db_operate import create_connection


def copy_cluster(time_stamp, copy_cluster_from):
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute(f"INSERT IGNORE INTO cluster_{time_stamp} "
                   f"(cluster_id, wiki_keyword, wiki_text, wiki_text_en, gpt_label, gemini_label, glm_label, llama_label, mistral_label, groundtruth, semi_groundtruth) "
                   f"SELECT cluster_id, wiki_keyword, wiki_text, wiki_text_en, gpt_label, gemini_label, glm_label, llama_label, mistral_label, groundtruth, semi_groundtruth "
                   f"FROM cluster_{copy_cluster_from} where semi_groundtruth is not null and semi_groundtruth != ''")
    conn.commit()
    conn.close()


def clean_cluster(time_stamp):
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM cluster_{time_stamp} WHERE wiki_text is NULL OR wiki_text = ''")
    cursor.execute(f"update url_cert_{time_stamp} set cluster_id = NULL where cluster_id not in (select cluster_id from cluster_{time_stamp})")
    conn.commit()
    conn.close()
