import sqlite3
import pymysql
from params import db_path, db_host, db_user, db_password, db_port


def in_list(l_, item):
    return item in eval(l_)


def create_table(time_stamp):
    conn = create_connection()
    c = conn.cursor()
    ip_url_table = f"ip_url_{time_stamp}"
    url_cert_table = f"url_cert_{time_stamp}"
    cluster_table = f"cluster_{time_stamp}"
    # on_list: [{on:cnt},{}], sld_list: [{sld:cnt},{}]
    c.execute(f'''CREATE TABLE IF NOT EXISTS {cluster_table} (
    cluster_id       INTEGER PRIMARY KEY,
    wiki_keyword     TEXT,
    wiki_text        TEXT,
    wiki_text_en     TEXT,
    gpt_label        TEXT,
    gemini_label     TEXT,
    glm_label        TEXT,
    llama_label      TEXT,
    mistral_label    TEXT,
    groundtruth      TEXT,
    semi_groundtruth TEXT
    )''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS {url_cert_table} (
    url              TEXT    NOT NULL,
    cn               TEXT,
    organizationName TEXT,
    san              LONGTEXT,
    whois_on         TEXT,
    cluster_type     TEXT,
    cluster_id       INTEGER REFERENCES {cluster_table}(cluster_id),
    title            TEXT,
    main_page_text   TEXT,
    inner_page_text  TEXT,
    meta_des         TEXT,
    url_label        TEXT,
    PRIMARY KEY (url(64))
    )''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS {ip_url_table} (
    ip  TEXT(40) NOT NULL,
    url TEXT NOT NULL REFERENCES {url_cert_table} (url),
    full_domain TEXT NOT NULL,
    bgp TEXT,
    asn TEXT,
    source TEXT,
    PRIMARY KEY (ip(40),full_domain(64))
    )''')

    conn.commit()
    conn.close()


def create_connection():
    try:
    # conn = sqlite3.connect(db_path)
        conn = pymysql.connect(host=db_host,
                            user=db_user,
                            port=db_port,
                            password=db_password,
                            database=db_path,
                            autocommit=False)
    # cursor = conn.cursor()
    # cursor.execute("SET autocommit = 0")
    # conn.commit()
    # cursor.execute("select @@autocommit")
    # print(cursor.fetchall())
    except pymysql.err.OperationalError as e:
        print(f"Error connecting to MySQL: {e}")
        exit(1)
    return conn


def insert_ip_url(ip, sld, full_domain, bgp, asn, source, time_stamp, connection=None):
    """
    :param ip:
    :param sld:
    :param full_domain:
    :param time_stamp:
    :param connection:
    :return:
    if connection is not None, commit and close connection should be done outside
    """
    if connection is None:
        conn = create_connection()
    else:
        conn = connection
    c = conn.cursor()
    ip_url_table = f"ip_url_{time_stamp}"
    url_cert_table = f"url_cert_{time_stamp}"
    c.execute(f"INSERT IGNORE INTO {url_cert_table} (url) VALUES (%s)", (sld,))
    c.execute(f"INSERT IGNORE INTO {ip_url_table} (ip, url, full_domain, bgp, asn, source) VALUES (%s, %s, %s, %s, %s, %s)",
              (ip, sld, full_domain, bgp, asn, source))
    if connection is None:
        conn.commit()
        conn.close()


def insert_ip_url_buff(ip_url_list, time_stamp, connection=None):
    """
    :param ip_url_list: [(ip, sld, full_domain, bgp, asn, source), ...]
    :param time_stamp:
    :param connection:
    :return:
    if connection is not None, commit and close connection should be done outside
    """
    if connection is None:
        conn = create_connection()
    else:
        conn = connection
    c = conn.cursor()
    ip_url_table = f"ip_url_{time_stamp}"
    url_cert_table = f"url_cert_{time_stamp}"
    c.executemany(f"INSERT IGNORE INTO {url_cert_table} (url) VALUES (%s)", [item[1] for item in ip_url_list])
    c.executemany(f"INSERT IGNORE INTO {ip_url_table} (ip, url, full_domain, bgp, asn, source) VALUES (%s, %s, %s, %s, %s, %s)",
                    ip_url_list)
    if connection is None:
        conn.commit()
        conn.close()


def update_url_cert(url, time_stamp, cn=None, organizationName=None, san=None, cluster_id=None, title=None, main_page_text=None,
                    inner_page_text=None, meta_des=None, url_label=None, connection=None):
    """
    :param url:
    :param time_stamp:
    :param cn:
    :param organizationName:
    :param san:
    :param cluster_id:
    :param title:
    :param main_page_text:
    :param inner_page_text:
    :param meta_des:
    :param url_label:
    :param connection:
    :return:
    if connection is not None, commit and close connection should be done outside
    """
    if connection is None:
        conn = create_connection()
    else:
        conn = connection
    c = conn.cursor()
    url_cert_table = f"url_cert_{time_stamp}"
    update_values = {k: v for k, v in locals().items() if v is not None and k != 'url' and k != 'time_stamp' and k != 'connection'}
    set_clause = ", ".join([f"{k} = %s" for k in update_values.keys()])
    values = tuple(update_values.values()) + (url,)
    c.execute(f"UPDATE {url_cert_table} SET {set_clause} WHERE url = %s", values)
    if connection is None:
        conn.commit()
        conn.close()


def update_url_cert_buff(urls, update_values, time_stamp, connection=None):
    """
    :param urls: list of urls
    :param update_values: list of dict of update values, key should be in ['cn', 'organizationName', 'san', 'whois_on',
        'cluster_type', 'cluster_id','title', 'main_page_text', 'inner_page_text', 'meta_des', 'url_label']
    :param time_stamp:
    :param connection:
    :return:
    if connection is not None, commit and close connection should be done outside
    key is determined by the first dict in update_values
    """
    if connection is None:
        conn = create_connection()
    else:
        conn = connection
    c = conn.cursor()
    url_cert_table = f"url_cert_{time_stamp}"
    set_clause = ", ".join([f"{k} = %s" for k in update_values[0].keys()])
    values = []
    for i in range(len(urls)):
        values.append(tuple([update_values[i][k] for k in update_values[0].keys()]) + (urls[i],))
    c.executemany(f"UPDATE {url_cert_table} SET {set_clause} WHERE url = %s", values)
    if connection is None:
        conn.commit()
        conn.close()


def update_webpage(urls, update_values, time_stamp, connection=None):
    """
    :param urls: list of urls
    :param update_values: list of dict of update values, key should be in ['title', 'main_page_text', 'inner_page_text', 'meta_des', 'label', 'label_type']
    :param time_stamp:
    :param connection:
    :return:
    if connection is not None, commit and close connection should be done outside
    key is determined by the first dict in update_values
    """
    if connection is None:
        conn = create_connection()
    else:
        conn = connection
    c = conn.cursor()
    url_cert_table = f"webpage_{time_stamp}"
    set_clause = ", ".join([f"{k} = %s" for k in update_values[0].keys()])
    values = []
    for i in range(len(urls)):
        values.append(tuple([update_values[i][k] for k in update_values[0].keys()]) + (urls[i],))
    c.executemany(f"UPDATE {url_cert_table} SET {set_clause} WHERE url = %s", values)
    # c.execute("create table if not exists webpage_2024_06 (url varchar(255) primary key, title text, main_page_text text, inner_page_text text, meta_des text, label text, label_type text)")
    if connection is None:
        conn.commit()
        conn.close()


def update_cluster_buff(cluster_id_list, update_values, time_stamp, connection=None):
    """
    :param cluster_id_list: list of cluster_id
    :param update_values: list of dict of update values, key should be in ['wiki_keyword', 'wiki_text',
        'wiki_text_en', 'gpt_label', 'gemini_label', 'glm_label', 'llama_label', 'mistral_label', 'groundtruth', 'semi_groundtruth']
    :param time_stamp:
    :param connection:
    :return:
    if connection is not None, commit and close connection should be done outside
    key is determined by the first dict in update_values
    """
    if connection is None:
        conn = create_connection()
    else:
        conn = connection
    c = conn.cursor()
    cluster_table = f"cluster_{time_stamp}"
    set_clause = ", ".join([f"{k} = %s" for k in update_values[0].keys()])
    values = []
    for i in range(len(cluster_id_list)):
        values.append(tuple([update_values[i][k] for k in update_values[0].keys()]) + (cluster_id_list[i],))
    c.executemany(f"UPDATE {cluster_table} SET {set_clause} WHERE cluster_id = %s", values)
    if connection is None:
        conn.commit()
        conn.close()


def update_cluster(cluster_id, time_stamp, on_list=None, sld_list=None, wiki_keyword=None, wiki_text=None,
                   wiki_text_en=None, gpt_label=None, gemini_label=None, glm_label=None, llama_label=None,
                   mistral_label=None, ground_truth=None, semi_groundtruth=None, connection=None):
    """
    :param cluster_id:
    :param time_stamp:
    :param on_list:
    :param sld_list:
    :param wiki_keyword:
    :param wiki_text:
    :param wiki_text_en:
    :param gpt_label:
    :param gemini_label:
    :param glm_label:
    :param llama_label:
    :param mistral_label:
    :param ground_truth:
    :param semi_groundtruth:
    :param connection:
    :return:
    if connection is not None, commit and close connection should be done outside
    """
    if connection is None:
        conn = create_connection()
    else:
        conn = connection
    c = conn.cursor()
    cluster_table = f"cluster_{time_stamp}"
    if on_list is not None:
        c.execute(f"UPDATE {cluster_table} SET on_list = %s WHERE cluster_id = %s", (str(on_list), cluster_id))
    if sld_list is not None:
        c.execute(f"UPDATE {cluster_table} SET sld_list = %s WHERE cluster_id = %s", (str(sld_list), cluster_id))
    if wiki_keyword is not None:
        c.execute(f"UPDATE {cluster_table} SET wiki_keyword = %s WHERE cluster_id = %s", (str(wiki_keyword), cluster_id))
    if wiki_text is not None:
        c.execute(f"UPDATE {cluster_table} SET wiki_text = %s WHERE cluster_id = %s", (str(wiki_text), cluster_id))
    if wiki_text_en is not None:
        c.execute(f"UPDATE {cluster_table} SET wiki_text_en = %s WHERE cluster_id = %s", (str(wiki_text_en), cluster_id))
    if gpt_label is not None:
        c.execute(f"UPDATE {cluster_table} SET gpt_label = %s WHERE cluster_id = %s", (str(gpt_label), cluster_id))
    if gemini_label is not None:
        c.execute(f"UPDATE {cluster_table} SET gemini_label = %s WHERE cluster_id = %s", (str(gemini_label), cluster_id))
    if glm_label is not None:
        c.execute(f"UPDATE {cluster_table} SET glm_label = %s WHERE cluster_id = %s", (str(glm_label), cluster_id))
    if llama_label is not None:
        c.execute(f"UPDATE {cluster_table} SET llama_label = %s WHERE cluster_id = %s", (str(llama_label), cluster_id))
    if mistral_label is not None:
        c.execute(f"UPDATE {cluster_table} SET mistral_label = %s WHERE cluster_id = %s",
                  (str(mistral_label), cluster_id))
    if ground_truth is not None:
        c.execute(f"UPDATE {cluster_table} SET groundtruth = %s WHERE cluster_id = %s", (str(ground_truth), cluster_id))
    if semi_groundtruth is not None:
        c.execute(f"UPDATE {cluster_table} SET semi_groundtruth = %s WHERE cluster_id = %s",
                  (str(semi_groundtruth), cluster_id))
    if connection is None:
        conn.commit()
        conn.close()


def select_table(table_name, condition=None, offset=None, limit=None, connection=None):
    """
    :param table_name:
    :param condition:
    :param offset:
    :param limit:
    :param connection:
    :return: tuple list of selected results
    if connection is not None, close connection should be done outside
    """
    if connection is None:
        conn = create_connection()
    else:
        conn = connection
    c = conn.cursor()
    if not condition and offset is None and limit is None:
        c.execute(f"SELECT * FROM {table_name} ")
    elif condition and offset is None and limit is None:
        c.execute(f"SELECT * FROM {table_name} WHERE {condition}")
    elif condition and offset is not None and limit is not None:
        c.execute(f"SELECT * FROM {table_name} WHERE {condition} LIMIT {limit} OFFSET {offset}")
    elif not condition and offset is not None and limit is not None:
        c.execute(f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}")
    results = c.fetchall()
    if connection is None:
        conn.close()
    return results


def insert_cluster(cluster_id, on_list, sld_list, time_stamp, connection=None):
    """
    :param cluster_id:
    :param on_list:
    :param sld_list:
    :param time_stamp:
    :param connection:
    :return: None
    if connection is not None, commit and close connection should be done outside
    """
    if connection is None:
        conn = create_connection()
    else:
        conn = connection
    c = conn.cursor()
    cluster_table = f"cluster_{time_stamp}"
    c.execute(f"INSERT IGNORE INTO {cluster_table} (cluster_id, on_list, sld_list) VALUES (%s, %s, %s)",
              (cluster_id, str(on_list), str(sld_list)))
    if connection is None:
        conn.commit()
        conn.close()


def insert_cluster_buff(cluster_keyword_list, time_stamp, connection=None):
    """
    if connection is not None, commit and close connection should be done outside
    :param cluster_keyword_list: [(cluster_id, keyword), ...]
    :param time_stamp:
    :param connection:
    :return:
    """
    if connection is None:
        conn = create_connection()
    else:
        conn = connection
    c = conn.cursor()
    cluster_table = f"cluster_{time_stamp}"
    c.executemany(f"INSERT IGNORE INTO {cluster_table} (cluster_id, wiki_keyword) VALUES (%s, %s)", cluster_keyword_list)
    if connection is None:
        conn.commit()
        conn.close()


def update_url_cert_cluster_buff(cluster_url_list, time_stamp, connection=None):
    """
    :param cluster_url_list: [(cluster_id, url)]
    :param time_stamp:
    :param connection:
    :return:
    if connection is not None, commit and close connection should be done outside
    """
    if connection is None:
        conn = create_connection()
    else:
        conn = connection
    c = conn.cursor()
    url_cert_table = f"url_cert_{time_stamp}"
    c.executemany(f"UPDATE {url_cert_table} SET cluster_id = %s, cluster_type = %s WHERE url = %s", cluster_url_list)
    if connection is None:
        conn.commit()
        conn.close()


def combine_cluster(cluster_id_before, cluster_id_after, time_stamp, connection=None):
    if connection is None:
        conn = create_connection()
    else:
        conn = connection
    c = conn.cursor()
    cluster_table = f"cluster_{time_stamp}"
    url_cert_table = f"url_cert_{time_stamp}"
    c.execute(f"UPDATE {url_cert_table} SET cluster_id = %s WHERE cluster_id = %s", (cluster_id_after, cluster_id_before))
    c.execute(f"Select cluster_id, on_list, sld_list from {cluster_table} where cluster_id = %s", (cluster_id_before,))
    results = c.fetchone()
    if not results:
        return
    on_list = eval(results[1]) if results[1] else {}
    sld_list = eval(results[2]) if results[2] else {}
    c.execute(f"Select cluster_id, on_list, sld_list from {cluster_table} where cluster_id = %s", (cluster_id_after,))
    results = c.fetchone()
    if not results:
        return
    on_list_after = eval(results[1]) if results[1] else {}
    sld_list_after = eval(results[2]) if results[2] else {}
    for on, cnt in on_list:
        if on not in on_list_after.keys():
            on_list_after[on] = cnt
        else:
            on_list_after[on] += cnt
    for sld, cnt in sld_list:
        if sld not in sld_list_after.keys():
            sld_list_after[sld] = cnt
        else:
            sld_list_after[sld] += cnt
    # update cluster on_list, sld_list, set wiki_keyword to None, re-label the cluster
    c.execute(f"UPDATE {cluster_table} SET on_list = %s, sld_list = %s, wiki_keyword = NULL WHERE cluster_id = %s",
              (str(on_list_after), str(sld_list_after), cluster_id_after))
    c.execute(f"DELETE FROM {cluster_table} WHERE cluster_id = %s", (cluster_id_before,))
    if connection is None:
        conn.commit()
        conn.close()


def select_url_cert_by_on(on, time_stamp):
    conn = create_connection()
    c = conn.cursor()
    url_cert_table = f"url_cert_{time_stamp}"
    c.execute(f"SELECT * FROM {url_cert_table} WHERE organizationName = %s", (on,))
    results = c.fetchall()
    conn.close()
    return results


def insert_into_asn_organization_buff(asn_value_list, time_stamp):
    '''
    :param asn_value_list: [(asn, organization_list, domain, asn_name), ...]
    :param time_stamp:
    :return:
    '''
    conn = create_connection()
    c = conn.cursor()
    asn_organization_table = f"asn_organization_{time_stamp}"
    c.executemany(f"INSERT IGNORE INTO {asn_organization_table} (asn, organization_list, domain, asn_name) VALUES (%s, %s, %s, %s)", asn_value_list)
    conn.commit()
    conn.close()


def update_asn_organization_buff(asn_list, update_values, time_stamp, connection=None):
    """
    :param connection:
    :param asn_list: list of asn
    :param update_values: list of dict of update values, key should be in ['organization_list', 'domain', 'asn_name', 'cluster_id', 'organization']
    :param time_stamp:
    :return:
    """
    if connection is None:
        conn = create_connection()
    else:
        conn = connection
    c = conn.cursor()
    asn_organization_table = f"asn_organization_{time_stamp}"
    set_clause = ", ".join([f"{k} = %s" for k in update_values[0].keys()])
    values = []
    for i in range(len(asn_list)):
        values.append(tuple([update_values[i][k] for k in update_values[0].keys()]) + (asn_list[i],))
    c.executemany(f"UPDATE {asn_organization_table} SET {set_clause} WHERE asn = %s", values)
    if connection is None:
        conn.commit()
        conn.close()
