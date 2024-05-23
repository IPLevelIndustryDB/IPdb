import time

import requests

import dns.resolver

from sql_operation.db_operate import create_connection, insert_cluster_buff, update_url_cert_buff
from tldextract import extract
import whois
import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


def query_whois(url):
    # check valid url
    try:
        dns.resolver.resolve(url, "MX")
    except dns.resolver.NoAnswer:
        return None, None
    except dns.resolver.NXDOMAIN:
        return None, None
    except dns.exception.Timeout:
        return None, None
    except dns.exception.DNSException:
        return None, None
    except Exception:
        return None, None
    try:
        time.sleep(1)
        w = whois.whois(url, command=True)
        return w, url
    except:
        return None, None


def organization_identification_by_whois(time_stamp):
    conn = create_connection()
    cursor = conn.cursor()
    url_cert_table = f"url_cert_{time_stamp}"
    cluster_table = f"cluster_{time_stamp}"
    limit = 10000
    offset = 0
    cursor.execute(f"select count(*) from {url_cert_table} where cluster_id is null")
    total = cursor.fetchone()[0]
    process_bar = tqdm.tqdm(total=total, desc="processing")
    while True:
        cursor.execute(f"select url from {url_cert_table} where cluster_id is null limit {limit} offset {offset}")
        data = cursor.fetchall()
        offset += limit
        if len(data) == 0:
            break
        # get current cluster id
        cursor.execute(f"select max(cluster_id) from {cluster_table}")
        cur_cluster_id = cursor.fetchone()
        cur_cluster_id = cur_cluster_id[0] + 1 if cur_cluster_id[0] is not None else 0
        # get clusters
        cursor.execute(f"select cluster_id, wiki_keyword from {cluster_table}")
        clusters = cursor.fetchall()
        keyword_cluster_id_map = {}
        new_cluster_keyword_list = []
        for c in clusters:
            c_id = c[0]
            keyword = c[1]
            keyword_cluster_id_map[keyword] = c_id
        # cluster new data
        url_list = []
        value_list = []
        with ThreadPoolExecutor(max_workers=500) as executor:
            futures = [executor.submit(query_whois, d[0]) for d in data]
            for future in as_completed(futures):
                process_bar.update(1)
                w, url = future.result()
                if w is None:
                    continue
                on = w.org if w.org is not None else w.name
                if on is None and "registrant_name" in w and w["registrant_name"] is not None:
                    on = w["registrant_name"]
                if on is None or "privacy" in on.lower() or "redacted" in on.lower() or "whoisguard" in on.lower() or "proxy" in on.lower() or "private" in on.lower() or "proxies" in on.lower() or "not disclosed" in on.lower():
                    continue
                # if on is list, choose the first one
                if isinstance(on, list):
                    on = on[0]
                    print(f"on is list: {on}: {url}")
                keyword_ = on
                # determine cluster id
                if keyword_ in keyword_cluster_id_map:
                    d_cluster_id = keyword_cluster_id_map[keyword_]
                else:
                    # new cluster
                    d_cluster_id = cur_cluster_id
                    cur_cluster_id += 1
                    keyword_cluster_id_map[keyword_] = d_cluster_id
                    new_cluster_keyword_list.append((d_cluster_id, keyword_))
                url_list.append(url)
                value_list.append({"cluster_id": d_cluster_id, "cluster_type": "by_whois", "whois_on": on})
        # insert new clusters
        if len(new_cluster_keyword_list) != 0:
            insert_cluster_buff(new_cluster_keyword_list, time_stamp, connection=conn)
        if len(url_list) != 0:
            update_url_cert_buff(url_list, value_list, time_stamp, connection=conn)
        conn.commit()
    conn.close()
    process_bar.close()


def organization_identification_by_cert(time_stamp):
    with open("./organization_cluster/on_blacklist.txt", "r", encoding="utf-8") as f:
        cdn_on_blacklist = [line.strip() for line in f.readlines()]
    with open("./organization_cluster/cdn_sld_blacklist.txt", "r", encoding="utf-8") as f:
        cdn_sld_blacklist = [line.strip() for line in f.readlines()]
        cdn_sld_blacklist = [extract(sld).domain for sld in cdn_sld_blacklist]
        cdn_sld_blacklist = list(set(cdn_sld_blacklist))
    url_cert_table = f"url_cert_{time_stamp}"
    cluster_table = f"cluster_{time_stamp}"
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute(f"select url, cn, organizationName, san from {url_cert_table} "
                          f"where cluster_id is null and organizationName is not null")
    data = cursor.fetchall()
    # get current cluster id
    cursor.execute(f"select max(cluster_id) from {cluster_table}")
    cur_cluster_id = cursor.fetchone()
    cur_cluster_id = cur_cluster_id[0] + 1 if cur_cluster_id[0] is not None else 0
    # get clusters
    cursor.execute(f"select cluster_id, wiki_keyword from {cluster_table}")
    clusters = cursor.fetchall()
    keyword_cluster_id_map = {}
    new_cluster_keyword_list = []

    for c in clusters:
        c_id = c[0]
        keyword = c[1]
        keyword_cluster_id_map[keyword] = c_id

    # cluster new data
    url_list = []
    value_list = []
    process_bar = tqdm.tqdm(total=len(data), desc="processing")
    for d in data:
        process_bar.update(1)
        on = d[2].strip()
        url = d[0]
        url_keyword = extract(url).registered_domain

        if on == "":
            keyword_ = "*." + url_keyword
            cluster_type = "by_cert_url"
        elif on in cdn_on_blacklist and extract(url_keyword).domain in cdn_sld_blacklist:
            # CDN organization
            keyword_ = on
            cluster_type = "by_cert_on"
        elif on not in cdn_on_blacklist:
            # not CDN organization
            keyword_ = on
            cluster_type = "by_cert_on"
        else:
            # not CDN organization, but use CDN cert
            keyword_ = "*." + url_keyword
            cluster_type = "by_cert_url"

        # determine cluster id
        if keyword_ in keyword_cluster_id_map:
            d_cluster_id = keyword_cluster_id_map[keyword_]
        else:
            # new cluster
            d_cluster_id = cur_cluster_id
            cur_cluster_id += 1
            keyword_cluster_id_map[keyword_] = d_cluster_id
            new_cluster_keyword_list.append((d_cluster_id, keyword_))
        # update cluster data
        url_list.append(url)
        value_list.append({"cluster_id": d_cluster_id, "cluster_type": cluster_type})
    # insert new clusters
    print(f"new cluster count: {len(new_cluster_keyword_list)}")
    insert_cluster_buff(new_cluster_keyword_list, time_stamp, connection=conn)
    for i in range(0, len(url_list), 10000):
        update_url_cert_buff(url_list[i:i + 10000], value_list[i:i + 10000], time_stamp, connection=conn)
        conn.commit()
    # update_url_cert_buff(url_list, value_list, time_stamp, connection=conn)
    process_bar.close()
    conn.close()
