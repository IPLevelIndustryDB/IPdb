import re

import requests
from tldextract import extract
from organization_cluster.org_identification import query_whois
from scraper.get_cert import worker as get_cert_worker
from concurrent.futures import ThreadPoolExecutor, as_completed
import tqdm

from sql_operation.db_operate import create_connection, insert_into_asn_organization_buff

common_domains = [
    "gmail.com", "yahoo.com", "outlook.com",
    "icloud.com", "aol.com", "zoho.com", "mail.com",
    "yandex.com", "protonmail.com", "gmx.com",
    "apnic.net", "ripe.net", "arin.net", "lacnic.net", "afrinic.net"
]


def get_as_whois(asn):
    """
    Get whois information for an ASN from RIR
    """
    url_list = ["https://rdap.db.ripe.net/autnum/", "https://rdap.arin.net/registry/autnum/",
                "https://rdap.apnic.net/autnum/", "https://rdap.afrinic.net/rdap/autnum/",
                "https://rdap.lacnic.net/rdap/autnum/"]
    for url in url_list:
        try:
            response = requests.get(url + str(asn))
            if response.status_code == 200:
                return asn, response.json()
        except Exception as e:
            continue
    return None, None


def extract_domain(text):
    domains = []
    # 正则表达式匹配网址域名
    url_pattern = r'(?:https?://)?(?:www\.)?([a-zA-Z0-9-]+\.[a-zA-Z0-9.-]+)'
    # 正则表达式匹配邮箱域名
    email_pattern = r'[a-zA-Z0-9._%+-]+@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
    # 提取网址域名
    url_domains = re.findall(url_pattern, text)
    # 提取邮箱域名
    email_domains = re.findall(email_pattern, text)
    for domain in url_domains:
        main_domain = extract(domain).registered_domain
        if main_domain != '':
            domains.append(main_domain)
    for domain in email_domains:
        main_domain = extract(domain).registered_domain
        if main_domain != '':
            domains.append(main_domain)
    return list(set(domains))


def extract_entity(data):
    as_name = data.get('name', 'Unknown AS Name')

    # 提取实体信息
    entities = data.get('entities', [])
    entity_names = []

    for entity in entities:
        vcard = entity.get('vcardArray', [])
        if len(vcard) > 1:
            for item in vcard[1]:
                if item[0] == 'fn':  # 找到实体名称
                    entity_names.append(item[3])
    return as_name, entity_names


def get_as_domain(asn_list):
    process_bar = tqdm.tqdm(total=len(asn_list), desc="getting domain")
    domain_counts = {}
    asn_domain = {}
    asn_entity = {}
    thread_pool = ThreadPoolExecutor(max_workers=50)
    futures = [thread_pool.submit(get_as_whois, asn) for asn in asn_list]
    for future in as_completed(futures):
        asn, as_info = future.result()
        process_bar.update(1)
        if as_info is None:
            continue
        domain_list = extract_domain(str(as_info))
        for domain in domain_list:
            if domain not in domain_counts:
                domain_counts[domain] = 0
            domain_counts[domain] += 1
        asn_domain[asn] = domain_list
        as_name, entity_names = extract_entity(as_info)
        if len(entity_names) > 0:
            asn_entity[asn] = entity_names[0]
        elif as_name != 'Unknown AS Name':
            asn_entity[asn] = as_name

    for asn in asn_domain:
        domain_list = asn_domain[asn]
        for domain in domain_list:
            if len(domain_list) > 1:
                if domain_counts[domain] > 100:
                    domain_list.remove(domain)
                elif domain in common_domains:
                    domain_list.remove(domain)
        asn_domain[asn] = domain_list
    process_bar.close()
    thread_pool.shutdown()
    return asn_domain, asn_entity


def get_cert_wrapper(asn, domain):
    host_, cn, on, san = get_cert_worker(domain)
    return asn, on


def get_whois_wrapper(asn, domain):
    w, _ = query_whois(domain)
    return asn, w


def get_as_organization(asn_list, time_stamp):
    asn_domain, asn_name = get_as_domain(asn_list)
    asn_organization = {}
    threadpool = ThreadPoolExecutor(max_workers=50)
    for asn in asn_domain:
        asn_organization[asn] = asn_domain[asn]
    # get cert
    futures = [threadpool.submit(get_cert_wrapper, asn, domain) for asn in asn_domain for domain in asn_domain[asn]]
    process_bar = tqdm.tqdm(total=sum([len(asn_domain[asn]) for asn in asn_domain]), desc="getting organization by cert")
    for future in as_completed(futures):
        asn, on = future.result()
        process_bar.update(1)
        if on is not None:
            asn_organization[asn].append(on)
    process_bar.close()
    # query whois
    futures = [threadpool.submit(get_whois_wrapper, asn, domain) for asn in asn_domain for domain in asn_domain[asn]]
    process_bar = tqdm.tqdm(total=sum([len(asn_domain[asn]) for asn in asn_domain]), desc="getting organization by whois")
    for future in as_completed(futures):
        asn, w = future.result()
        process_bar.update(1)
        if w is not None:
            on = w.org if w.org is not None else w.name
            if on is None and "registrant_name" in w and w["registrant_name"] is not None:
                on = w["registrant_name"]
            if on is None or "privacy" in on.lower() or "redacted" in on.lower() or "whoisguard" in on.lower() or "proxy" in on.lower() or "private" in on.lower() or "proxies" in on.lower() or "not disclosed" in on.lower():
                continue
            # if on is list, choose the first one
            if isinstance(on, list):
                asn_organization[asn].extend(on)
            else:
                asn_organization[asn].append(on)
    for asn in asn_organization:
        asn_organization[asn] = list(set(asn_organization[asn]))
    process_bar.close()
    asn_value_list = []
    for asn in asn_organization:
        asn_value_list.append((asn, str(asn_organization[asn]), str(asn_domain[asn]), str(asn_name[asn])))
    insert_into_asn_organization_buff(asn_value_list, time_stamp)


def label_as(time_stamp):
    ip_as_industry_table = f"ip_asn_industry_{time_stamp}"
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute(f"select distinct asn from {ip_as_industry_table}")
    asn_list_ = cursor.fetchall()
    asn_list_ = [asn[0] for asn in asn_list_]
    get_as_organization(asn_list_, time_stamp)
