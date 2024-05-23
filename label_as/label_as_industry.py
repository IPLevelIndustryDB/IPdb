# get most similar organization name and retrieve wikipedia based on it and label as industry
from sql_operation.db_operate import create_connection, insert_cluster_buff, update_asn_organization_buff
from sentence_transformers import SentenceTransformer
from scipy.spatial.distance import cosine
from tldextract import extract
import tqdm
common_domains = [
    "gmail.com", "yahoo.com", "outlook.com",
    "icloud.com", "aol.com", "zoho.com", "mail.com",
    "yandex.com", "protonmail.com", "gmx.com",
    "apnic.net", "ripe.net", "arin.net", "lacnic.net", "afrinic.net", ""
]


def get_most_similar(asn_name, org_list):
    org_list = set(org_list)
    for domain in common_domains:
        if domain in org_list:
            org_list.remove(domain)
    org_list = list(org_list)
    if len(org_list) == 0:
        return asn_name
    model = SentenceTransformer('/home/ipdb/all-MiniLM-L6-v2')
    org_name_embedding = model.encode(asn_name)
    org_list_embedding = [model.encode(org) for org in org_list]
    similarity = []
    for org in org_list_embedding:
        similarity.append(1 - cosine(org_name_embedding, org))
    return org_list[similarity.index(max(similarity))]


def label_as_industry(time_stamp):
    asn_organization_table = f"asn_organization_{time_stamp}"
    cluster_table = f"cluster_{time_stamp}"
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT asn, organization_list, asn_name FROM {asn_organization_table}")
    asn_organization = cursor.fetchall()
    cursor.execute(f"SELECT cluster_id, wiki_keyword FROM {cluster_table}")
    cluster_wiki = cursor.fetchall()
    cursor.execute(f"SELECT max(cluster_id) FROM {cluster_table}")
    max_cluster_id = cursor.fetchone()[0] + 1
    conn.close()
    org_cluster = {}
    asn_list = []
    update_list = []
    new_cluster = []
    process_bar = tqdm.tqdm(total=len(asn_organization))
    for cluster_id, wiki_keyword in cluster_wiki:
        org_cluster[wiki_keyword] = cluster_id
    for asn, organization_list, asn_name in asn_organization:
        organization_list = eval(organization_list)
        process_bar.update(1)
        most_similar_org = get_most_similar(asn_name, organization_list)
        if extract(most_similar_org).suffix is not None:
            most_similar_org = "*." + most_similar_org
        if most_similar_org in org_cluster:
            asn_list.append(asn)
            update_list.append({"cluster_id": org_cluster[most_similar_org], "organization": most_similar_org})
        else:
            asn_list.append(asn)
            update_list.append({"cluster_id": max_cluster_id, "organization": most_similar_org})
            new_cluster.append((max_cluster_id, most_similar_org))
            max_cluster_id += 1
    process_bar.close()
    conn = create_connection()
    insert_cluster_buff(new_cluster, time_stamp, conn)
    update_asn_organization_buff(asn_list, update_list, time_stamp, conn)
    conn.commit()
    conn.close()
