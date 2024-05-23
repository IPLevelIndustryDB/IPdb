import ssl
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import OpenSSL
from urllib.parse import urlparse
from tqdm import tqdm
from sql_operation.db_operate import update_url_cert_buff, create_connection
from organization_cluster.org_identification import organization_identification_by_cert

port = 443
lock = threading.Lock()
lock_results = threading.Lock()


def worker(host_):
    # host_ = urlparse(to_get[0]).netloc
    try:
        cert = ssl.get_server_certificate((host_, port), ca_certs=None, timeout=20)
        if not cert:
            print("get Certificate: None" + " " + host_)
            return
        x509 = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, cert)
        # get org info
        flag = False
        on = ""
        cn = ""
        san = ""
        # ON: organizationName
        if x509.get_subject().organizationName:
            flag = True
            on = x509.get_subject().organizationName
        # CN: commonName
        if x509.get_subject().commonName:
            flag = True
            cn = x509.get_subject().commonName
        # SAN: subjectAltName
        ext_cnt = x509.get_extension_count()
        for i in range(ext_cnt):
            ext = x509.get_extension(i)
            if ext.get_short_name() == b"subjectAltName":
                san = str(ext)
                if cn == "":
                    cn = san.split("DNS:")[1].split(",")[0]
                flag = True
                break
        if flag:
            return host_, cn, on, san
        else:
            # print("get subjectInfo: None" + " " + host_)
            return host_, None, None, None
    except Exception as e:
        # print(str(e) + " " + host_)
        return host_, None, None, None


def get_cert(time_stamp):
    conn = create_connection()
    cursor = conn.cursor()
    limit = 100000
    offset = 0
    cursor.execute(f"SELECT COUNT(*) FROM url_cert_{time_stamp} WHERE cn IS NULL AND cluster_id IS NULL")
    tot = cursor.fetchone()[0]
    progress_bar = tqdm(total=tot, desc="processing")
    while True:
        cnt = 0
        res_urls = []
        res_values = []
        cursor.execute(f"SELECT url FROM url_cert_{time_stamp} WHERE cn IS NULL AND cluster_id IS NULL LIMIT {limit} OFFSET {offset}")
        offset += limit
        results_to_get = cursor.fetchall()
        if len(results_to_get) == 0:
            break
        with ThreadPoolExecutor(max_workers=300) as executor:
            futures = [executor.submit(worker, to_get[0]) for to_get in results_to_get]
            for future in as_completed(futures):
                progress_bar.update(1)
                res = future.result()
                if res[1] is not None:
                    res_urls.append(res[0])
                    res_values.append({"cn": res[1], "organizationName": res[2], "san": res[3]})
                    cnt += 1
        print(f"valid count: {cnt} / {limit}, percentage: {cnt / limit * 100:.2f}%")
        if len(res_urls) == 0:
            continue
        update_url_cert_buff(urls=res_urls, update_values=res_values, time_stamp=time_stamp, connection=conn)
        conn.commit()
    conn.close()
    progress_bar.close()
