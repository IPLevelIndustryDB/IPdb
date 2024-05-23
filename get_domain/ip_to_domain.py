import pyasn
from tldextract import extract
from sql_operation.db_operate import insert_ip_url_buff, create_connection
from tqdm import tqdm


def insert_ip_url(ip_url_file, asn_file, time_stamp):
    """
    Insert ip to domain mapping in database
    :param ip_url_file: file containing ip to domain mapping
    :param time_stamp: time stamp
    :return: None
    """
    asndb = pyasn.pyasn(asn_file)
    conn = create_connection()
    progress_bar = tqdm(total=2000000000, desc="proccess")
    batch_size = 100000
    cnt = 0
    buff = []
    with open(ip_url_file, "r") as f:
        for line in f:
            data = eval(line)
            ip = data["ip"]
            progress_bar.update(1)
            for url in data["ptr"]:
                if extract(url).suffix == "" or extract(url).domain == "": # invalid domain
                    continue
                sld = extract(url).domain + "." + extract(url).suffix
                asn, bgp = asndb.lookup(ip)
                buff.append((ip, sld, url, bgp, asn, "IPv4_PTR_ipsniper"))
                cnt += 1
                if len(buff) >= batch_size:
                    insert_ip_url_buff(buff, time_stamp, conn)
                    buff = []
                    conn.commit()
    if len(buff) > 0:
        insert_ip_url_buff(buff, conn)
        conn.commit()
    conn.close()
    progress_bar.close()



