import os
import sys

# 获取当前文件的父目录路径
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

# 将父目录添加到 sys.path
sys.path.append(parent_dir)

import pyasn
from sql_operation.db_operate import create_connection


def add_asn(asn_file, time_stamp):
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute(f"select ip, url from ipdb.ip_url_{time_stamp} where asn is NULL")
    rows = cursor.fetchall()
    print(f"total {len(rows)}")
    asndb = pyasn.pyasn(asn_file)
    buff = []
    for row in rows:
        ip = row[0]
        url = row[1]
        asn, bgp = asndb.lookup(ip)
        if asn is None:
            continue
        buff.append((asn, bgp, ip, url))
        if len(buff) >= 100000:
            cursor.executemany(f"update ipdb.ip_url_{time_stamp} set asn=%s, bgp=%s where ip=%s and url=%s", buff)
            conn.commit()
            buff = []
    if len(buff) > 0:
        cursor.executemany(f"update ipdb.ip_url_{time_stamp} set asn=%s, bgp=%s where ip=%s and url=%s", buff)
    conn.commit()
    conn.close()


if __name__ == '__main__':
    add_asn("/home/ipdb/ip_asn_2024_06_28.dat", "2024_06")
