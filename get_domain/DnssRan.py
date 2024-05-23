import dns.resolver
import dns.reversename
import concurrent.futures
from tqdm import tqdm

from get_domain.utils import is_valid_ip
from sql_operation.db_operate import insert_ip_url_buff, create_connection
from tldextract import extract
from threading import Lock
import pyasn

tmux = Lock()

dns_server = '8.8.8.8'
start_ipv6 = ''


def get_ptr(ip):
    try:
        resolver = dns.resolver.Resolver()
        resolver.nameservers = [dns_server]
        rev_name = dns.reversename.from_address(ip)
        ptr_name = str(resolver.resolve(rev_name, "PTR")[0])
        return ptr_name
    except (dns.resolver.NXDOMAIN, dns.resolver.Timeout, dns.exception.SyntaxError):
        return None
    except Exception as exc:
        # print(f'Error: {exc}')
        return None


def process_batch(ipv6_addresses, asndb, time_stamp, batch_count, conn):
    results = []
    with tqdm(total=len(ipv6_addresses), desc=f"Batch {batch_count}") as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
            future_to_ip = {executor.submit(get_ptr, ip): ip for ip in ipv6_addresses}
            for future in concurrent.futures.as_completed(future_to_ip):
                ip = future_to_ip[future]
                try:
                    ptr_record = future.result()
                    if ptr_record is not None:
                        sld = extract(ptr_record).domain + '.' + extract(ptr_record).suffix
                        asn, bgp = asndb.lookup(ip)
                        results.append((ip, sld, ptr_record, bgp, asn, "IPv6_PTR_public_hitlist"))
                except Exception as exc:
                    continue
                pbar.update(1)  # 更新进度条

    # 将结果写入到磁盘s
    insert_ip_url_buff(results, time_stamp, conn)
    conn.commit()
    print(f"valid count: {len(results)} / {len(ipv6_addresses)}, percentage: {len(results) / len(ipv6_addresses) * 100:.2f}%")


def ipv6_to_ptr(file_name, asn_file, time_stamp, batch_size=10000):
    asndb = pyasn.pyasn(asn_file)
    ipv6_addresses = []
    batch_count = 0
    start = False
    start = True
    conn = create_connection()
    with open(file_name, 'r') as f:
        while True:
            line = f.readline()
            if not line:
                break
            line = line.strip()
            # if line == start_ipv6:
            #     start = True
            if start:
                if is_valid_ip(line):
                    ipv6_addresses.append(line)
                if len(ipv6_addresses) == batch_size:
                    batch_count += 1
                    print(f"Processing batch {batch_count} with {len(ipv6_addresses)} addresses..., end of batch: {line}")
                    process_batch(ipv6_addresses, asndb, time_stamp, batch_count, conn)
                    ipv6_addresses = []
        if ipv6_addresses:
            batch_count += 1
            print(f"Processing final batch {batch_count} with {len(ipv6_addresses)} addresses..., end of batch: {line}")
            process_batch(ipv6_addresses, asndb, time_stamp, batch_count, conn)
    conn.close()

# ipv6_to_ptr('/home/sgl/pynic/Data/adm/hitlist-2.txt', '/home/sgl/pynic/Data/adm/amhit-ptr.txt', batch_size=100000)
