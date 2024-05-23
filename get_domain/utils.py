import ipaddress


def is_valid_ip(address):
    try:
        ipaddress.IPv6Address(address)
        return True
    except ipaddress.AddressValueError:
        try:
            ipaddress.IPv4Address(address)
            return True
        except ipaddress.AddressValueError:
            print(f'Invalid IP address: {address}')
            return False
