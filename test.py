import requests
import socks

proxy_host = 'localhost'
proxy_port = 4444

socks.set_default_proxy(socks.SOCKS5, proxy_host, proxy_port)
socket = socks.socksocket

#proxy_servs = {"https":"127.0.0.1:4444"}
x = requests.get(url="https://rfgf.ru", proxies={'https': 'socks5://localhost:4444'}, verify=False)

print(x)
