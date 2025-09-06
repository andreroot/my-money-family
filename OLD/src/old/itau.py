# from itauscraper import ItauScraper

# itau = ItauScraper(agencia='9293', conta='22915', digito='0', senha='401471')
# itau.login()
# print(itau.extrato())
# print(itau.cartao())
# # TODO: Divirta-se!

# from pyitau import Itau

# # Login
# itau = Itau(agency='9293', account='22915', account_digit='0', password='401471')
# print(itau)
# itau.authenticate()

# itau.get_statements()

import requests
from cached_property import cached_property

from pyitau import pages

ROUTER_URL = "https://internetpf5.itau.com.br/router-app/router" #https://internetpf5.itau.com.br/router-app/router#30horas

def _authenticate2():
    agency='9293'
    account='22915'
    account_digit='0'
    password='401471'

    _session = requests.Session()
    _session.headers = {
        **_session.headers,
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Ubuntu Chromium/72.0.3626.121 "
            "Chrome/72.0.3626.121 Safari/537.36"
        ),
    }
    
    response = _session.post(
        ROUTER_URL,
        data={
            "portal": "005",
            "pre-login": "pre-login",
            "tipoLogon": "7",
            "usuario.agencia": agency,
            "usuario.conta": account,
            "usuario.dac": account_digit,
            "destino": "",
        },
    )
    page = pages.FirstRouter(response.text)
    print(response.text)
    _session.cookies.set("X-AUTH-TOKEN", page.auth_token)
    # self._op2 = page.secapdk
    # self._op3 = page.secbcatch
    # self._op4 = page.perform_request
    # self._flow_id = page.flow_id
    # self._client_id = page.client_id

_authenticate2()