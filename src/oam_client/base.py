import ssl
from typing import Optional
from abc import ABC


class BrokerClientBase(ABC):
    url: str
    ssl_context: ssl.SSLContext

    def __init__(
            self,
            url: str,
            keylog_filename: Optional[str] = None,
            verify: bool = True
    ):
        self.url = url

        self.ssl_context = ssl.create_default_context()
        self.ssl_context.keylog_filename = keylog_filename
        if not verify:
            self.ssl_context.check_hostname = False
            self.ssl_context.verify_mode = ssl.CERT_NONE
