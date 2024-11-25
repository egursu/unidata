from paramiko import SSHClient, SFTPClient, AutoAddPolicy 
from urllib.parse import ParseResult, urlsplit, parse_qsl


class SSH:
    url: str
    scheme: str
    hostname: str
    port: int
    username: str
    password: str
    path: str
    query: dict
    ssh: SSHClient
    _sftp: SFTPClient = None

    def __init__(self, url: str, *args, **kwargs):
        self.url = url
        url: ParseResult = urlsplit(url)
        self.scheme = url.scheme.lower()
        if self.scheme not in ("ssh", "ftp", "sftp", "scp"):
            raise ValueError(f'Unknown scheme "{self.scheme}", aborting.')
        self.hostname = url.hostname
        self.port = url.port or 22
        self.username = url.username
        self.password = url.password
        self.path = url.path
        self.query = {item[0].lower(): item[1] for item in parse_qsl(url.query)}
        self.ssh = SSHClient()
        self.ssh.set_missing_host_key_policy(AutoAddPolicy())
        self.connect(self.params)

    @property
    def params(self) -> dict:
        attr = ("hostname", "username", "password", "port")
        return {param: getattr(self, param) for param in attr if getattr(self, param, None)}

    @property
    def sftp(self):
        if not self._sftp:
            self._sftp = self.ssh.open_sftp()
        return self._sftp

    def connect(self, params: dict) -> None:
        self.ssh.connect(**params, timeout=3)

    def reconnect(self) -> None:
        self.close()
        self.ssh = None
        self.connect(self.params)

    def connected(self) -> bool:
        return self.ssh is not None
    
    def exec_command(self, command):
        return self.ssh.exec_command(command)

    def __enter__(self):
        return self.ssh

    def close(self):
        if self._sftp:
            self._sftp.close()
        if self.ssh:
            self.ssh.close()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()
