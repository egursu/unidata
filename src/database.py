from collections.abc import Sequence
import constants as c
from .dbapi2 import DBConnection
from importlib import import_module
from inspect import signature
from urllib.parse import ParseResult, urlsplit, parse_qsl
from .utils import to_list, is_matrix


class Database(DBConnection):
    url: str
    engine: str
    library: object
    library_name: str
    host: str
    port: int
    user: str
    password: str
    database: str
    kwargs: dict
    placeholder: str

    def __init__(self, url: str) -> None:
        self.url = url
        url: ParseResult = urlsplit(url)
        if not (url.scheme and url.netloc):
            raise ValueError(f"Invalid connection string (URL): {self.url}")
        url_scheme = url.scheme.lower()
        if "+" in url_scheme:
            self.engine, self.library_name = url_scheme.split("+")
        else:
            self.engine, self.library_name = (url_scheme, c.DEFAULT_LIBRARY.get(url_scheme, None))
        # raise error if database or library not supported
        if self.engine not in c.DB_TYPES:
            raise KeyError(f'Database type "{self.engine}" not supported.')
        if not self.library_name:
            raise KeyError(f'Library name "{url_scheme}" not supported.')
        self.host = url.hostname
        # self.port = url.port or c.DEFAULT_PORT.get(self.engine, None)
        self.port = url.port
        self.user = url.username
        self.password = url.password
        self.database = url.path.lstrip("/") if self.host else url.path
        self.kwargs = {item[0].lower(): item[1] for item in parse_qsl(url.query)}
        # get library
        try:
            self.library = import_module(self.library_name)
        except ImportError as e:
            print(f'Unknown db library "{self.library_name}": {e}')
            exit(1)
        # placeholder for variable replacement
        # self.placeholder = c.PARAMSTYLE["nobindvars"] if self.engine == c.ACCESS else c.PARAMSTYLE.get(self.library.paramstyle)
        self.placeholder = c.PARAMSTYLE.get(self.library_name, c.NOBINDVARS)
        # self.placeholder = c.PARAMSTYLE.get(self.library.paramstyle, c.PLACEHOLDER.get(self.library_name, c.NOBINDVARS))
        # oracledb init_oracle_client by default
        if self.library_name == c.ORACLEDB and self.library.is_thin_mode():
            self.library.init_oracle_client(lib_dir=self.kwargs.get("lib_dir", None))
        self.connect(self.params)

    def connect(self, params: dict) -> None:
        connect_def = self.library.connect
        # filter arguments there are in connect() function
        params.update({k: v for k, v in self.kwargs.items() if k in signature(connect_def).parameters})
        self.connection = connect_def(**params)

    def reconnect(self) -> None:
        self.connection = None
        self.connect(self.params)

    def connected(self) -> bool:
        return self.connection is not None

    @property
    def params(self) -> dict:
        attr: tuple = ("host", "database", "user", "password", "port")
        params: dict = {param: getattr(self, param) for param in attr if getattr(self, param, None)}
        if self.library_name == c.ORACLEDB:
            params["service_name"] = params.pop("database")
        elif self.library_name in (c.PSYCOPG, c.PSYCOPG2):
            params["dbname"] = params.pop("database")
        elif self.library_name == c.PYMSSQL:
            params["server"] = params.pop("host")
        elif self.library_name == c.PYODBC:
            params["server"] = params.pop("host")
            if not params.get("driver", None):
                drivers = self.library.drivers()
                if drivers and isinstance(drivers, list):
                    drivers = [driver for driver in drivers if c.DEFAULT_PYODBC_DRIVER[self.engine].lower() in driver.lower()]
                    if drivers:
                        params["driver"] = drivers[-1]
                    else:
                        raise Exception(f"Default ODBC Driver for {self.engine.upper()} not found.")
                else:
                    raise Exception(f"PyODBC Drivers is empty.")
            if self.engine == c.ACCESS:
                params["DBQ"] = params.pop("database")
        elif self.library_name == c.SQLITE3:
            pass
        return params

    @property
    def fields(self):
        return [item[0] for item in self.description]
    
    @staticmethod
    def noname_fields(data: list|tuple, numeric: bool=False) -> list:
        len_data = len(data[0]) if isinstance(data[0], list|tuple) else len(data)
        return [i if numeric else f"Col{i}" for i in range(1, len_data + 1)]
    
    def bind_params(self, fields: str|Sequence, delim: str=",", operator: str=None) -> str:
        delim = delim.strip().upper()
        delim = f"{'' if delim=="," else ' '}{delim} "
        operator = ("{}"+operator).format if operator else "".format
        return delim.join([f"{operator(field)}{self.placeholder.format(field)}" for field in to_list(fields)])
    
    def run_sql(self, sql_text, data):
        kwargs = {}
        if is_matrix(data) and self.library_name == c.PYMSSQL:
            kwargs = {'batch_size': len(data)} 
        return (self.executemany if data and is_matrix(data) else self.execute)(sql_text, data, **kwargs)
    
    def insert(self, table: str, data: list, fields: str|list|tuple = None) -> None:
        fields = to_list(fields)
        join_fields = f" ({', '.join(fields)})" if fields else ""
        sql_text = f"INSERT INTO {table}{join_fields} VALUES ({self.bind_params(fields or self.noname_fields(data))})"
        self.run_sql(sql_text, data)
    
    def update(self, table: str, data: list, fields: str|list|tuple, keys: str|list|tuple=None) -> None:
        fields = to_list(fields)
        condition = f"WHERE {self.bind_params(to_list(keys), delim="AND", operator="=")}" if keys else ""
        sql_text = f'UPDATE {table} SET {self.bind_params(fields, delim=",", operator="=")} {condition}'
        self.run_sql(sql_text, data)

    def delete(self, table: str, data: list=None, keys: str|list|tuple=None) -> None:
        condition = f"WHERE {self.bind_params(to_list(keys), delim="AND", operator="=")}" if keys else ""
        sql_text = f'DELETE FROM {table} {condition}' if keys else c.TRUNCATE_TABLE[self.engine].format(table)
        self.run_sql(sql_text, data)

    def callproc(self, procname: str, *args, **kwargs):
        if self.engine == c.ACCESS:
            raise Exception("No ACCES SPL!")
        elif hasattr(self.cursor(), "callproc"):
            return self.cursor.callproc(procname, *args, **kwargs)
        else:
            procname = procname.replace(";", "")
            procname = (f'{procname.replace("()", "")}({",".join(self.bind_paramstyle("") for _ in args)})' if args else procname)
            return self.cursor.execute(c.SPLCALL[self.library_name].format(procname), args).fetchall()
    
    @property
    def library_version(self) -> str:
        try:
            return self.library.__version__
        except:
            return self.library.version

    @property
    def version(self) -> str:
        version = "unknown"
        sql = c.METADATA[c.DBVERSION, self.engine]
        if self.engine == c.ACCESS:
            version = "unavailable for MS Access through SQL"
        elif sql:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            version = cursor.fetchone()[0]
            cursor.close()
            del cursor
        elif hasattr(self.connection, "version"):
            version = self.connection.version
        return str(version)

    @property
    def tables(self) -> tuple:
        result: tuple = None
        sql = c.METADATA[c.TABLES, self.engine]
        if self.engine != c.ACCESS:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            result = tuple(item[0] for item in cursor.fetchall())
            cursor.close()
            del cursor
        return result
    
    @property
    def views(self) -> dict:
        result: dict = {}
        sql = c.METADATA[c.VIEWS, self.engine]
        if self.engine != c.ACCESS:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            for item in cursor.fetchall():
                result[item[0]] = {
                    'sql_text': str(item[1]),
                    'check_option': item[2], 
                    'is_updatable': item[3],
                    'is_insertable': item[4],
                    'is_deletable': item[5],
                }
            cursor.close()
            del cursor
        return result

    def _object_columns(self, object_name: str, object_type: str=c.TAB_COL) -> dict:
        result: dict = {}
        sql = c.METADATA[object_type, self.engine].format(object_name)
        if self.engine != c.ACCESS:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            for item in cursor.fetchall():
                result[item[1]] = {
                    'id': item[0],
                    'type': item[2], 
                    'is_nullable': item[3],
                    'default_value': item[4],
                    'comment': str(item[5]),
                }
            cursor.close()
            del cursor
        return result

    def table_columns(self, table: str) -> dict:
        return self._object_columns(table, c.TAB_COL)

    def view_columns(self, view: str) -> dict:
        return self._object_columns(view, c.VIEW_COL)