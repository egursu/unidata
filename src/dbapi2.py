# PEP 249 Database API 2.0 Types
# https://www.python.org/dev/peps/pep-0249/

from collections.abc import Mapping, Sequence, Iterator
from typing import Any, Protocol, Literal, Self
from typing_extensions import TypeAlias


DBAPITypeCode: TypeAlias = Any | None
# Strictly speaking, this should be a Sequence, but the type system does
# not support fixed-length sequences.
DBAPIColumnDescription: TypeAlias = tuple[str, DBAPITypeCode, int | None, int | None, int | None, int | None, bool | None]


class DBAPICursor(Protocol):
    @property
    def description(self) -> Sequence[DBAPIColumnDescription] | None: ...
    @property
    def rowcount(self) -> int: ...
    def callproc(self, procname: str, parameters: Sequence[Any] = ..., /) -> Sequence[Any]: ...
    def close(self) -> object: ...
    def execute(self, operation: str, parameters: Sequence[Any] | Mapping[str, Any] = ..., /) -> object: ...
    def executemany(self, operation: str, seq_of_parameters: Sequence[Sequence[Any]], /) -> object: ...
    def fetchone(self) -> Sequence[Any] | None: ...
    def fetchmany(self, size: int = ..., /) -> Sequence[Sequence[Any]]: ...
    def fetchall(self) -> Sequence[Sequence[Any]]: ...
    def nextset(self) -> None | Literal[True]: ...
    arraysize: int
    def setinputsizes(self, sizes: Sequence[DBAPITypeCode | int | None], /) -> object: ...
    def setoutputsize(self, size: int, column: int = ..., /) -> object: ...


class DBAPIConnection(Protocol):
    def close(self) -> object: ...
    def commit(self) -> object: ...
    def rollback(self) -> Any: ...
    def cursor(self) -> DBAPICursor: ...


class DBConnection:
    _connection: DBAPIConnection
    _cursor: DBAPICursor
    
    def close(self) -> None: 
        try:
            self._cursor.close() 
        finally:
            self._connection.close()

    @property
    def connection(self):
        return self._connection
    
    @connection.setter
    def connection(self, conn: DBAPIConnection|None):
        if conn:
            self._connection = conn
            self._cursor = conn.cursor()
        else:
            self.close()

    def commit(self) -> None: 
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def __enter__(self) -> Self:
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        try:
            self.rollback()
        finally:
            self.close()
    
    @property
    def cursor(self) -> DBAPICursor:
        if not self._cursor:
            self._cursor = self.connection.cursor()
        return self._cursor
    
    @cursor.setter
    def cursor(self, cur: DBAPICursor|None):
        self._cursor = cur if cur else self._cursor.close()    
        return self._cursor
    
    def cursor_create(self, *args, **kwargs) -> DBAPICursor:
        return self.connection.cursor(*args, **kwargs)
    
    def iteritems(self, size: int=100000) -> Iterator[Sequence[Sequence[Any]]]:
        while True:
            rows = self.fetchmany(size)
            if not rows:
                break
            yield rows

    @property
    def description(self) -> Sequence[DBAPIColumnDescription] | None:
        return self.cursor.description
    
    @property
    def rowcount(self) -> int:
        return self.cursor.rowcount

    @property 
    def arraysize(self) -> int:
        return self.cursor.arraysize
    
    @arraysize.setter
    def arraysize(self, arraysize: int) -> None:
        self.cursor.arraysize = arraysize

    def execute(self, operation: Any, *args, **kwargs) -> DBAPICursor:
        cursor = self.cursor.execute(operation, *args, **kwargs)
        return cursor or self.cursor
    
    def executemany(
        self,
        operation: Any,
        seq_of_parameters: Sequence[Any] | Mapping[Any, Any],
        *args,
        **kwargs
    ) -> None:
        if hasattr(self.cursor, 'fast_executemany'):
            self.cursor.fast_executemany = True
        self.cursor.executemany(operation, seq_of_parameters, *args, **kwargs)
    
    def fetchone(self) -> Sequence[Any] | None:
        return self.cursor.fetchone()
    
    def fetchmany(self, size: int = 0) -> Sequence[Sequence[Any]]:
        return self.cursor.fetchmany(size)
    
    def fetchall(self) -> Sequence[Sequence[Any]]:
        return self.cursor.fetchall()
    
    def nextset(self) -> None | Literal[True]:
        return self.cursor.nextset()
    
    def setinputsizes(self, sizes: Sequence[DBAPITypeCode | int | None]) -> object:
        return self.cursor.setinputsizes(sizes)
    
    def setoutputsize(self, size: int, column: int | None = None) -> object:
        return self.cursor.setoutputsize(size, column)
    
    # def callproc(self, procname: str, *args, **kwargs) -> Sequence[Any]:
        # if hasattr(self.cursor(), "callproc"):
        #     return self.cursor.callproc(procname, *args, **kwargs)
        # return None
