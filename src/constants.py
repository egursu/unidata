_sql = lambda sql: "\n".join(line.strip() for line in sql.splitlines())

XLSX_MAX_ROWS = 1048576

# SUPPORTED DATABASE TYPES.
ACCESS = "access"
MYSQL = "mysql"
ORACLE = "oracle"
POSTGRESQL = "postgresql"
SQLITE = "sqlite"
MSSQL = "mssql"
DB_TYPES = [ACCESS, MYSQL, ORACLE, POSTGRESQL, SQLITE, MSSQL]

# GROUPINGS OF DATABASE TYPES.
USES_CONNECTION_STRING = set(DB_TYPES) - {MYSQL}
FILE_DATABASES = {ACCESS, SQLITE}

# DATABASE LIBRARIES.
ORACLEDB = "oracledb"
PSYCOPG = "psycopg"
PSYCOPG2 = "psycopg2"
PYMYSQL = "pymysql"
PYODBC = "pyodbc"
PYMSSQL = "pymssql"
SQLITE3 = "sqlite3"

DEFAULT_LIBRARY = {
    ACCESS: PYODBC,
    MYSQL: PYMYSQL,
    ORACLE: ORACLEDB,
    POSTGRESQL: PSYCOPG,
    SQLITE: SQLITE3,
    MSSQL: PYODBC,
}

DEFAULT_PORT = {
    MYSQL: 3306,
    ORACLE: 1521,
    POSTGRESQL: 5432,
    MSSQL: 1433,
}

DEFAULT_PYODBC_DRIVER = {
    ACCESS: "Microsoft Access Driver",
    MSSQL: "SQL Server",
    MYSQL: "MYSQL",
    ORACLE: "Oracle",
    POSTGRESQL: "Postgres",
    SQLITE: "SQLite",
}

# PARAMETERIZATION/BIND VARIABLE FORMAT USED HERE.
# PARAMSTYLE = {
#     "qmark": "?",
#     "numeric": ":{}",
#     "named": ":{}",
#     "format": "%s",
#     "pyformat": "%({})s",
# }.setdefault("nobindvars", "{}")

# PLACEHOLDER = {
#     ORACLEDB: PARAMSTYLE["named"],
#     PSYCOPG: PARAMSTYLE["format"],
#     PSYCOPG2: PARAMSTYLE["format"],
#     PYMYSQL: PARAMSTYLE["format"],
#     PYMSSQL: PARAMSTYLE["format"],
#     PYODBC: PARAMSTYLE["qmark"],
#     SQLITE3: PARAMSTYLE["qmark"],
# }

NAMED = ":{}"
PYFORMAT = "%s"
QMARK = "?"
NOBINDVARS = "{}"
# NAMED = 'named'
# PYFORMAT = 'pyformat'
# QMARK = 'qmark'
# NOBINDVARS = 'nobindvars'
PARAMSTYLE = {
    ORACLEDB: NAMED,
    PSYCOPG: PYFORMAT,
    PSYCOPG2: PYFORMAT,
    PYMYSQL: PYFORMAT,
    PYMSSQL: PYFORMAT,
    PYODBC: QMARK,
    SQLITE3: NAMED,
}

ORACLE_SPL = "BEGIN {}; END"
CALL_SPL = "CALL {}"
EXEC_SPL = "EXEC {}"

SPLCALL = {
    ORACLEDB: ORACLE_SPL,
    PSYCOPG: CALL_SPL,
    PSYCOPG2: CALL_SPL,
    PYMYSQL: CALL_SPL,
    PYMSSQL: CALL_SPL,
    PYODBC: EXEC_SPL,
    SQLITE3: CALL_SPL,
}

TRUNCATE_SQL = "TRUNCATE TABLE {}"
DELETE_SQL = "DELETE FROM {}"

TRUNCATE_TABLE = {
  ORACLE: TRUNCATE_SQL,
  POSTGRESQL: TRUNCATE_SQL,
  MSSQL: TRUNCATE_SQL,
  MYSQL: TRUNCATE_SQL,
  SQLITE: DELETE_SQL,
}

NOT_IMPLEMENTED = "FINDING YOUR {} NOT IMPLEMENTED FOR {}."
NOT_POSSIBLE_SQL = "SQL CANNOT READ THE SCHEMA IN {} THROUGH {}."

DBVERSION = "DBVERSION"
TABLES = "TABLES"
TAB_COL = "TABLE COLUMNS"
VIEWS = "VIEWS"
VIEW_COL = "VIEW COLUMNS"
INDEXES = "INDEXES"
INDEX_COL = "INDEX COLUMNS"

METADATA = dict()

# QUERIES FOR FINDING DATABASE VERSION.
METADATA[DBVERSION, ACCESS] = NOT_POSSIBLE_SQL
METADATA[DBVERSION, MYSQL] = _sql("SELECT version()")
METADATA[DBVERSION, POSTGRESQL] = _sql("SELECT version()")
METADATA[DBVERSION, ORACLE] = _sql("SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'")
METADATA[DBVERSION, SQLITE] = _sql("SELECT sqlite_version()")
METADATA[DBVERSION, MSSQL] = _sql("SELECT @@VERSION")

# QUERIES FOR FINDING TABLES.
METADATA[TABLES, ACCESS] = NOT_POSSIBLE_SQL
METADATA[TABLES, MYSQL] = _sql(
"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
    AND table_schema = database()
    ORDER BY table_name
"""
)
METADATA[TABLES, ORACLE] = _sql(
"""
    SELECT table_name
    FROM user_tables
    ORDER BY table_name
"""
)
METADATA[TABLES, POSTGRESQL] = _sql(
    """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
    AND table_schema = 'public'
    ORDER BY table_name
"""
)
METADATA[TABLES, SQLITE] = _sql(
    """
    SELECT name AS table_name
    FROM sqlite_master
    WHERE type='table'
    AND name NOT LIKE 'sqlite_%'
    ORDER BY name
"""
)
METADATA[TABLES, MSSQL] = _sql(
    """
    SELECT name AS table_name
    FROM sys.tables
    WHERE type='U'
    ORDER BY name
"""
)

# QUERIES FOR FINDING VIEWS.
METADATA[VIEWS, ACCESS] = NOT_POSSIBLE_SQL
METADATA[VIEWS, MYSQL] = _sql(
    """
    SELECT table_name AS view_name, view_definition AS view_sql,
    check_option, is_updatable, 'No' AS is_insertable,
    'No' AS is_deletable
    FROM information_schema.views
    WHERE table_schema = database()
    ORDER BY table_name
"""
)
METADATA[VIEWS, ORACLE] = _sql(
    """
    SELECT view_name, text AS view_sql,
    'No' AS check_option, 'No' AS is_updatable, 'No' AS is_insertable, 'No' AS is_deletable
    FROM user_views
    ORDER BY view_name
"""
)
METADATA[VIEWS, POSTGRESQL] = _sql(
    """
    SELECT table_name AS view_name, view_definition AS view_sql,
    check_option, is_updatable, is_insertable_into AS is_insertable,
    'No' AS is_deletable,
    is_trigger_insertable_into, is_trigger_updatable, is_trigger_deletable
    FROM information_schema.VIEWS
    WHERE table_schema = 'public'
    ORDER BY table_name
"""
)
METADATA[VIEWS, SQLITE] = _sql(
    """
    SELECT name AS view_name, sql AS view_sql,
    'No' AS check_option, 'No' AS is_updatable, 'No' AS is_insertable,
    'No' AS is_deletable
    FROM sqlite_master
    WHERE type='view'
    ORDER BY name
"""
)
METADATA[VIEWS, MSSQL] = _sql(
    """
    SELECT name AS view_name, object_definition(object_id(name)) AS view_sql,
    'No' AS check_option, 'No' AS is_updatable, 'No' AS is_insertable,
    'No' AS is_deletable
    FROM sys.views WHERE type='V'
    ORDER BY name
"""
)

# QUERIES FOR FINDING TABLE COLUMNS.
METADATA[TAB_COL, ACCESS] = NOT_POSSIBLE_SQL
METADATA[TAB_COL, MYSQL] = _sql(
    """
    SELECT ordinal_position AS column_id, column_name,
    column_type AS data_type, is_nullable as nullable,
    column_default AS default_value, column_comment AS comments
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = '{}'
    AND table_schema = database()
"""
)
METADATA[TAB_COL, ORACLE] = _sql(
    """
    SELECT column_id, c.column_name,
      CASE
        WHEN (data_type LIKE '%CHAR%' OR data_type IN ('RAW','UROWID'))
          THEN data_type||'('||c.char_length||
              DECODE(char_used,'B',' BYTE','C',' CHAR','')||')'
        WHEN data_type = 'NUMBER'
          THEN
            CASE
              WHEN c.data_precision IS NULL AND c.data_scale IS NULL
                THEN 'NUMBER'
              WHEN c.data_precision IS NULL AND c.data_scale IS NOT NULL
                THEN 'NUMBER(38,'||c.data_scale||')'
              ELSE data_type||'('||c.data_precision||','||c.data_scale||')'
              END
        WHEN data_type = 'BFILE'
          THEN 'BINARY FILE LOB (BFILE)'
        WHEN data_type = 'FLOAT'
          THEN data_type||'('||to_char(data_precision)||')'||DECODE(
              data_precision, 126,' (double precision)', 63,' (real)','')
        ELSE data_type
        END AS data_type,
      DECODE(nullable,'Y','Yes','No') AS nullable,
      data_default AS default_value,
      comments
    FROM user_tab_cols c, user_col_comments com
    WHERE c.table_name = '{}'
    AND c.table_name = com.table_name
    AND c.column_name = com.column_name
    ORDER BY column_id
"""
)
METADATA[TAB_COL, POSTGRESQL] = _sql(
    """
    SELECT ordinal_position AS column_id, column_name,
      CASE
        WHEN data_type = 'character varying'
          THEN 'varchar('||character_maximum_length||')'
        WHEN data_type = 'bit'
          THEN 'bit('||character_maximum_length||')'
        WHEN data_type = 'bit varying'
          THEN 'varbit('||character_maximum_length||')'
        WHEN data_type = 'character'
          THEN 'char('||character_maximum_length||')'
        WHEN data_type='numeric' AND numeric_precision IS NOT NULL AND
              numeric_scale IS NOT NULL
          THEN 'numeric('||numeric_precision||','||numeric_scale||')'
        WHEN data_type IN ('bigint', 'boolean', 'date', 'double precision',
              'integer', 'money', 'numeric', 'real', 'smallint', 'text')
          THEN data_type
        WHEN data_type LIKE 'timestamp%' AND datetime_precision != 6
          THEN REPLACE(data_type, 'timestamp',
              'timestamp('||datetime_precision||')')
        WHEN data_type LIKE 'time%' AND datetime_precision != 6
          THEN REGEXP_REPLACE(data_type, '^time',
              'time('||datetime_precision||')')
        ELSE data_type
        END AS data_type,
      is_nullable AS nullable,
      column_default AS default_value,
      '' AS comments
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = lower('{}')
    AND table_schema = 'public'
"""
)
METADATA[TAB_COL, SQLITE] = _sql(
    """
    SELECT cid AS column_id, name AS column_name, type AS data_type,
      CASE
        WHEN \"notnull\" = 1
          THEN 'No'
        ELSE 'Yes'
        END AS nullable,
      dflt_value AS default_value,
      '' AS comments
    FROM pragma_table_info('{}')
    ORDER BY column_id
"""
)

# Used for both METADATA[TAB_COL, MSSQL] and METADATA[VIEW_COL, MSSQL].
TAB_COL_MSSQL = _sql(
    """
    SELECT c.column_id, c.name AS column_name,
      CASE
        WHEN t.name in ('char','varchar')
          THEN CONCAT(t.name,'(',c.max_length,')')
        WHEN t.name in ('nchar','nvarchar')
          THEN CONCAT(t.name,'(',c.max_length/2,')')
        WHEN t.name in ('numeric','decimal')
          THEN CONCAT(t.name,'(',c.precision,',',c.scale,')')
        WHEN t.name in ('real','float')
          THEN CONCAT(t.name,'(',c.precision,')')
        WHEN t.name LIKE '%INT'
          THEN t.name
        WHEN t.name IN ('money','datetime')
          THEN t.name
        ELSE t.name
        END AS data_type,
      CASE
        WHEN c.is_nullable = 0
          THEN 'NOT NULL'
        ELSE ''
        END AS nullable,
      '' AS default_value,
      '' AS comments,
      CASE
        WHEN c.is_identity = 1
          THEN 'IDENTITY'
        ELSE ''
        END AS \"identity\"
    FROM sys.columns c INNER JOIN sys.objects o
      ON o.object_id = c.object_id
    LEFT JOIN sys.types t
      ON t.user_type_id = c.user_type_id
    WHERE o.type = '{}'
    AND o.name = '{}'
    ORDER BY c.column_id
"""
)
METADATA[TAB_COL, MSSQL] = TAB_COL_MSSQL.format("U", "{}")

# QUERIES FOR FINDING VIEW COLUMNS.
METADATA[VIEW_COL, ACCESS] = NOT_POSSIBLE_SQL
# TODO CHECK THIS
METADATA[VIEW_COL, MYSQL] = METADATA[TAB_COL, MYSQL]
METADATA[VIEW_COL, ORACLE] = METADATA[TAB_COL, ORACLE]
# TODO CHECK THIS
METADATA[VIEW_COL, POSTGRESQL] = METADATA[TAB_COL, POSTGRESQL]
METADATA[VIEW_COL, SQLITE] = METADATA[TAB_COL, SQLITE]
METADATA[VIEW_COL, MSSQL] = TAB_COL_MSSQL.format("V", "{}")

# QUERIES FOR FINDING INDEXES.
METADATA[INDEXES, ACCESS] = NOT_POSSIBLE_SQL
METADATA[INDEXES, MYSQL] = NOT_IMPLEMENTED
METADATA[INDEXES, ORACLE] = _sql(
    """
    SELECT index_name, index_type, table_type,
      CASE
        WHEN uniqueness = 'UNIQUE'
          THEN 'Yes'
      ELSE 'No'
      END AS \"unique\"
    FROM user_indexes WHERE table_name = '{}'
    ORDER BY index_name
"""
)
METADATA[INDEXES, POSTGRESQL] = NOT_IMPLEMENTED
METADATA[INDEXES, SQLITE] = _sql(
    """
    SELECT name AS index_name, '' AS index_type, '' AS table_type,
      CASE
        WHEN \"unique\" = 1
          THEN 'Yes'
        ELSE 'No'
        END AS \"unique\",
      CASE
        WHEN partial = 1
          THEN 'Yes'
        ELSE 'No'
        END AS partial
    FROM pragma_index_list('{}')
"""
)
METADATA[INDEXES, MSSQL] = NOT_IMPLEMENTED

# QUERIES FOR FINDING INDEX COLUMNS.
METADATA[INDEX_COL, ACCESS] = NOT_POSSIBLE_SQL
METADATA[INDEX_COL, MYSQL] = NOT_IMPLEMENTED
METADATA[INDEX_COL, ORACLE] = _sql(
    """
    SELECT ic.column_position, column_name, descend,
      column_expression FROM user_ind_columns ic
    LEFT OUTER JOIN user_ind_expressions ie
    ON ic.column_position = ie.column_position
    AND ic.index_name = ie.index_name
    WHERE ic.index_name = '{}'
    ORDER BY ic.column_position
"""
)
METADATA[INDEX_COL, POSTGRESQL] = NOT_IMPLEMENTED
METADATA[INDEX_COL, SQLITE] = _sql(
    """
    SELECT seqno AS column_position, name AS column_name,
      CASE
        WHEN desc = 1
          THEN 'DESC'
        ELSE 'ASC'
        END AS descend,
      '' AS column_expression
    FROM pragma_index_xinfo('{}')
    WHERE key = 1
"""
)
METADATA[INDEX_COL, MSSQL] = NOT_IMPLEMENTED
