"""

.. versionadded:: 3006

Minion data cache plugin for postgres database.

:codeauthor: Paul Martin <https://github.com/PaulChristophel>

It is up to the system administrator to set up and configure the postgres
infrastructure. All is needed for this plugin is a working postgres server.

The module requires that the (user defined) `salt_cache` database and table exist.
The keys are indexed using the `bank` and `psql_key` columns.

To enable this cache plugin, the master will need the python client for
postgres installed. This can be easily installed with pip:

.. code-block:: bash

    pip install psycopg2

Optionally, depending on the postgres agent configuration, the following values
could be set in the master config. These are the defaults:

.. code-block:: yaml

    cache.pgbytea.host: 127.0.0.1
    cache.pgbytea.port: 5432
    cache.pgbytea.user: None
    cache.pgbytea.password: None
    cache.pgbytea.dbname: salt_cache
    cache.pgbytea.table_name: salt_cache

To use the pgbytea module as a minion data cache backend, set the master ``cache`` config
value to ``pgbytea``:

.. code-block:: yaml

    cache: pgbytea

To use the plugin with the default table_name, use the following table structure:

.. code-block:: sql

    --
    -- Table structure for table 'salt_cache'
    --
    CREATE TABLE salt_cache (
        bank varchar(255) NOT NULL,
        psql_key varchar(255) NOT NULL,
        data bytea,
        id uuid DEFAULT gen_random_uuid(),
        data_changed timestamp default now(),
        PRIMARY KEY(bank, psql_key)
    );
    CREATE INDEX idx_salt_cache_bank ON salt_cache(bank);
    CREATE INDEX idx_salt_cache_psql_key ON salt_cache(psql_key);

    CREATE FUNCTION data_changed() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
    BEGIN
    NEW.data_changed := current_timestamp;
    RETURN NEW;
    END;
    $$;

    CREATE TRIGGER trigger_data_changed
    BEFORE UPDATE ON salt_cache
    FOR EACH ROW
    WHEN (OLD.data IS DISTINCT FROM NEW.data)
    EXECUTE PROCEDURE data_changed();
"""

import uuid
import copy
import logging
from contextlib import contextmanager
from typing import Any, Union, Tuple, Dict, Generator

import salt.exceptions
import salt.utils.json

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

_DEFAULT_DATABASE_NAME = "salt_cache"
_DEFAULT_TABLE_NAME = "salt_cache"


# Module properties

__virtualname__ = "pgbytea"
__func_alias__ = {"list_": "list", "id_": "id"}

log = logging.getLogger(__name__)


def __virtual__() -> (Union[str, Tuple[bool, str]]):
    """
    Confirm that a python postgres client is installed.
    """
    return "pgbytea" if psycopg2 else (False, "psycopg2 not installed.")


def _get_options() -> (Dict[str, Union[int, str]]):
    """
    Get the pgbytea options from salt.
    """
    opts = copy.deepcopy(__opts__)  # type: ignore
    return {
        "host": opts.pop("cache.pgbytea.host", "127.0.0.1"),
        "user": opts.pop("cache.pgbytea.user", None),
        "password": opts.pop(
            "cache.pgbytea.password", opts.pop("cache.pgbytea.passwd", None)
        ),
        "dbname": opts.pop(
            "cache.pgbytea.dbname",
            opts.pop("cache.pgbytea.database", _DEFAULT_DATABASE_NAME),
        ),
        "table": opts.pop("cache.pgbytea.table_name", _DEFAULT_TABLE_NAME),
        "port": int(opts.pop("cache.pgbytea.port", 5432)),
    }


@contextmanager
def _get_serv(
    commit: bool = False,
) -> (Generator[Tuple[psycopg2.extensions.cursor, Union[int, str]], Any, Any]):
    """
    Return a psql cursor.
    """
    _options = _get_options()
    try:
        conn = psycopg2.connect(
            host=_options.get("host"),
            user=_options.get("user"),
            password=_options.get("password"),
            database=_options.get("dbname"),
            port=_options.get("port"),
        )

    except psycopg2.OperationalError as exc:
        raise salt.exceptions.SaltMasterError(
            f"pgjsonb cache could not connect to database: {exc}"
        )

    cursor = conn.cursor()

    try:
        yield cursor, _options["table"]
    except psycopg2.DatabaseError as err:
        error = err.args
        log.error(error)
        cursor.execute("ROLLBACK")
        raise
    else:
        if commit:
            cursor.execute("COMMIT")
        else:
            cursor.execute("ROLLBACK")
    finally:
        conn.close()


def store(bank: str, key: str, data: Any) -> (None):
    """
    Store a key value.
    """
    data = psycopg2.Binary(salt.payload.dumps(data))
    with _get_serv(commit=True) as cursor:
        cur, table = cursor
        query = f"""INSERT INTO {table} (bank, psql_key, data)
                    VALUES ('{bank}', '{key}', {data})
                    ON CONFLICT (bank, psql_key) DO UPDATE
                      SET data = EXCLUDED.data"""
        log.debug(query)
        try:
            cur.execute(query)
            log.debug(query)
        except TypeError as err:
            log.error(err)
            log.error("table: %s", table)
            log.error("bank: %s", bank)
            log.error("psql_key: %s", key)
            log.error("data: %s", data)
        except psycopg2.IntegrityError as err:
            # https://github.com/saltstack/salt/issues/22171
            log.error(err)
            log.error("table: %s", table)
            log.error("bank: %s", bank)
            log.error("psql_key: %s", key)
            log.error("data: %s", data)
        except psycopg2.DataError as err:
            log.error(err)
            log.error("table: %s", table)
            log.error("bank: %s", bank)
            log.error("psql_key: %s", key)
            log.error("data: %s", data)
        except psycopg2.Error as err:
            log.error(err)
            log.error("table: %s", table)
            log.error("bank: %s", bank)
            log.error("psql_key: %s", key)
            log.error("data: %s", data)


def fetch(bank: str, key: str) -> (Any):
    """
    Fetch a key value.
    """
    with _get_serv(commit=True) as cursor:
        cur, table = cursor
        query = f"""SELECT data FROM {table}
                    WHERE bank='{bank}' AND psql_key='{key}'"""
        log.debug(query)
        cur.execute(query)
        data = cur.fetchall()
        log.debug(data)
        if data is None:
            return {}
        try:
            return_data = data[0][0]
            return_data = salt.payload.loads(return_data)
            log.debug(return_data)
            return return_data
        except IndexError as err:
            log.error(err)
            log.error("table: %s", table)
            log.error("bank: %s", bank)
            log.error("psql_key: %s", key)
            log.error("data: %s", data)
        return {}


def flush(bank: str, key: (Union[str, None]) = None) -> (bool):
    """
    Remove the key from the cache bank with all the key content.
    """
    with _get_serv(commit=True) as cursor:
        cur, table = cursor
        query = f"DELETE FROM {table} WHERE bank='{bank}'"
        if key:
            query += f" AND psql_key='{key}'"
        log.debug(query)
        try:
            cur.execute(query)
            return True
        except psycopg2.Error as err:
            log.error(err)
            log.error("table: %s", table)
            log.error("bank: %s", bank)
            log.error("psql_key: %s", key)
        return False


def list_(bank: str) -> (Tuple[str, ...]):
    """
    Return an iterable object containing all entries stored in the specified
    bank.
    """
    with _get_serv(commit=True) as cursor:
        cur, table = cursor
        query = f"SELECT psql_key FROM {table} WHERE bank='{bank}'"
        log.debug(query)
        cur.execute(query)
        data = cur.fetchall()
        return tuple(_[0] for _ in data if len(_) == 1) if data else ()


def contains(bank: str, key: str) -> (bool):
    """
    Checks if the specified bank contains the specified key.
    """
    with _get_serv(commit=True) as cursor:
        cur, table = cursor
        query = f"""SELECT COUNT(data) FROM {table}
                    WHERE bank='{bank}' AND psql_key='{key}'"""
        log.debug(query)
        cur.execute(query)
        data = cur.fetchall()
        return data[0][0] == 1


def updated(bank: str, key: str) -> (int):
    """
    Return the integer Unix epoch update timestamp of the specified bank and
    key.
    """
    with _get_serv(commit=True) as cursor:
        cur, table = cursor
        query = f"""SELECT extract(epoch FROM data_changed)::int FROM {table}
                    WHERE bank='{bank}' AND psql_key='{key}'"""
        log.debug(query)
        cur.execute(query)
        data = cur.fetchall()
        log.debug(data)
        if data is None:
            return 0
        try:
            return_data = data[0][0]
            log.debug(return_data)
            return return_data
        except IndexError as err:
            log.error(err)
            log.error("table: %s", table)
            log.error("bank: %s", bank)
            log.error("psql_key: %s", key)
            log.error("data: %s", data)
        return 0


def id_(bank: str, key: str) -> (Union[uuid.UUID]):
    """
    Return the uuid of the specified bank and key.
    """
    with _get_serv(commit=True) as cursor:
        cur, table = cursor
        query = f"""SELECT id
                    FROM {table}
                    WHERE bank='{bank}' AND psql_key='{key}'"""
        log.debug(query)
        cur.execute(query)
        data = cur.fetchall()
        log.debug(data)
        if data is None:
            return uuid.UUID("00000000-0000-0000-0000-000000000000")
        try:
            return_data = data[0][0]
            log.debug(return_data)
            return uuid.UUID(return_data)
        except IndexError as err:
            log.error(err)
            log.error("table: %s", table)
            log.error("bank: %s", bank)
            log.error("psql_key: %s", key)
            log.error("data: %s", data)
        return uuid.UUID("00000000-0000-0000-0000-000000000000")
