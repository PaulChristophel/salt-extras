"""
Minion data cache plugin for pgjsonb database.

.. versionadded:: 3005

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

    cache.pgjsonb_encoded.host: 127.0.0.1
    cache.pgjsonb_encoded.port: 5432
    cache.pgjsonb_encoded.user: None
    cache.pgjsonb_encoded.password: None
    cache.pgjsonb_encoded.dbname: salt_cache
    cache.pgjsonb_encoded.table_name: salt_cache

To use the pgjsonb_encoded module as a minion data cache backend, set the master ``cache`` config
value to ``pgjsonb_encoded``:

.. code-block:: yaml

    cache: pgjsonb_encoded

To use the plugin with the default table_name, use the following table structure:

.. code-block:: sql

    --
    -- Table structure for table 'salt_cache'
    --
    CREATE TABLE salt_cache (
        bank varchar(255) NOT NULL,
        psql_key varchar(255) NOT NULL,
        data jsonb,
        id uuid DEFAULT gen_random_uuid(),
        data_changed timestamp default now(),
        encoded default false,
        PRIMARY KEY(bank, psql_key)
    );
    CREATE INDEX idx_salt_cache_bank ON salt_cache(bank);
    CREATE INDEX idx_salt_cache_psql_key ON salt_cache(psql_key);
    CREATE INDEX idx_salt_cache_data ON salt_cache USING gin(data) WITH (fastupdate=on);

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

import copy
import logging
from contextlib import contextmanager
from typing import Any

import salt.exceptions
import salt.utils.stringutils
from salt.utils.stringutils import base64

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

_DEFAULT_DATABASE_NAME = "salt_cache"
_DEFAULT_TABLE_NAME = "salt_cache"


# Module properties

__virtualname__ = "pgjsonb_encoded"
__func_alias__ = {"list_": "list"}

log = logging.getLogger(__name__)


def __virtual__():
    """
    Confirm that a python postgres client is installed.
    """
    return "pgjsonb_encoded" if psycopg2 else False, "psycopg2 not installed."


def _get_options():
    """
    Get the pgjsonb options from salt.
    """
    opts = copy.deepcopy(__opts__)  # type: ignore
    return {
        "host": opts.pop("cache.pgjsonb_encoded.host", "127.0.0.1"),
        "user": opts.pop("cache.pgjsonb_encoded.user", None),
        "password": opts.pop(
            "cache.pgjsonb_encoded.password",
            opts.pop("cache.pgjsonb_encoded.passwd", None),
        ),
        "dbname": opts.pop(
            "cache.pgjsonb_encoded.dbname",
            opts.pop("cache.pgjsonb_encoded.database", _DEFAULT_DATABASE_NAME),
        ),
        "table": opts.pop("cache.pgjsonb_encoded.table_name", _DEFAULT_TABLE_NAME),
        "port": int(opts.pop("cache.pgjsonb_encoded.port", 5432)),
    }


@contextmanager
def _get_serv(commit=False):
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
            f"pgjsonb_encoded cache could not connect to database: {exc}"
        )

    cursor = conn.cursor()

    try:
        yield cursor
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


def store(bank, key, data) -> None:
    """
    Store a key value.
    """
    table = _get_options()["table"]
    with _get_serv(commit=True) as cur:
        encoded = False
        if isinstance(data, bytes):
            try:
                data = salt.utils.stringutils.to_str(data)
            except UnicodeDecodeError:
                encoded = True
                data = base64.b64encode(data).decode("utf-8")
        data = psycopg2.extras.Json(data)
        query = f"""INSERT INTO {table} (bank, psql_key, data, encoded)
                    VALUES ('{bank}', '{key}', to_jsonb({data}::json), {encoded})
                    ON CONFLICT (bank, psql_key) DO UPDATE
                      SET data = EXCLUDED.data,
                       encoded = EXCLUDED.encoded"""
        log.debug(query)
        try:
            cur.execute(query)
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


def fetch(bank, key) -> Any:
    """
    Fetch a key value.
    """
    table = _get_options()["table"]
    with _get_serv(commit=True) as cur:
        query = f"""SELECT data, encoded
                    FROM {table}
                    WHERE bank='{bank}' AND psql_key='{key}'"""
        log.debug(query)
        cur.execute(query)
        data = cur.fetchall()
        log.debug(data)
        if data is None:
            return {}
        try:
            return_data = data[0][0] if not data[0][1] else base64.b64decode(data[0][0])
            log.debug(return_data)
            return return_data
        except IndexError as err:
            log.error(err)
            log.error("table: %s", table)
            log.error("bank: %s", bank)
            log.error("psql_key: %s", key)
            log.error("data: %s", data)
        return {}


def flush(bank, key=None) -> (bool):
    """
    Remove the key from the cache bank with all the key content.
    """
    table = _get_options()["table"]
    with _get_serv(commit=True) as cur:
        query = f"""DELETE FROM {table}
                    WHERE bank='{bank}'"""
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


def list_(bank) -> tuple:
    """
    Return an iterable object containing all entries stored in the specified
    bank.
    """
    table = _get_options()["table"]
    with _get_serv(commit=True) as cur:
        query = f"""SELECT psql_key
                    FROM {table}
                    WHERE bank='{bank}'"""
        log.debug(query)
        cur.execute(query)
        data = cur.fetchall()
        return tuple(_[0] for _ in data if len(_) == 1) if data else ()


def contains(bank, key) -> bool:
    """
    Checks if the specified bank contains the specified key.
    """
    table = _get_options()["table"]
    with _get_serv(commit=True) as cur:
        query = f"""SELECT COUNT(data)
                    FROM {table}
                    WHERE bank='{bank}' AND psql_key='{key}'"""
        log.debug(query)
        cur.execute(query)
        data = cur.fetchall()
        return data[0][0] == 1


def updated(bank, key) -> int:
    """
    Return the integer Unix epoch update timestamp of the specified bank and
    key.
    """
    table = _get_options()["table"]
    with _get_serv(commit=True) as cur:
        query = f"""SELECT extract(epoch FROM data_changed)::int
                    FROM {table}
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
