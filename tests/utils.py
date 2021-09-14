# -*- coding: utf-8 -*-

import os
import time
import signal
import platform
import multiprocessing
from contextlib import closing

import sqlite3
import pytest

from usql.main import special

DATABASE = os.getenv("PYTEST_DATABASE", "test.sqlite3")


def db_connection(dbname=":memory:"):
    conn = sqlite3.connect(database=dbname, isolation_level=None)
    return conn


try:
    db_connection()
    CAN_CONNECT_TO_DB = True
except Exception as ex:
    CAN_CONNECT_TO_DB = False

dbtest = pytest.mark.skipif(
    not CAN_CONNECT_TO_DB, reason="Error creating sqlite connection"
)


def create_db(dbname):
    with closing(db_connection().cursor()) as cur:
        try:
            cur.execute("""DROP DATABASE IF EXISTS _test_db""")
            cur.execute("""CREATE DATABASE _test_db""")
        except:
            pass


def drop_tables(dbname):
    with closing(db_connection().cursor()) as cur:
        try:
            cur.execute("""DROP DATABASE IF EXISTS _test_db""")
        except:
            pass


def run(executor, sql, rows_as_list=True):
    """Return string output for the sql to be run."""
    result = []

    for title, rows, headers, status in executor.run(sql):
        rows = list(rows) if (rows_as_list and rows) else rows
        result.append(
            {"title": title, "rows": rows, "headers": headers, "status": status}
        )

    return result


def set_expanded_output(is_expanded):
    """Pass-through for the tests."""
    return special.set_expanded_output(is_expanded)


def is_expanded_output():
    """Pass-through for the tests."""
    return special.is_expanded_output()


def send_ctrl_c_to_pid(pid, wait_seconds):
    """Sends a Ctrl-C like signal to the given `pid` after `wait_seconds`
    seconds."""
    time.sleep(wait_seconds)
    system_name = platform.system()
    if system_name == "Windows":
        os.kill(pid, signal.CTRL_C_EVENT)
    else:
        os.kill(pid, signal.SIGINT)


def send_ctrl_c(wait_seconds):
    """Create a process that sends a Ctrl-C like signal to the current process
    after `wait_seconds` seconds.

    Returns the `multiprocessing.Process` created.

    """
    ctrl_c_process = multiprocessing.Process(
        target=send_ctrl_c_to_pid, args=(os.getpid(), wait_seconds)
    )
    ctrl_c_process.start()
    return ctrl_c_process
