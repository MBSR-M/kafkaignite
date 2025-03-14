#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql
from utils.decorators import measure_and_log_elapsed_time
from sshtunnel import SSHTunnelForwarder


class DbFacade:
    def __init__(self, ssh_host, ssh_port, ssh_username, ssh_private_key, mysql_host, mysql_port, mysql_user,
                 mysql_password, mysql_db):
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_username = ssh_username
        self.ssh_private_key = ssh_private_key
        self.mysql_host = mysql_host
        self.mysql_port = mysql_port
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_db = mysql_db

    def _run_with_connection_and_cursor(self, query_func):
        """Establishes an SSH tunnel and runs a query function using a MySQL connection and cursor."""
        with SSHTunnelForwarder(
                (self.ssh_host, self.ssh_port),
                ssh_username=self.ssh_username,
                ssh_pkey=self.ssh_private_key,
                remote_bind_address=(self.mysql_host, self.mysql_port)
        ) as tunnel:
            connection = pymysql.connect(
                host=self.mysql_host,
                port=tunnel.local_bind_port,
                user=self.mysql_user,
                password=self.mysql_password,
                database=self.mysql_db
            )
            with connection.cursor() as cursor:
                cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
                cursor.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT')
                return query_func(connection, cursor)

    @measure_and_log_elapsed_time
    def execute_query(self, query):
        """Executes a query and returns the result."""

        def run_query(connection, cursor):
            cursor.execute(query)
            return cursor.fetchall()

        return self._run_with_connection_and_cursor(run_query)

    def fetch_data(self, query):
        """Returns a list of tables in the database."""
        return self.execute_query(query)

    def fetch_data_from_database_and_publish(self, query):
        return self.execute_query(query)
