# -- coding: utf-8 --

import re
import json
import logging

import asyncio
import aiopg
import psycopg2

from aiohttp import web
from aiohttp.web import Application


CONFIG = {
    "databases": {
        "postgres": {
            "host": "",
            "port": 0,
            "database": "",
            "user": "",
            "password": ""
        }
    }
}


IP_PATTERN = re.compile("("
                        "([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\."
                        "){3}"
                        "([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])")


PORT_PATTERN = re.compile("\d+")
MIN_PORT_NUM = 1
MAX_PORT_NUM = 65535


WAIT_TIME = 30


server_logger = logging.getLogger(__name__)


class PostgresClient(object):

    """
    A client to work with PostgreSQL via aiopg  lib.
    """

    def __init__(self, config):

        self.config = config

    async def create_pg_connection_pool(self, app: Application) -> Application:
        config = self.config['databases']['postgres']

        app['pool'] = await aiopg.create_pool(**config)

        return app

    async def close_pg_pool(self, app: Application):
        app['pool'].close()
        await app['pool'].wait_closed()

    async def execute(self, conn: aiopg.Connection, sql: str,
                      binds=None, fetch=True):

        async with conn.cursor() as cursor:
            await cursor.execute(sql, binds)

            if fetch:
                result = await cursor.fetchall()
                return result


class ServicesHandler(object):

    """
    A handler serving an end point '/service'.
    E.g. /service?ip=0.0.0.0&port=53
    """

    SELECT_SERVICE_SQL = 'SELECT ip, port, available ' \
                         'FROM services ' \
                         'WHERE ip = %s'

    OK = 200
    BAD_REQUEST = 400
    NOT_FOUND = 404
    SERVER_ERROR = 500

    def __init__(self, pg_client: PostgresClient):

        self.pg_client = pg_client

    async def get_service_info(self, request: web.Request) -> web.Response:

        ip = request.query.get("ip", None)
        port = request.query.get("port", None)

        if not self.valid_ip(ip):
            data = {'status': self.BAD_REQUEST,
                    'reason': 'Invalid IP address'}
            return self.make_response(data)

        if not self.valid_port(port):
            data = {'status': self.BAD_REQUEST,
                    'reason': 'Invalid port value'}
            return self.make_response(data)

        async with request.app['pool'].acquire() as conn:
            try:
                sql = self.SELECT_SERVICE_SQL
                binds = ip,

                data_rows = await self.pg_client.execute(conn, sql, binds)

                if not data_rows:
                    data = {'status': self.NOT_FOUND,
                            'reason': 'IP does not exist'}
                    return self.make_response(data)

                entities = self.build_enities(data_rows)

                if port is None:
                    data = {'status': self.OK, 'services': entities}
                    return self.make_response(data)

                entity_by_port = self.get_by_port(entities, int(port))

                data = {'status': self.OK, 'services': entity_by_port}

            except Exception as e:
                data = {'status': self.SERVER_ERROR, 'reason': str(e)}
                server_logger.critical(e, exc_info=True)

            return self.make_response(data)

    @staticmethod
    def make_response(data: dict) -> web.Response:

        return web.Response(text=json.dumps(data),
                            status=data['status'],
                            content_type='application/json')

    @staticmethod
    def get_by_port(entities: list, port: int) -> list:

        for entity in entities:
            if entity['port'] == port:
                return [entity]
        return []

    @staticmethod
    def build_enities(data: list) -> list:

        converted = []
        for row in data:
            converted.append({'ip': row[0],
                              'port': int(row[1]),
                              'available': row[2]})

        return converted

    @staticmethod
    def valid_ip(ip: str) -> bool:

        if not IP_PATTERN.match(ip):
            return False
        return True

    @staticmethod
    def valid_port(port: str) -> bool:

        if port and any([not PORT_PATTERN.match(port),
                         int(port) < MIN_PORT_NUM,
                         int(port) > MAX_PORT_NUM]):
            return False
        return True


class PortScanningHandler(object):

    """
    Background task that updates the state of a service
    every 30 second
    """

    SELECT_SERVICES_SQL = "SELECT ip, port, available FROM services"
    UPDATE_SERVICES_SQL = "UPDATE services SET available = %s " \
                          "WHERE ip = %s and port = %s"

    SCAN_TIMEOUT = 0.5

    def __init__(self, wait_time: int, pg_client: PostgresClient):

        self.wait_time = wait_time
        self.pg_client = pg_client

    async def scan_ports(self, app: Application):

        sql_select = self.SELECT_SERVICES_SQL
        sql_update = self.UPDATE_SERVICES_SQL

        async with app['pool'].acquire() as conn:
            while True:
                try:
                    services = await self.pg_client.execute(conn, sql_select)
                except psycopg2.Error as e:
                    server_logger.exception(e)
                    break

                for service in services:
                    await self._update_service_info(service, conn, sql_update)

                await asyncio.sleep(self.wait_time)

    async def _update_service_info(self, service: tuple,
                                   conn: aiopg.Connection,
                                   sql: str):

        ip = service[0]
        port = int(service[1])
        available = service[2]

        try:
            await asyncio.wait_for(
                asyncio.open_connection(ip, port), timeout=self.SCAN_TIMEOUT)
        except ConnectionRefusedError as e:
            available = False
            server_logger.info(f'{ip}:{port} is unavailable')
        else:
            available = True
        finally:
            binds = (available, ip, port)
            try:
                await self.pg_client.execute(conn, sql, binds, fetch=False)
            except psycopg2.Error as e:
                server_logger.exception(e)

    async def start_scanning_task(self, app: Application):
        app['scanner'] = app.loop.create_task(self.scan_ports(app))

    async def stop_scanning_task(self, app: Application):
        app['scanner'].cancel()
        await app['scanner']


def make_app() -> Application:

    """
    1. Initialize instances of a database client,
    service hander and port scanner.
    2. Initialize the app.

    :return: Application
    """

    pg_client = PostgresClient(CONFIG)
    service_handler = ServicesHandler(pg_client)
    scanning_handler = PortScanningHandler(WAIT_TIME, pg_client)

    app_ = Application()
    app_.on_startup.append(pg_client.create_pg_connection_pool)
    app_.on_startup.append(scanning_handler.start_scanning_task)
    app_.on_cleanup.append(pg_client.close_pg_pool)
    app_.on_cleanup.append(scanning_handler.stop_scanning_task)
    app_.add_routes([web.get('/service', service_handler.get_service_info)])

    return app_


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    app = make_app()
    web.run_app(app)
