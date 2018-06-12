# -- coding: utf-8 --

import re
import json

import aiopg

from aiohttp import web
from aiohttp.web import Application


CONFIG = {
    "databases": {
        "postgres": {
            "host": "localhost",
            "port": 5432,
            "database": "public",
            "user": "postgres",
            "password": "r00t"
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


class PostgresClient(object):

    def __init__(self, config):

        self.config = config

    async def create_pg_connection_pool(self, app: Application) -> Application:
        config = self.config['databases']['postgres']

        app['pool'] = await aiopg.create_pool(**config)

        return app

    async def close_pg_pool(self, app: Application):
        app['pool'].close()
        await app['pool'].wait_closed()

    async def execute(self, conn: aiopg.Connection, sql: str, binds=None):
        async with conn.cursor() as cursor:
            await cursor.execute(sql, binds)

            result = await cursor.fetchall()

        return result


class ServicesHandler(object):

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

        async with request.app['pool'].acquire() as conn:
            try:
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

                sql = self.SELECT_SERVICE_SQL
                binds = ip,

                data_rows = await self.pg_client.execute(conn, sql, binds)

                if not data_rows:
                    data = {'status': self.NOT_FOUND,
                            'reason': 'IP does not exist'}
                    return self.make_response(data)

                entities = self.build_enities(data_rows)

                if port is None:
                    data = {'status': self.OK,
                            'services': entities}
                    return self.make_response(data)

                port = int(port)
                entity_by_port = self.get_by_port(entities, port)

                data = {'status': self.OK, 'services': entity_by_port}

            except Exception as e:
                data = {'status': self.SERVER_ERROR, 'reason': str(e)}

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


def make_app() -> web.Application:

    pg_client = PostgresClient(CONFIG)

    service_handler = ServicesHandler(pg_client)

    app_ = Application()

    app_.on_startup.append(pg_client.create_pg_connection_pool)
    app_.on_cleanup.append(pg_client.close_pg_pool)

    app_.add_routes([web.get('/service', service_handler.get_service_info)])

    return app_


if __name__ == "__main__":

    app = make_app()
    web.run_app(app)
