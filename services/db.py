import asyncpg
from .config import DbConfig


class Postgres:
    def __init__(self):
        self.pool = None

    async def connect(self, db_config : DbConfig):
        conf = {
            "user": db_config.user,
            "password": db_config.password,
            "database": db_config.database,
            "host": db_config.host,
            "port": db_config.port,
        }
        self.pool = await asyncpg.create_pool(**conf)

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def fetch(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def execute(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

its_db = Postgres()
spbe_db = Postgres()
