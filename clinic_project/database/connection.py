# database/connection.py
import os
import logging
import time
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, TimeoutError, OperationalError
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class DatabasePool:
    _instance = None
    _engine = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabasePool, cls).__new__(cls)
            cls._instance._initialize_pool()
        return cls._instance

    def _validate_environment(self):
        """التحقق من وجود متغيرات البيئة الأساسية."""
        required_vars = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_NAME']
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise EnvironmentError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

    def _initialize_pool(self):
        """تهيئة تجمع الاتصالات باستخدام SQLAlchemy."""
        self._validate_environment()

        try:
            db_user = os.getenv('DB_USER')
            db_password = os.getenv('DB_PASSWORD')
            db_host = os.getenv('DB_HOST')
            db_port = os.getenv('DB_PORT', '5432')
            db_name = os.getenv('DB_NAME')

            db_url = (
                f"postgresql+psycopg2://{db_user}:{db_password}"
                f"@{db_host}:{db_port}/{db_name}"
            )

            self._engine = create_engine(
                db_url,
                poolclass=QueuePool,
                pool_size=int(os.getenv('DB_MAX_CONNECTIONS', 50)),
                max_overflow=int(os.getenv('DB_OVERFLOW', 10)),
                pool_timeout=int(os.getenv('DB_POOL_TIMEOUT', 30)),
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args={
                    "sslmode": os.getenv('DB_SSLMODE', 'require'),
                    "connect_timeout": int(os.getenv('DB_CONNECT_TIMEOUT', 10)),
                    "keepalives": 1,
                    "keepalives_idle": 5,
                    "keepalives_interval": 2,
                }
            )
            logger.info(
                f"SQLAlchemy connection pool created: pool_size={self._engine.pool.size()}, "
                f"max_overflow={self._engine.pool._max_overflow}"
            )

            # تحذير إذا كان حجم التجمع يقترب من حد PostgreSQL
            try:
                with self._engine.connect() as conn:
                    result = conn.execute(text("SHOW max_connections")).fetchone()
                    pg_max = int(result[0])
                    pool_total = self._engine.pool.size() + self._engine.pool._max_overflow
                    if pool_total >= pg_max * 0.8:
                        logger.warning(
                            f"Connection pool total ({pool_total}) is near PostgreSQL limit ({pg_max})"
                        )
            except Exception as e:
                logger.warning(f"Could not check PostgreSQL max_connections: {e}")

        except Exception as e:
            logger.exception(f"Failed to initialize connection pool: {e}")
            raise

    @contextmanager
    def get_connection(self):
        conn = None
        start = time.time()
        try:
            conn = self._engine.connect()
            elapsed = time.time() - start
            if elapsed > 1.0:
                logger.warning(f"Slow connection acquisition: {elapsed:.2f}s")
            yield conn
            conn.commit()
        except TimeoutError as e:
            if conn:
                conn.rollback()
            logger.exception(f"Connection timeout: {e}")
            raise
        except OperationalError as e:
            if conn:
                conn.rollback()
            logger.exception(f"Operational error: {e}")
            raise
        except SQLAlchemyError as e:
            if conn:
                conn.rollback()
            logger.exception(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    @contextmanager
    def get_cursor(self):
        with self.get_connection() as conn:
            cursor = conn.connection.cursor()
            start = time.time()
            try:
                yield cursor
                elapsed = time.time() - start
                if elapsed > 1.0:
                    logger.warning(f"Slow query detected: {elapsed:.2f}s")
            except Exception as e:
                logger.exception(f"Query error: {e}")
                raise
            finally:
                cursor.close()

    def health_check(self) -> bool:
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except Exception as e:
            logger.exception(f"Health check failed: {e}")
            return False

    def get_stats(self) -> dict:
        pool = self._engine.pool
        return {
            "size": pool.size(),
            "checked_in": pool.checkedin(),
            "overflow": pool.overflow(),
            "total": pool.size() + pool.overflow()
        }


db = DatabasePool()