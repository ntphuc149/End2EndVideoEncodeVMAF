import os
from dotenv import load_dotenv
from mysql.connector import pooling, Error
from conf.log_config import logger

load_dotenv()

class Config:
    MYSQL_HOST = os.getenv('MYSQL_HOST')
    MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
    MYSQL_USER = os.getenv('MYSQL_USER')
    MYSQL_PASS = os.getenv('MYSQL_PASS')
    MYSQL_DB = os.getenv('MYSQL_DB')

    @classmethod
    def validate(cls):
        required = ['MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PASS', 'MYSQL_DB']
        missing = [key for key in required if not getattr(cls, key)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


class MySqlConnectionPool:
    _instance = None
    _pool = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MySqlConnectionPool, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._pool is None:
            try:
                Config.validate()
                
                self._pool = pooling.MySQLConnectionPool(
                    pool_name='mypool',
                    pool_size=5,
                    pool_reset_session=True,
                    host=Config.MYSQL_HOST,
                    port=Config.MYSQL_PORT,
                    user=Config.MYSQL_USER,
                    password=Config.MYSQL_PASS,
                    database=Config.MYSQL_DB,
                    charset='utf8mb4',
                    collation='utf8mb4_general_ci'
                )
                logger.info("MySQL connection pool created successfully")
            except Error as e:
                logger.error(f'Error while connecting to MySQL using connection pool: {e}')
                raise
            except ValueError as e:
                logger.error(str(e))
                raise

    def get_connection(self):
        """Get a connection from the pool."""
        try:
            if self._pool is None:
                raise Error("Connection pool not initialized")
            return self._pool.get_connection()
        except Error as e:
            logger.error(f'Error getting connection from pool: {e}')
            raise


class DBAccess:
    def __init__(self):
        self._pool = MySqlConnectionPool()

    def execute_query(self, query: str, params=None):
        connection = None
        cursor = None
        try:
            connection = self._pool.get_connection()
            cursor = connection.cursor(dictionary=True)
            cursor.execute(query, params)
            
            results = []
            while True:
                try:
                    result = cursor.fetchall()
                    if result:
                        results.extend(result)
                    if not cursor.nextset():
                        break
                except:
                    break
                    
            return results if results else None
            
        except Error as e:
            logger.error(f'Error executing query: {e}')
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Error:
                    pass
            if connection:
                try:
                    connection.close()
                except Error:
                    pass
    
    def get_available_codec_names(self, key=None):
        sql_prompt = '''
        CALL get_available_codec_name()
        '''
        rows = self.execute_query(sql_prompt)
        return rows
    
    def get_available_profile_names(self, codec_name: str):
        sql_prompt = '''CALL get_available_profile_name(%s)'''
        rows = self.execute_query(sql_prompt, (codec_name,))
        return rows
    
    def get_profile_detail(self, codec_name: str, profile_name: str):
        sql_prompt = '''CALL get_profile_detail(%s, %s)'''
        rows = self.execute_query(sql_prompt, (codec_name, profile_name))
        return rows
