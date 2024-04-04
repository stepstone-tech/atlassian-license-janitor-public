from sshtunnel import SSHTunnelForwarder
import pg8000

class DatabaseOperations() :

    """This is a wrapper class to speed up database operations with the psycopg2 modules"""

    def __init__(self,credentials,log,local_port):
        """Constructor used to establish connections to the database_jira"""
        self.log_local=log
        self.ssh_tunnel = SSHTunnelForwarder(
            (credentials['SSH_PROXY_HOST'], 22),
            ssh_username=credentials['SSH_USERNAME'],
            ssh_private_key=credentials['SSH_PRIVATE_KEY'],
            local_bind_address=('localhost', local_port),
            remote_bind_address=(credentials['ADDRESS'],5432))
        self.ssh_tunnel.start()
        if self.ssh_tunnel.is_active:
            self.log_local.info("SSH tunnel established")
        else:
            self.log_local.error("Failed to establish SSH tunnel")
        try:
            self.db_connection = pg8000.connect(
            database = credentials['DB_NAME'],
            user = credentials['DB_USER'],
            password = credentials['DB_PASS'],
            host = self.ssh_tunnel.local_bind_host,
            port = self.ssh_tunnel.local_bind_port)
        except pg8000.DatabaseError as dberror:
            self.log_local.error("Failed to establish db connection")
            self.log_local.exception(dberror)

    def select(self, sql):
        """Executes select passed in "sql" on the database, returns result"""
        if sql is None:
            self.log_local.error("Empty querry")
            raise ValueError("Empty querry")
        try:
            db_cursor = self.db_connection.cursor()
            db_cursor.execute(sql)
            result = db_cursor.fetchall()
            db_cursor.close()
            return result
        except pg8000.DatabaseError as dberror:
            self.log_local.exception(dberror)

    def update(self,sql):
        """Executes update passed in "sql" on the database, returns amount of updated rows"""
        if sql is None:
            self.log_local.error("Empty querry")
            raise ValueError("Empty querry")
        try:
            db_cursor = self.db_connection.cursor()
            db_cursor.execute(sql)
            result = db_cursor.rowcount
            db_cursor.close()
            self.db_connection.commit()
            return result
        except pg8000.DatabaseError as dberror:
            self.log_local.exception(dberror)
            self.db_connection.rollback()

    def rollback(self):
        """Rolls back the current transaction"""
        if self.db_connection is not None:
            try:
                self.db_connection.rollback()
            except pg8000.DatabaseError as e:
                self.log_local.exception(e)
        else:
            self.log_local.error("Cannot rollback, no database connection established.")
