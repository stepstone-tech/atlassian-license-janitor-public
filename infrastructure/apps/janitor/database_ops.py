import pg8000
from sshtunnel import SSHTunnelForwarder


class DatabaseOperations:
    """Wrapper class to speed up database operations with the pg8000 module.

    Connects either directly to the database or through an SSH tunnel,
    depending on whether the credentials include an SSH_PROXY_HOST.
    """

    def __init__(self, credentials, log, local_port=None, use_ssh_tunnel=None):
        """Constructor used to establish a connection to the database"""
        self.log_local = log
        self.ssh_tunnel = None
        self.db_connection = None

        if use_ssh_tunnel is None:
            use_ssh_tunnel = bool(credentials.get('SSH_PROXY_HOST'))

        remote_port = int(credentials.get('PORT', 5432))

        if use_ssh_tunnel:
            if local_port is None:
                raise ValueError("local_port is required when using an SSH tunnel")
            self.ssh_tunnel = SSHTunnelForwarder(
                (credentials['SSH_PROXY_HOST'], 22),
                ssh_username=credentials['SSH_USERNAME'],
                ssh_private_key=credentials['SSH_PRIVATE_KEY'],
                ssh_private_key_password=credentials.get('SSH_PRIVATE_KEY_PASSWORD') or None,
                local_bind_address=('localhost', local_port),
                remote_bind_address=(credentials['ADDRESS'], remote_port))
            self.ssh_tunnel.start()
            if self.ssh_tunnel.is_active:
                self.log_local.info("SSH tunnel established")
            else:
                self.log_local.error("Failed to establish SSH tunnel")
            db_host = self.ssh_tunnel.local_bind_host
            db_port = self.ssh_tunnel.local_bind_port
        else:
            db_host = credentials['ADDRESS']
            db_port = remote_port

        try:
            self.db_connection = pg8000.connect(
                database=credentials['DB_NAME'],
                user=credentials['DB_USER'],
                password=credentials['DB_PASS'],
                host=db_host,
                port=db_port)
        except pg8000.Error as dberror:
            self.log_local.error("Failed to establish db connection")
            self.log_local.exception(dberror)
            self._stop_tunnel()
            raise

    def select(self, sql, params=()):
        """Executes select passed in "sql" on the database, returns result"""
        if sql is None:
            self.log_local.error("Empty querry")
            raise ValueError("Empty querry")
        try:
            db_cursor = self.db_connection.cursor()
            db_cursor.execute(sql, params)
            result = db_cursor.fetchall()
            db_cursor.close()
            return result
        except pg8000.Error as dberror:
            self.log_local.exception(dberror)
            raise

    def update(self, sql, params=()):
        """Executes update passed in "sql" on the database, returns amount of updated rows"""
        if sql is None:
            self.log_local.error("Empty querry")
            raise ValueError("Empty querry")
        try:
            db_cursor = self.db_connection.cursor()
            db_cursor.execute(sql, params)
            result = db_cursor.rowcount
            db_cursor.close()
            self.db_connection.commit()
            return result
        except pg8000.Error as dberror:
            self.log_local.exception(dberror)
            self.db_connection.rollback()
            raise

    def rollback(self):
        """Rolls back the current transaction"""
        if self.db_connection is not None:
            try:
                self.db_connection.rollback()
            except pg8000.Error as e:
                self.log_local.exception(e)
        else:
            self.log_local.error("Cannot rollback, no database connection established.")

    def _stop_tunnel(self):
        if self.ssh_tunnel is not None:
            try:
                self.ssh_tunnel.stop()
            except Exception:
                self.log_local.exception("Error stopping SSH tunnel")
            finally:
                self.ssh_tunnel = None

    def close(self):
        """Closes the database connection and, if used, the SSH tunnel. Safe to call more than once."""
        if self.db_connection is not None:
            try:
                self.db_connection.close()
            except Exception:
                self.log_local.exception("Error closing database connection")
            finally:
                self.db_connection = None
        self._stop_tunnel()

    def __del__(self):
        self.close()
