import pg8000

class DatabaseOperations() :

    """This is a wrapper class to speed up database operations with the psycopg2 modules"""

    def __init__(self,credentials,log):
        """Constructor used to establish connections to the database_jira"""
        self.log_local=log
        try:
            self.db_connection = pg8000.connect(
            database = credentials['DB_NAME'],
            user = credentials['DB_USER'],
            password = credentials['DB_PASS'],
            host = credentials['ADDRESS'],
            port = 5432)
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

    def __del__(self):
        self.db_connection.close()