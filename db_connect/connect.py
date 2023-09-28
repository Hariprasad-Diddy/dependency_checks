import pyodbc
from mysql.connector import Error
import mysql.connector
import jaydebeapi



class CommerceDB:
    """_summary_
    """
    def __init__(self) -> None:
        """_summary_
        """
        ...

    def db_connection(self,creds) -> None:
        """_summary_

        Args:
            creds (_type_): _description_

        Returns:
            _type_: _description_
        """
        self.cnxn = pyodbc.connect('DRIVER='+str(creds['driver'])+';SERVER='+str(creds['server'])+';PORT='+str(creds['port'])+';DATABASE='+str(creds['database'])+';UID='+str(creds['username'])+';PWD='+str(creds['password']))
        self.cnxn.autocommit = True

        return self.cnxn


class ClickstreamDB:
    """_summary_
    """
    def __init__(self) -> None:
        """_summary_
        """
        ...

    def db_connection(self,creds) -> None:
        """_summary_

        Args:
            creds (_type_): _description_

        Returns:
            _type_: _description_
        """
        self.cnxn = pyodbc.connect('DRIVER='+str(creds['driver'])+';SERVER='+str(creds['server'])+';PORT='+str(creds['port'])+';DATABASE='+str(creds['database'])+';UID='+str(creds['username'])+';PWD='+str(creds['password']))
        self.cnxn.autocommit = True

        return self.cnxn
    



class PersonifyDB:
    """_summary_
    """
    def __init__(self) -> None:
        """_summary_
        """
        ...

    def db_connection(self,creds) -> None:
        """_summary_

        Args:
            creds (_type_): _description_

        Returns:
            _type_: _description_
        """
        try:
            connection = mysql.connector.connect(host=creds['host'],
                                                database=creds['database'],
                                                user=creds['user'],
                                                password=creds['password'])
            if connection.is_connected():
                db_Info = connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                print(f"You're connected to {creds['host']} database: ")
                
                return connection
        except Error as e:
            print("Error while connecting to MySQL", e)


class AdhocQueryDB:
    """_summary_
    """
    def __init__(self) -> None:
        """_summary_
        """
        ...
    
    def db_connection(self,creds) -> None:
        """_summary_

        Args:
            creds (_type_): _description_

        Returns:
            _type_: _description_
        """
        try:
            connection = mysql.connector.connect(host=creds['host'],
                                                database=creds['database'],
                                                user=creds['user'],
                                                password=creds['password'])
            if connection.is_connected():
                db_Info = connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                print(f"You're connected to {creds['host']} database: ")
                
                return connection
        except Error as e:
            print("Error while connecting to MySQL", e)


class RBACDB:
    """_summary_
    """
    def __init__(self) -> None:
        """_summary_
        """
        ...
    
    def db_connection(self,creds) -> None:
        """_summary_

        Args:
            creds (_type_): _description_

        Returns:
            _type_: _description_
        """
        try:
            connection = mysql.connector.connect(host=creds['host'],
                                                database=creds['database'],
                                                user=creds['user'],
                                                password=creds['password'])
            if connection.is_connected():
                db_Info = connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                print(f"You're connected to {creds['host']} database: ")
                
                return connection
            
        except Error as e:
            print("Error while connecting to MySQL", e)


class ErrorManagementDB:
    """_summary_
    """
    def __init__(self) -> None:
        """_summary_
        """
        ...
    
    def db_connection(self,creds) -> None:
        """_summary_

        Args:
            creds (_type_): _description_

        Returns:
            _type_: _description_
        """
        try:
            connection = mysql.connector.connect(host=creds['host'],
                                                database=creds['database'],
                                                user=creds['user'],
                                                password=creds['password'])
            if connection.is_connected():
                db_Info = connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                print(f"You're connected to {creds['host']} database: ")
                
                return connection
            
        except Error as e:
            print("Error while connecting to MySQL", e)


class BifrostDB:
    """ Test Bifrost DB connection"""
    def __init__(self) -> None:
        """_summary_
        """
        ...
    
    def db_connection(self,creds) -> None:
        """_summary_

        Args:
            creds (_type_): _description_

        Returns:
            _type_: _description_
        """
        try:
            path = '/home/myntra/Drivers/bifrost-jdbc-2.0-20210826.145828-2.jar'
            connection = jaydebeapi.connect("com.myntra.bifrost.jdbc.BifrostDriver",
                              "jdbc:bifrost://10.162.140.23:6080/hive",
                          ['mdpdi.platform', '4Rti0LKm3Wkh'],
                          path)
            # connection = mysql.connector.connect(host=creds['host'],
            #                                     database=creds['database'],
            #                                     user=creds['user'],
            #                                     password=creds['password'])
            if connection.is_connected():
                db_Info = connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                print(f"You're connected to {creds['host']} database: ")
                
                return connection
            
        except Error as e:
            print("Error while connecting to MySQL", e)