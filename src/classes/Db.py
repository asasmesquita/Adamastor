import oracledb
import os


#one connection per query to generate and kill the connection each time
class Db:
    #environmental variables set on local machine
    USER = "AGRIVIEW_PROD_DB_USER"
    PASSWORD= "AGRIVIEW_PROD_DB_PASSWORD"
    HOST= "AGRIVIEW_PROD_DB_HOST"
    PORT= "AGRIVIEW_PROD_DB_PORT"
    SERVICE= "AGRIVIEW_PROD_DB_SERVICE"
    #AGRIVIEW PMEF VIEW
    TABLE= "AGRIVIEW_PMEF.VW_FACT_INDICATOR_USR"
    
    def __init__(self):
        #getting environmental variables needed for connection
        self.__user = os.getenv(Db.USER)
        if self.__user is None:
            raise ValueError(f"{Db.USER} environmental variable is not set.")
        
        self.__password = os.getenv(Db.PASSWORD)
        if self.__password is None:
            raise ValueError(f"{Db.PASSWORD} environmental variable is not set.")
        
        self.__host = os.getenv(Db.HOST)
        if self.__host is None:
            raise ValueError(f"{Db.HOST} environmental variable is not set.")
        
        self.__port = os.getenv(Db.PORT)
        if self.__port is None:
            raise ValueError(f"{Db.PORT} environmental variable is not set.")
        
        self.__service = os.getenv(Db.SERVICE)
        if self.__service is None:
            raise ValueError(f"{Db.SERVICE} environmental variable is not set.")
        
        pass


    def GetAllPmefCodes(self)->list:
        result = []
        try:
            sql = r"""
                SELECT DISTINCT ("Code")
                FROM """ + Db.TABLE + r"""
                WHERE "Layer" = 'PUBLICATION'
                ORDER BY "Code"
            """
            with oracledb.connect(
                user= self.__user,
                password= self.__password,
                host= self.__host,
                port= self.__port,
                service_name= self.__service
            ) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                result = cursor.fetchall()
            
        except Exception as ex:
            print(f"Unable to execute {sql} on {Db.TABLE}: {ex}")
        finally:
            return result
    
    def GetAllPmefPublication(self)-> list:
        result = []
        try:
            sql = r"""
                    SELECT "Code", "Sub-indicator", "Member State", "Year", "Value"
                    FROM """ + Db.TABLE + r"""
                    WHERE "Layer" = 'PUBLICATION' AND "Member State" != 'European Union'
                """
            with oracledb.connect(
                user= self.__user,
                password= self.__password,
                host= self.__host,
                port= self.__port,
                service_name= self.__service
            ) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                result = cursor.fetchall()

        except Exception as ex:
            print(f"Unable to execute {sql} on {Db.TABLE}: {ex}")

        finally:
            return result
        