from psycopg2.pool import SimpleConnectionPool
from configparser import ConfigParser

def engine_conn_string(string):
    d = db(string)
    return f'postgresql://{d.params["user"]}:{d.params["password"]}@{d.params["host"]}:{d.params["port"]}/{d.params["dbname"]}'

# engine_conn_st

class db:
    def __init__(self, keyword = None):
        if keyword == None:
            self.params = config()
            self.str_1 = SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
            self.str = self.str_1.getconn()
        else:
            if "dimadev" in keyword:
                self.params = config(section=f'{keyword}')
                self.params['options'] = "-c search_path=dimadev"
                self.str_1 = SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
                self.str = self.str_1.getconn()


            else:
                self.params = config(section=f'{keyword}')
                self.params['options'] = "-c search_path=public"
                self.str_1 = SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
                self.str = self.str_1.getconn()

def config(filename='src/utils/database.ini', section='postgresql'):
    """
    Uses the configpaser module to read .ini and return a dictionary of
    credentials
    """
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(
        section, filename))

    return db