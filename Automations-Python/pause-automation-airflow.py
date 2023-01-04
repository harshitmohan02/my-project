# from ipaddress import ip_address
# from urllib import response
# import requests
# from requests.auth import HTTPBasicAuth
# import urllib3
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# import json
# import sys
# import datetime
# import psycopg2
# import snowflake.connector
import pyodbc


username = ''
password = ''

def postgres_connect():
    conn = psycopg2.connect(
                            database="airflow_ip_address", 
                            user='audit_dbadmin', 
                            password='', 
                            host='batpsql-airflow-audit-dbserver-nprod.postgres.database.azure.com', 
                            port= '5432', 
                            sslmode = 'allow'
    )
    cursor = conn.cursor()
    cursor.execute('select * from ip_address;')
    ip = cursor.fetchall()
    print(str(datetime.datetime.now())+': PostgresSQL Connection Successfull')
    print(str(datetime.datetime.now())+": IP_Addresses: " + str(ip))
    conn.close()
    fetch_active_dags(ip)


def fetch_active_dags(ip):    
    result = []
    try:
        for m in range (len(ip)):
            print(str(datetime.datetime.now())+": Start Fetching Active DAGs on IP:" +ip[m][1])
            for i in range(0,25):
                URL = 'http://'+ip[m][1]+'/airflow/'
                headers = {'content-type': 'application/json'}
                k = i*100
            
                response = requests.request("GET",
                                    URL + f"api/v1/dags?offset={k}",
                                    auth=HTTPBasicAuth(username, password),
                                    headers=headers, data='{}')
                response.raise_for_status()
                json_str = json.dumps(response.json())
                resp = json.loads(json_str)
                print(str(datetime.datetime.now())+": Fetching Page :"+str(i))

                for j in range(0,len(resp['dags'])):
                    if str(resp['dags'][j]['is_paused']) == 'False':
                        value = resp['dags'][j]['dag_id']
                        result += [value]
            print(str(datetime.datetime.now())+": Active DAG IDs:"+str(result))
            print(str(datetime.datetime.now())+": Completed Fetching DAGs on IP:" +ip[m][1])
            pause_active_dags(ip[m][1],result)
    except Exception as e:
            print("Error: " + str(e))
    


def pause_active_dags(ip,result):
    print(str(datetime.datetime.now())+": Pausing Active DAGs on IP: " + str(ip))
    session = requests.Session()
    session.auth = (username, password)
    auth_headers = {'Content-type': 'application/json'}
    for dag in result:
        dag_unpause = session.patch(f'http://'+ip+f'/airflow/api/v1/dags/{dag}',headers = auth_headers, data='{"is_paused": true}')
    try:
        dag_unpause.raise_for_status()
        print(str(datetime.datetime.now())+": All Active DAGs Paused on IP: " + str(ip))
    except requests.exceptions.HTTPError as e:
        error = "Error: "+ str(e)
        print(error)     

def conn_sflake():
    #ctx = snowflake.connector.connect(user= sys.argv[1], password= sys.argv[2], account= sys.argv[3], role = sys.argv[4], warehouse= sys.argv[5], database= sys.argv[6], schema= sys.argv[7])
    ctx = snowflake.connector.connect(user= 'devops_app', password= '', account= 'fx54096.west-europe.azure', role = 'EDP_MONITORING_ENGINEER_FR', warehouse= 'EDP_MONITORING_SERVICE_ELT_WH', database= 'EDP_MONITORING_PRESENTATION', schema= 'MAIN')
    cursor_snow = ctx.cursor() 
    return(cursor_snow)

def ms_connect_dev(market):
    driver= '{ODBC Driver 17 for SQL Server}'
    #cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+sys.argv[8]+';PORT=1433;DATABASE='+sys.argv[9]+';UID='+sys.argv[10]+';PWD='+ sys.argv[11], autocommit=True)
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER=batsql-pp-we-edp-wscp-dev-01.database.windows.net;PORT=1433;DATABASE=WS_'+market+'_DEV;UID=wscp_dev_admin;PWD=', autocommit=True)
    cursor_sql = cnxn.cursor()
    return(cursor_sql)

def on_hold_jobs():
    # cursor_snow = conn_sflake()
    # command_sql = 'select SPOKE_NAME from SPOKE_VM;'
    # cursor_snow.execute(command_sql)
    # value = command_sql.fetchall()
    # print(value)

    value = ['GTR']

    for i in value:
        cursor_mssql = ms_connect_dev(i)
        command_sql = "UPDATE ws_wrk_job_ctrl SET wjc_status = 'H';"
        cursor_mssql.execute(command_sql)

def main():
    # print(str(datetime.datetime.now())+": Pausing DAGs on Airflow Started")
    # postgres_connect()
    # print(str(datetime.datetime.now())+": Pausing DAGs on Airflow Completed")
    # print("###############")
    # print(str(datetime.datetime.now())+": Pausing DAGs on WhereScape Started")
    on_hold_jobs()

main()