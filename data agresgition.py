from datetime import date, datetime, timedelta
import datetime as dtt
import csv
import sys
import os
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector  import Error
from mysql.connector.constants import ClientFlag
import configparser
import pathlib

config = configparser.ConfigParser()
config.read('/home/sdp/cred/db.ini')

host_sdp = config['mysql_sdp']['host']
user_sdp = config['mysql_sdp']['user']
passwd_sdp = config['mysql_sdp']['passwd']
db_sdp = config['mysql_sdp']['db']

tablename = 'area1.vmerge_rev_kabupaten'


def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta


def getrange(delta):
    currenttime = dtt.datetime.now()
    dayFirstTime = currenttime - timedelta(days=delta - 1)
    dayFirstDate = dayFirstTime.strftime("%Y-%m-%d")
    currentdate = currenttime.strftime("%Y-%m-%d")
    return dayFirstDate, currentdate

def export_to_sql(filename, tablename, y_m_d):

    try:
        connection = mysql.connector.connect(host=host_sdp, user=user_sdp, database=db_sdp, password=passwd_sdp, allow_local_infile=True)
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print(dtt.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]+"|Connected to MySQL Server version :"+db_Info+"\n")
    except Error as e:
        print("Error while connecting to MySQL. Operations Aborted.", e)
        sys.exit(1)
        
    print("Executing SQL: ", y_m_d)

    #delete current date
    cursor = connection.cursor()
    query = """DELETE FROM %s WHERE trx_date = '%s'
    """ % (tablename, y_m_d)
    print(query)
    cursor.execute(query)
    connection.commit()
    cursor.close()

    #insert the current file
    cursor = connection.cursor()
    query = """LOAD DATA LOCAL INFILE '%s' 
    INTO TABLE %s 
    FIELDS TERMINATED BY '|'
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 LINES
    """ % (filename, tablename)
    print(query)
    cursor.execute(query)
    connection.commit()
    cursor.close()
    
    #close the connection
    connection.close()           


def main():
    #range1, range2 = getrange(7)
    #range = [range1, range2]
    range = ["2019-10-01","2019-11-23"]

    date1 = datetime.strptime(range[0], '%Y-%m-%d')
    date2 = datetime.strptime(range[1], '%Y-%m-%d') + timedelta(days=1)

    print(dtt.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + "| Started")

    # this will iterate date
    for result in perdelta(date1, date2, timedelta(days=1)):

        try:
            
            process_tables = result.strftime('%Y%m')
            y_m_d = result.strftime('%Y-%m-%d')
            ymd = result.strftime('%Y%m%d')
            first_month_date = result.strftime('%Y-%m-01')
            y_m_d_1 = (datetime.strptime(y_m_d, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

            dir_out = "/home/sdp/raw_data/v_merge_revenue_daily_summary_kabupaten"

            prefix_output = "v_merge_revenue_daily_summary_kabupaten_nasional_"

            #export to sql
           
            try:
                directory=dir_out
                prefix = prefix_output
          
                filename = directory+"/"+prefix+y_m_d
               
                #check file exist first, if not skipped
                f = open(filename)
                f.close()
                export_to_sql(filename, tablename, y_m_d)
                
            except FileNotFoundError:
                print('SQL operations skipped '+ filename + 'does not exist')
                #sys.exit(1)
            
        except FileNotFoundError:
            line_file_input1 = 0
            print('Operations Skipped ' + file_input_now + " does not exist")
            #sys.exit(1)

    #print("\n" + dtt.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + "| Finished")


if __name__ == "__main__":
    main()

