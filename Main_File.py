import tkinter as tk
import tkinter as Tk
from pprint import pprint
import pandas as pd
import os
import numpy as np
import math
import shelve
from datetime import datetime
from Connect_to_PGDB import * # getMCR_Master, getUrjas_data
import warnings
from tkinter import *
from tkinter import ttk
import threading as thd
import sys #Imports sys, used to end the program later
import calendar
import xlsxwriter
import re

## pyinstaller --windowed --onefile Main_File.py

warnings.filterwarnings("ignore")
os.system('cls')


def connectDB(DBIP,DBPort,DBUID,DBPwd,BaseDB,DBID):
   # print('Connecting Database: ',DBID)
   conn = None; cur = None
   try:
      # connect to the PostgreSQL server
      conn = psycopg2.connect(host=DBIP,port=DBPort,database=BaseDB,user=DBUID,password=DBPwd)
      cur = conn.cursor()
      if cur is not None:
         pass
         # print('Database connected Successfully..!!')
   except (Exception, psycopg2.DatabaseError) as error:
      print('DB Connection Failed. Error : ', error)
   return cur, conn




def fetchDBData(Prj_in, Module, qry, clmn):
    Project = {
        'MSEDCL': {
            'IP': '',
            'Port': '',
            'uid': 'DB username',
            'pwd': 'passcode',
            'HES': 'DB name',
            'WFM': 'DB name',
            'MDM': 'DB name'
        }
    }

    DBIP = Project[Prj_in]['IP']
    DBPort = Project[Prj_in]['Port']
    DBUID = Project[Prj_in]['uid']
    DBPwd = Project[Prj_in]['pwd']
    BaseDB = Project[Prj_in][Module]

    cur, conn = connectDB(DBIP, DBPort, DBUID, DBPwd, BaseDB, (str(Prj_in) + str('_') + str(Module)))
    
    if cur is not None:
        try:
            cur.execute(qry)
            data = cur.fetchall()
            # Always return a DataFrame, even if empty
            if data:
                df = pd.DataFrame(data, columns=clmn)
            else:
                df = pd.DataFrame(columns=clmn)
        except (Exception, psycopg2.DatabaseError) as error:
            print('DB Connection Failed. Error:', error)
            df = pd.DataFrame(columns=clmn)
        finally:
            cur.close()
            conn.close()
    else:
        df = pd.DataFrame(columns=clmn)
    return df




def fetchDBData_inparts(Prj_in,Module,qry,clmn):
   Project = {'TS1506':{'IP':'',
                        'Port':'',
                        'uid':'DB username',
                        'pwd':'passcode',
                        'HES':'DB name',
                        'MDM':'DB name'},
            '':{'IP':'""',
                        'Port':'',
                        'uid':'',
                        'pwd':'',
                        'HES':'',
                        'MDM':''}}

   DBIP = Project[Prj_in]['IP']
   DBPort = Project[Prj_in]['Port']
   DBUID = Project[Prj_in]['uid']
   DBPwd = Project[Prj_in]['pwd']
   BaseDB = Project[Prj_in][Module]
   
   cur, conn = connectDB(DBIP,DBPort,DBUID,DBPwd,BaseDB,(str(Prj_in) + str('_') + str(Module)))
   if cur is not None:
      try:
         cur.execute(qry)
         data = cur.fetchall()
         df = pd.DataFrame(data,columns=clmn)
      except (Exception, psycopg2.DatabaseError) as error:
         print('DB Connection Failed. Error : ', error)  
         df = []
      finally:
         cur.close()
         conn.close()
         # print('DB Connection Closed Successfully..!!')
   else:
      df = pd.DataFrame()
   return df


def getMin_Max_MI():
   Prj_in = 'MSEDCL'
   Module = 'WFM'
   clmn = ['MIDate']
   qry = "select min(survey_timings) from ami_master.survey_output"
   df = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   Min_date = df.loc[0,'MIDate']

   qry = "select max(survey_timings) from ami_master.survey_output"
   df = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   Max_date = df.loc[0,'MIDate']

   MMl = int(Max_date[5:7])
   YYl = int(Max_date[0:4])

   df1 = pd.DataFrame(columns= ['MM','YYYY'])
   
   flag = True
   ii = 0
   while flag:
      if ii == 0:
         MMi = int(Min_date[5:7])
         YYi = int(Min_date[:4])
      else:
         if MMi == 12:
            MMi = 1
            YYi = YYi + 1
         else:
            MMi = MMi + 1
      if MMi > 9:
         df1.loc[ii,'MM'] = str(MMi)
      else:
         df1.loc[ii,'MM'] = str(0) + str(MMi)
      df1.loc[ii,'YYYY'] = str(YYi)      
      ii = ii + 1
      if MMi == MMl and YYi == YYl: flag = False
   return df1


def getAll_MCR_Master():
## Get MCR from WFM..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'
   clmn = ['circle','subdivision','sdocode','feedercode','kno','survey_timings','newmeterno','newmetermake','connectiontype','verify_status','consumer_type','comm']
   df = pd.DataFrame(columns=clmn)
   timeFrm = getMin_Max_MI()
   for tmi in range(len(timeFrm)):
      MM = timeFrm.loc[tmi,'MM']
      YY = timeFrm.loc[tmi,'YYYY']
   
      Flag = True
      while Flag:
         isPass = 0
         # Fetching LT Consumer
         if True: # dtype == 'cons':
            msg_body = str("LT Consumer MI Data Received for ") + str(YY) + str("-") + str(MM) + str(' : ')
            try:
               qry1 = "select circle,subdivision,sdocode,feedercode,kno,survey_timings,newmeterno,newmetermake,connectiontype,verify_status,consumer_type,(select comm from ami_master.master_main where mtrno = survey_output.newmeterno limit 1) as comm \
                        from ami_master.survey_output where survey_timings like '"
               qry = qry1 + str(YY) + str("-") + str(MM) + "%'"
               # print(qry)
               df0 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
               if len(df0) > 0: 
                  isPass = 1
                  df = df._append(df0)
               print('')
               print(msg_body, len(df0), ', Total Count : ', len(df))
               msgBoard.set(str(msg_body) + str(len(df0)) + ', Total Count : ' + str(len(df)))
            except:
               print('')
               print(msg_body, 0)
               msgBoard.set(str(msg_body) + str(0))
         if isPass == 1: Flag = False

   # Fetching HTCT Consumer
   qry = "select circle,subdivision,sdocode,feedercode,kno,survey_timings,newmeterno,newmetermake,	\
            kno as connectiontype,verify_status,consumer_category as consumer_type,(select comm from ami_master.master_main where mtrno = htct_meter_installation_details.newmeterno limit 1) as comm from ami_master.htct_meter_installation_details"
   df0 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   df0['connectiontype'] = '4-Phase'
   df0['consumer_type'] = 'HTCT_Cons'
   
   df = df._append(df0)
   print("HTCT Consumer MI Data Received : ", len(df0), ', Total Count : ', len(df))

   # Fetching LTCT Consumer
   qry = "select circle,subdivision,sdocode,feedercode,kno,survey_timings,newmeterno,newmetermake,	\
          kno as connectiontype,verify_status,consumer_category as consumer_type,(select comm from ami_master.master_main where mtrno = ltct_meter_installation_details.newmeterno limit 1) as comm from ami_master.ltct_meter_installation_details"
   df1 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   df1['connectiontype'] = '34-Phase'
   df1['consumer_type'] = 'LTCT_Cons'
   print("LTCT Consumer MI Data Received : ", len(df1))
   df = df._append(df1)

   qry = "select circle,subdivision,sdocode,fdrcode,dtcode,surveyed_date,new_meter_serial_number,new_meter_make,new_meter_make,verify_status,verify_status \
          from ami_master.dt_meter_installation_data"
   df1 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from dt_meter_replacement_details table
   df1['connectiontype'] = 'LTCT_DT'
   df1['consumer_type'] = 'LTCT_DT'
   print("DT MI Data Received : ", len(df1))
   df = df._append(df1)

   qry = "select circle,subdivision,sdocode,fdrcode,fdrcode, surveyed_date,new_meter_serial_number,new_meter_make,new_meter_phase,verify_status,verify_status \
          from ami_master.feeder_meter_installation_data"
   df2 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from dt_meter_replacement_details table
   df2['connectiontype'] = 'HTCT_FD'
   df2['consumer_type'] = 'HTCT_FD'
   print("Feeder MI Data Received : ", len(df2))
   df = df._append(df2)
   return df


def getMCR_Master(dtype):
## Get MCR from WFM..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'
   clmn = ['circle', 'subdivision', 'sdocode', 'feedercode', 'kno', 'survey_timings', 'newmeterno', 'newmetermake', 'connectiontype', 'verify_status', 'consumer_type','comm']
   df = pd.DataFrame(columns=clmn)
   timeFrm = getMin_Max_MI()

   for tmi in range(len(timeFrm)):
    MM = timeFrm.loc[tmi, 'MM']
    YY = timeFrm.loc[tmi, 'YYYY']
    
    msg_body = f"LT Consumer MI Data Received for {YY}-{MM} : "
    
    try:
        qry = f"""
            SELECT circle, subdivision, sdocode, feedercode, kno, survey_timings, 
                   newmeterno, newmetermake, connectiontype, verify_status, consumer_type,(select comm from ami_master.master_main where mtrno = survey_output.newmeterno limit 1) as comm
            FROM ami_master.survey_output 
            WHERE survey_timings LIKE '{YY}-{MM}%'
        """
        df0 = fetchDBData(Prj_in, Module, qry, clmn)
        
        if len(df0) > 0:
            df = df._append(df0, ignore_index=True)
            print(f"\n{msg_body} {len(df0)}, Total Count : {len(df)}")
            msgBoard.set(f"{msg_body} {len(df0)}, Total Count : {len(df)}")
        else:
            print(f"\n{msg_body} 0")
            msgBoard.set(f"{msg_body} 0")
    
    except Exception as e:
        print(f"\n{msg_body} 0 (Error occurred)")
        msgBoard.set(f"{msg_body} 0")
        # Optionally log the exception
        print("Error:", e)


   # Fetching HTCT Consumer
   qry = "select circle,subdivision,sdocode,feedercode,kno,survey_timings,newmeterno,newmetermake,	\
            kno as connectiontype,verify_status,consumer_category as consumer_type,(select comm from ami_master.master_main where mtrno = htct_meter_installation_details.newmeterno limit 1) from ami_master.htct_meter_installation_details"
   df0 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   df0['connectiontype'] = '3-Phase'
   df0['consumer_type'] = 'HTCT_Cons'
   
   df = df._append(df0)
   print("HTCT Consumer MI Data Received : ", len(df0), ', Total Count : ', len(df))

   # Fetching LTCT Consumer
   qry = "select circle,subdivision,sdocode,feedercode,kno,survey_timings,newmeterno,newmetermake,	\
          kno as connectiontype,verify_status,consumer_category as consumer_type,(select comm from ami_master.master_main where mtrno = ltct_meter_installation_details.newmeterno limit 1) as comm from ami_master.ltct_meter_installation_details"
   df1 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   df1['connectiontype'] = '34-Phase'
   df1['consumer_type'] = 'LTCT_Cons'
   
   df = df._append(df1)
   print("LTCT Consumer MI Data Received : ", len(df1), ', Total Count : ', len(df))
   

   # if dtype == 'All': 
   #    qry = "select circle,subdivision,sdocode,fdrcode,dtcode,surveyed_date,new_meter_serial_number,new_meter_make,new_meter_make,verify_status,verify_status \
   #           from ami_master.dt_meter_installation_data"
   #    df1 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from dt_meter_replacement_details table
   #    df1['connectiontype'] = 'LTCT_DT'
   #    print("DT MI Data Received : ", len(df1))
   #    df = df._append(df1)

   # if dtype == 'All':  
   #    qry = "select circle,subdivision,sdocode,fdrcode,fdrcode, surveyed_date,new_meter_serial_number,new_meter_make,new_meter_phase,verify_status,verify_status \
   #           from ami_master.feeder_meter_installation_data"
   #    df2 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from dt_meter_replacement_details table
   #    df2['connectiontype'] = 'HTCT_FD'
   #    print("Feeder MI Data Received : ", len(df2))
   #    df = df._append(df2)
   return df


def getFDDT_MCR_Master(dtype):
## Get MCR from WFM..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'

   clmn = ['circle','subdivision','sdocode','substation','feedername','FD_DT_Code','survey_timings','newmeterno','newmetermake','connectiontype','verify_status'
            ,'OldMrtNo_Master','OldMrtMake_Master','OldMrtNo_Field','OldMrtMake_Field','OldMtr_kWh_Imp','OldMtr_kWh_Exp','NewMtr_kWh_Imp','NewMtr_kWh_exp','comm','Rejection Reason']
   df = pd.DataFrame()
   if dtype == 'FD': 
      qry = "select circle,subdivision,sdocode,substation,feedername,fdrcode,surveyed_date,new_meter_serial_number,new_meter_make,  \
             new_meter_phase,verify_status, feeder_meter_serial_number,feeder_meter_make,oldmtrno_in_field,oldmtrmake_in_field,\
			    kwh_old_meter_reading,kwh_old_meter_reading_export,kwh_new_meter_reading,kwh_new_meter_reading_export,(select comm from ami_master.master_main where mtrno = feeder_meter_installation_data.new_meter_serial_number limit 1) as comm,rej_reason\
             from ami_master.feeder_meter_installation_data"
      df1 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from dt_meter_replacement_details table
      df1['connectiontype'] = 'HTCT_FD'
      print("Feeder MI Data Received : ", len(df1))
      df = df._append(df1)

   if dtype == 'DT': 
      qry = "select circle,subdivision,sdocode,substation,feedername,dtcode,surveyed_date,new_meter_serial_number,new_meter_make,\
             new_meter_make,verify_status,mtr_sr_no,mtr_make,oldmtrno_in_field,oldmtrmake_in_field,old_mtrreading_kwh,\
             old_mtrreading_kwh,new_mtr_rdgkwh,new_mtr_rdgkwh,(select comm from ami_master.master_main where mtrno = dt_meter_installation_data.new_meter_serial_number limit 1) as comm,remark\
             from ami_master.dt_meter_installation_data"
      df2 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from dt_meter_replacement_details table
      df2['connectiontype'] = 'LTCT_DT'
      print("DT MI Data Received : ", len(df2))
      df = df._append(df2)
   
   return df


def getSAP_API_Sync():
## Get Tracker Data for WFM-SAP Data Sync..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'

   clmn = ['id',	'time_stamp',	'api_name',	'request_body',	'response_body',	'kno',	'meterno',	'flag']

   qry = "SELECT id,time_stamp,api_name,request_body,response_body,kno,meterno,flag FROM ami_master.sap_api_tracker WHERE api_name='pushAllInstallDataNew' order by time_stamp desc"
   df = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   print("SAP API Tracker Table Data Received : ", len(df))
   return df
    
    
    

def getFDDT_MMR_Data():
    """Get Tracker Data for WFM-SAP Data Sync..!!"""
    Prj_in = 'MSEDCL'
    Module = 'WFM'
    clmn = ['id', 'feeder_dt_code', 'survey_time', 'request_time', 'response_time', 
            'response', 'request_data', 'request_status', 'category', 'entry_by','api_type','request_id']

    qry = "select * from ami_master.arms_feeder_dt_replacement_tracking order by response_time desc"
    df = fetchDBData(Prj_in, Module, qry, clmn)
    
    if df.empty:
        print("No data received from SAP API Tracker Table")
        return pd.DataFrame(columns=clmn)
    
    print("SAP API Tracker Table Data Received:", len(df))
    return df
 
 
 

def getConsumer_MMR1_Data_old():
## Get Tracker Data for Consumer MMR L1 API..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'
   timeFrm = getMin_Max_MI()
   clmn = ['id','kno','meterno','survey_time','response_time','response','request_status','old_applicationid','old_response', \
         'old_entrydate','new_applicationid','new_response','new_entrydate','old_payload','new_payload','L1_meter_flag']
   df = pd.DataFrame(columns=clmn)
   
   for tmi in range(len(timeFrm)):
      MM = timeFrm.loc[tmi,'MM']
      YY = timeFrm.loc[tmi,'YYYY']
   
      Flag = True
      while Flag and tmi > 0:
         isPass = 0
         # Fetching LT Consumer
         if True: # dtype == 'cons':
            msg_body = str("Consumer MMR L1 API Tracker Data Received for ") + str(YY) + str("-") + str(MM) + str(' :')
            try:
               qry1 = "select id,kno,meterno,survey_time,response_time,response,request_status,old_applicationid,old_response,old_entrydate, \
                        new_applicationid,new_response,new_entrydate,old_payload,new_payload, meter_flag \
                        from ami_master.arms_mtr_replacement_tracking where EXTRACT(MONTH FROM response_time) = "
               
               #  id >= 30000 and id < 40000

               qry = qry1 + str(int(MM)) + str(" and EXTRACT(YEAR FROM response_time) = ") + str(int(YY)) + str(" order by meterno, request_time")
               # print(qry)
               df0 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
               if len(df0) > 0: 
                  isPass = 1
                  df = df._append(df0)
               print('')
               print(msg_body, len(df0), ', Total Count : ', len(df))
            except:
               print('')
               print(msg_body, 0)

         if isPass == 1: Flag = False
   return df


def getConsumer_MMR1_Data():
    ## Get Tracker Data for Consumer MMR L1 API..!!
    Prj_in = 'MSEDCL'
    Module = 'WFM'
    clmn = ['count']

    qry = "select max(id) from ami_master.arms_mtr_replacement_tracking"
    df0 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
    tblCount = df0.loc[0,'count']
    aa1 = math.ceil(tblCount / 10000)

    # Ensure all required columns are included
    clmn = ['id','kno','meterno','survey_time','response_time','response','request_status',
            'old_applicationid','meter_flag','old_entrydate',
            'new_applicationid','new_entrydate']
    df = pd.DataFrame(columns=clmn)

    for tmi in range(aa1):
        r1 = tmi * 10000
        r2 = r1 + 10000
        Flag = True
        while Flag:
            isPass = 0
            msg_body = str("Consumer MMR L1 API Tracker Data Received for Lot ") + str(tmi+1) + str(' : ')
            try:
                qry1 = "select id,kno,meterno,survey_time,response_time,response,request_status,old_applicationid,meter_flag,old_entrydate, \
                        new_applicationid,new_entrydate \
                        from ami_master.arms_mtr_replacement_tracking where id >= "

                qry = qry1 + str(int(r1)) + str(" and id < ") + str(int(r2)) + str(" order by meterno, request_time")
                df0 = fetchDBData(Prj_in,Module,qry,clmn)
                if len(df0) > 0: 
                    isPass = 1
                    df = df._append(df0)
                print('')
                print(msg_body, len(df0), ', Total Count : ', len(df))
                msgBoard.set(str(msg_body) + str(len(df0)) + ', Total Count : ' + str(len(df)))
            except Exception as e:
                print('')
                print(msg_body, 0, "Error:", e)
                msgBoard.set(str(msg_body) + str(0) + " Error: " + str(e))
            if isPass == 1: Flag = False
    
    # Add L1_meter_flag column if it doesn't exist
    if 'meter_flag' in df.columns:
        df['L1_meter_flag'] = df['meter_flag']
    else:
        df['L1_meter_flag'] = ''  # or whatever default value makes sense
    
    return df




def getConsumer_MMR2_Data_old():
## Get Tracker Data for Consumer MMR L2 API..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'

   clmn = ['smart_meter_flag',	'api_execution_status_code',	'api_execution_status_message',	'application_id',	'sdocode',	'kno', \
           	'current_workflow_status',	'current_workflow_status_id',	'remark',	'replacement_date',	'entry_date',	'mastertable_status','L2_meter_flag']

   qry = "select smart_meter_flag,api_execution_status_code,api_execution_status_message,application_id,sdocode,kno, current_workflow_status, \
            current_workflow_status_id,remark,replacement_date,entry_date,mastertable_status,meter_flag as L2_meter_flag \
               from ami_master.discom_mtr_replacement_tracking order by entry_date desc"
   df = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   print("Consumer MMR L2 API Tracker Data Received : ", len(df))

   return df


def getConsumer_MMR2_Data():
## Get Tracker Data for Consumer MMR L2 API..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'
   clmn = ['count']

   qry = "select max(id) from ami_master.discom_mtr_replacement_tracking"
   df0 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   tblCount = df0.loc[0,'count']
   aa1 = math.ceil(tblCount / 10000)

   clmn = ['smart_meter_flag',	'api_execution_status_code',	'api_execution_status_message',	'application_id',	'sdocode',	'kno', \
           	'current_workflow_status',	'current_workflow_status_id',	'remark',	'replacement_date',	'entry_date',	'mastertable_status','L2_meter_flag']
   df = pd.DataFrame(columns=clmn)

   for tmi in range(aa1):
      r1 = tmi * 10000
      r2 = r1 + 10000
      Flag = True
      while Flag:
         isPass = 0
         
         
         if True:
            msg_body = str("Consumer MMR L2 API Tracker Data Received for Lot ") + str(tmi+1) + str(' :')
            try:
               qry1 = "select smart_meter_flag,api_execution_status_code,api_execution_status_message,application_id,sdocode,kno, current_workflow_status, \
                           current_workflow_status_id,remark,replacement_date,entry_date,mastertable_status,meter_flag as L2_meter_flag \
                           from ami_master.discom_mtr_replacement_tracking where id >= "

               qry = qry1 + str(int(r1)) + str(" and id < ") + str(int(r2)) + str(" order by entry_date desc")
               # print(qry)
               df0 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
               if len(df0) > 0: 
                  isPass = 1
                  df = df._append(df0)
               print('')
               print(msg_body, len(df0), ', Total Count : ', len(df))
            except:
               print('')
               print(msg_body, 0)

         if isPass == 1: Flag = False
   return df


def getHTCT_MI_MMR_Data():
## Get MCR from WFM..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'
   clmn = ['circle','subdivision','sdocode','feedercode','kno','survey_timings','newmeterno','newmetermake','connectiontype','verify_status','consumer_type','comm']
   df_mcr = pd.DataFrame(columns=clmn)

   # Fetching HTCT Consumer
   qry = "select circle,subdivision,sdocode,feedercode,kno,survey_timings,newmeterno,newmetermake,	\
            kno as connectiontype,verify_status,consumer_category as consumer_type,(select comm from ami_master.master_main where mtrno = htct_meter_installation_details.newmeterno limit 1) as comm from ami_master.htct_meter_installation_details"
   df_mcr = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   df_mcr['connectiontype'] = '4-Phase'
   df_mcr['consumer_type'] = 'HTCT_Cons'

   print("HTCT Consumer MI Data Received : ", len(df_mcr))

## Get Tracker Data for Consumer MMR L1 API..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'

   clmn = ['id','kno','meterno','survey_time','response_time','response','request_status','old_applicationid','old_response', \
         'old_entrydate','new_applicationid','new_response','new_entrydate','old_payload','new_payload','L1_meter_flag']
   df_mmr = pd.DataFrame(columns=clmn)

   msg_body = str("HTCT Consumer MMR L1 API Tracker Data Received, Tot. Records ") 

   qry = "select id,kno,meterno,survey_time,response_time,response,request_status,old_applicationid,old_entrydate, \
            new_applicationid,meter_flag,new_entrydate \
            from ami_master.arms_mtr_replacement_tracking where \
                        kno in (select kno from ami_master.htct_meter_installation_details) \
                           order by meterno, request_time"

   df_mmr = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   print("HTCT Consumer MI Data Received : ", len(df_mmr))
   # msgBoard.set(str(msg_body) + str(len(df_mmr)))

   return df_mcr, df_mmr

# df_mcr, df_mmr = getHTCT_MI_MMR_Data()




def getNFMD_Data(Nod,datatype):
## Get Tracker Data for NFMS Data Push..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'
   curr_date = datetime.now()
   clmn = ['id','time_stamp','transid','feedercode','mtrno','data_type','data_type_slot','data','max_id','response_time','response_code','message','details']   
   df = pd.DataFrame(columns=clmn)
   datatype = 'Block Load Survey'
   for iii in range(Nod):     
      frmDate = (curr_date - timedelta(days=iii)).strftime('%Y-%m-%d')
      toDate = (curr_date - timedelta(days=(iii-1))).strftime('%Y-%m-%d')

      Flag = True
      while Flag:
         isPass = 0
         # Fetching LT Consumer
         if True: # dtype == 'cons':
            msg_body = str("NFMS Cumm Data Pushed at ") + str(frmDate) + str(' : ')
            try:
               q1 = "select * from ami_master.nfms_tracker where response_time >= '"
               q2 = "' and response_time < '"
               q3 = "' and data_type = '"
               q4 = "' order by response_time desc"

               qry = q1 + str(frmDate) + q2 + str(toDate) + q3 + str(datatype) + q4
               # print(qry)
               df0 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
               if len(df0) > 0: 
                  isPass = 1
                  df = df._append(df0)
               print('')
               print(msg_body, len(df0), ', Total Count : ', len(df))
               # msgBoard.set(str(msg_body) + str(len(df0)) + ', Total Count : ' + str(len(df)))
            except:
               print('')
               print(msg_body, 0)
               msgBoard.set(str(msg_body) + str(0))
         if isPass == 1: Flag = False
   return df


def getMDAS_API_data(dt_type,dt):
## Get API Tracker Data for MDAS..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'
   if dt_type == 'BLP':
      d1 = dt.strftime("%Y-%m-%d")
      d2 = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

      clmn = ['meter_number',	'category',	'response_msg',	'count']

      qry1 = "select meter_number, category, response_msg, count(*) from ami_master.mcl_block_load_profile where dayprofile_date >= '"
      qry2 = d1
      qry3 = "'and dayprofile_date < '"
      qry4 = d2
      qry5 = "' group by meter_number, category, response_msg"
      qry = qry1 + qry2 + qry3 + qry4 + qry5 
      df = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
      # print("MDAS API -- Block Load Data Received : ", len(df))
   return df   


def getNDM_API_data(dt_type,dt):
## Get API Tracker Data for MDAS..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'
   if dt_type == 'BLP':
      d1 = dt.strftime("%Y-%m-%d")
      d2 = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

      clmn = ['meter_number','response_msg','count']

      qry1 = "select meter_number, response_msg, count(*) from ami_master.mcl_feeder_load_profile where reading_date >= '"
      qry2 = d1
      qry3 = "' and reading_date < '"
      qry4 = d2
      qry5 = "' group by meter_number, response_msg"
      qry = qry1 + qry2 + qry3 + qry4 + qry5 
      df = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
      # print("MDAS API -- Block Load Data Received : ", len(df))
   return df 


def Data_Storage(f,key,datain):
    try: 
      shfile = shelve.open("Data_Selve") # In this step, we create a shelf file.
      dataout = 0
      if int(f) == 1: # f = 1: Date Store, 0:Data Fetch
         shfile[key] = datain
      else:
         dataout = shfile[key]
      shfile.close()
    except:
       pass
    return dataout


def saveExcel(df,fname,type):
   Fname = fname + type
   try: 
      if type == '.xlsx':      df.to_excel(Fname)
      if type == '.csv':       df.to_csv(Fname)
   except:
      Send2UI = ['File ' + Fname + ' is Open, Please Close and Enter..!!'];  tv2.insert(parent='', index=0, values=(Send2UI))
      input('File ' + Fname + ' is Open, Please Close and Enter..!!')
      if type == '.xlsx':      df.to_excel(Fname)
      if type == '.csv':       df.to_csv(Fname)
   
   
   

def getReset(df):
    df.reset_index(inplace=True)
    try:
        df = df.drop(columns=['index'])
    except:
        pass
    try:
        df = df.drop(columns=['level_0'])
    except:
        pass
    try:
        df = df.drop(columns=['Unnamed: 0'])
    except:
        pass       
    return df


def pivot_tabll(df,Index_Name):
   colmn = [Index_Name, "Total MI", "Communicating", "Per_Comm.", "Non_Communicating", "Never_Communicating"]
   table1 = pd.DataFrame(columns=colmn)
   
   Uniq = df[Index_Name].unique()
   for qq in range(len(Uniq)):
      table1.loc[qq,Index_Name] = str(Uniq[qq])   
      df0 = df.loc[df[Index_Name] == str(Uniq[qq])];
      		
      table1.loc[qq,'Communicating'] = int(sum(df0['Communicating']))
      table1.loc[qq,'Per_Comm.'] = str(round(table1.loc[qq,'Communicating'] * 100 / len(df0),1)) + str(" %")
      table1.loc[qq,'Non_Communicating'] = int(sum(df0['Non_Communicating']))
      table1.loc[qq,'Never_Communicating'] = int(sum(df0['Never_Communicating']))
      table1.loc[qq,'Total MI'] = len(df0)
   
   aaa = sum(table1.loc[:,'Communicating'])
   bbb = sum(table1.loc[:,'Non_Communicating'])
   ccc = sum(table1.loc[:,'Never_Communicating'])

   table1.loc[len(table1)+1,Index_Name] = 'Grand_Total'
   table1.loc[len(table1),'Communicating'] = int(aaa)
   table1.loc[len(table1),'Non_Communicating'] = int(bbb)
   table1.loc[len(table1),'Never_Communicating'] = int(ccc)
   table1.loc[len(table1),'Total MI'] = len(df)
   table1.loc[len(table1),'Per_Comm.'] = str(round(table1.loc[len(table1),'Communicating'] * 100 / len(df),1)) + str(" %")
   # print('    ')
   # print(table1)
   return table1


def ProcessMDAS_API_data():
   YYYY = 2025
   MM = 1
   DD = 1
   tday = 31

   ## Processing MDAS API Data - BLP
   dt_type = 'BLP'   
   clmn = ['Push_date','BLP_date','meter_number','category','response_msg','count']
   df = pd.DataFrame(columns=clmn)
   while DD <= tday:
      date_data = datetime(2025, MM, DD)
      d1 = date_data + timedelta(days=2)
      df0 = getMDAS_API_data(dt_type,date_data)
      if len(df0) > 0:
         df0['Push_date'] = d1.strftime("%Y-%m-%d")
         df0['BLP_date'] =  date_data.strftime("%Y-%m-%d")
         df = df._append(df0) 
      print(len(df0),' Meters Data Processed for ', date_data.strftime("%Y-%m-%d"), ' at ', d1.strftime("%Y-%m-%d"))       
      DD = DD + 1
   
   fname = 'MDAS_BLP_' + str(MM) + str('_') + str(YYYY) + str('.xlsx')
   df.to_excel(fname)
   MDAS_BLP = pd.pivot_table(df, values='meter_number', index=['BLP_date','category'],columns=['response_msg'], aggfunc="count", fill_value=0)
   print(MDAS_BLP)

# ProcessMDAS_API_data()

def ProcessSAP_API_Data(): 
   isUpdatedMI = IsUpdateMI.get()
   isUpdateSAP_API = IsUpdateSAPAPI.get()

   if isUpdatedMI == 'Yes':
      # get Consumer MCR
      Send2UI = ["Capturing Updated MI Data..!! "]; tv2.insert(parent='', index=0, values=(Send2UI))
      mcr = getAll_MCR_Master()
      mcr.reset_index(inplace=True)
      Send2UI = ["Cummulative MCR Raw Data Received : "+ str(len(mcr))]; tv2.insert(parent='', index=0, values=(Send2UI))
      Data_Storage(1,'cons_mcrK',mcr)
      print('MCR Raw Data Stored..!!')
      tv2.insert(parent='', index=0, values=([str('Cummulative MCR Raw Data Stored..!!')])) 
      saveExcel(mcr,'MCR_ALL_SAP','.xlsx')
   else:
      mcr = Data_Storage(0,'cons_mcrK',"")
      print('MCR Raw Data Loaded..!!')
      tv2.insert(parent='', index=0, values=([str('Cummulative MCR Raw Data Loaded..!!')]))
      mmm = "Cummulative MCR Raw Data Loaded : " + str(len(mcr))
      print(mmm)
      Send2UI = [mmm]; tv2.insert(parent='', index=0, values=(Send2UI))
   
   try: mcr["circle"] = mcr["circle"].replace(["Nagpur U"], "NAGPUR (U) CIRCLE")
   except: pass

   if isUpdateSAP_API == 'Yes':
      # get Consumer MCR
      Send2UI = ["Capturing Updated SAP API Raw Data..!! "]; tv2.insert(parent='', index=0, values=(Send2UI))
      df_SAP_API_Data = getSAP_API_Sync()
      df_SAP_API_Data = getReset(df_SAP_API_Data)
      Send2UI = ["SAP API Raw Data Received : "+ str(len(df_SAP_API_Data))]; tv2.insert(parent='', index=0, values=(Send2UI))
      Data_Storage(1,'SAPAPI',df_SAP_API_Data)
      print('SAP API Raw Data Stored..!!')
      Send2UI = ['SAP API Raw Data Stored..!!']; tv2.insert(parent='', index=0, values=(Send2UI))
      saveExcel(df_SAP_API_Data,'SAP_API_RAW','.xlsx')
   else:
      df_SAP_API_Data = Data_Storage(0,'SAPAPI',"")
      print('     ')
      print('SAP API Raw Data Loaded..!!')
      Send2UI = ['SAP API Raw Data Loaded..!!']; tv2.insert(parent='', index=0, values=(Send2UI))
      print("SAP API Tracker Table Data Loaded : ", len(df_SAP_API_Data))
      Send2UI = ["SAP API Tracker Table Data Loaded : "+ str(len(df_SAP_API_Data))]; tv2.insert(parent='', index=0, values=(Send2UI))

   print('Processing MCR & SAP API Data..!!')
   Send2UI = ['Processing MCR & SAP API Data..!!']; tv2.insert(parent='', index=0, values=(Send2UI))
   df_SAP_API_Data["UID"] = df_SAP_API_Data["meterno"] # df_SAP_API_Data["kno"].astype(str) +"_"+ df_SAP_API_Data["meterno"]
   mcr["UID"]             = mcr["newmeterno"]          # mcr["kno"].astype(str) +"_"+ mcr["newmeterno"]
   
   Pass_cases = pd.DataFrame()
   indx = df_SAP_API_Data.index[(df_SAP_API_Data["flag"] == "1")]
   Pass_cases['UID'] = df_SAP_API_Data.loc[indx,'UID']
   Pass_cases['Pass_date'] = df_SAP_API_Data.loc[indx,'time_stamp']
   Pass_cases.loc[:,'Pass_flag'] = 1
   Pass_cases = Pass_cases.drop_duplicates(subset='UID')
   Pass_cases = getReset(Pass_cases)
   
   mcr["isPush"] = 0
   mcr["Push_date"] = 0
   Pass_cases.set_index("UID",inplace=True)
   mcr["isPush"] = mcr.UID.map(Pass_cases["Pass_flag"]) ## Error if Pass_cases have duplicate UIDs
   mcr["Push_date"] = mcr.UID.map(Pass_cases["Pass_date"]) ## Error if Pass_cases have duplicate UIDs
   
   df_SAP_API_Data["isPush"] = 0
   df_SAP_API_Data["isPush"] = df_SAP_API_Data.UID.map(Pass_cases["Pass_flag"]) ## Error if Pass_cases have duplicate UIDs

   data = df_SAP_API_Data
   indx = data.index[(data["isPush"] == 1.0)]
   data.drop(indx,axis=0,inplace=True)
   data = getReset(data)

   data.sort_values(by = ['UID', 'time_stamp'], inplace=True, ascending=False); 
   data = getReset(data)

   data = data.drop_duplicates(subset='UID')
   data = getReset(data)

   mcr["Error"] = "-"
   mcr["Error_date"] = "-"
   data.set_index("UID",inplace=True)
   mcr["Error"] = mcr.UID.map(data["response_body"]) ## Error if Pass_cases have duplicate UIDs
   mcr["Error_date"] = mcr.UID.map(data["time_stamp"]) ## Error if Pass_cases have duplicate UIDs
   
   mcr["Final_Status"] = "Pending: Not Pushed"

   indx = mcr[mcr['Error'].str.contains("Meter not issued")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Meter not issued'

   indx = mcr[mcr['Error'].str.contains("Meter not exist")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Meter not exist'
   
   indx = mcr.index[(mcr["isPush"] == 1.0)]
   mcr.loc[indx,'Final_Status'] = 'Success: Pushed to SAP'

   indx = mcr.index[(mcr["Error"] == "HTTP response code: 500")]
   mcr.loc[indx,'Final_Status'] = 'Error: HTTP response code: 500'
   
   DD = datetime.now().day
   MM = datetime.now().month
   YYYY = datetime.now().year
   Fname1 = 'MCR_SAP_API_Final' + str("_") + str(DD) + str(MM) + str(YYYY)
   saveExcel(mcr,Fname1,'.xlsx')

   # table = pd.pivot_table(mcr, values='UID', index=['circle', 'subdivision'],columns=['connectiontype'], aggfunc="count", fill_value=0)
   MCR_Summary = pd.pivot_table(mcr, values='UID', index=['circle'],columns=['connectiontype'], aggfunc="count", fill_value=0)
   SAP_API_Status = pd.pivot_table(mcr, values='UID', index=['Final_Status'],columns=['circle'], aggfunc="count", fill_value=0)
   SAP_API_Status_1 = pd.pivot_table(mcr, values='UID', index=['Final_Status'], aggfunc="count", fill_value=0)
   print('     ')
   print('*************MCR Data Summary Circle / Type Wise*************')
   pprint(MCR_Summary)
   print('     ')
   print('*************SAP API Data Sync Status*************')
   pprint(SAP_API_Status)
   print('     ')
   print('*************SAP API Data Sync Status*************')
   pprint(SAP_API_Status_1)
   print(SAP_API_Status_1.to_markdown())
   
   saveExcel(MCR_Summary,'MCR_Summary','.csv')
   saveExcel(SAP_API_Status_1,'SAP_API_Status_1','.csv')
   
   print('     ')
   Send2UI = [""]; tv2.insert(parent='', index=0, values=(Send2UI))
   Send2UI = ["*************MCR Data ConnectionType Wise*************"]; tv2.insert(parent='', index=0, values=(Send2UI))
   print("1-Phase Consumer MI Data Received : ", mcr['connectiontype'].value_counts()['1-Phase'])
   Send2UI = [str("1-Phase Consumer MI Data Received : ") + str(mcr['connectiontype'].value_counts()['1-Phase'])]; tv2.insert(parent='', index=0, values=(Send2UI))
   print("3-Phase Consumer MI Data Received : ", mcr['connectiontype'].value_counts()['3-Phase'])
   Send2UI = [str("3-Phase Consumer MI Data Received : ") + str(mcr['connectiontype'].value_counts()['3-Phase'])]; tv2.insert(parent='', index=0, values=(Send2UI))
   print("LTCT-DT MI Data Received : ", mcr['connectiontype'].value_counts()['LTCT_DT'])
   Send2UI = [str("LTCT-DT MI Data Received : ") + str(mcr['connectiontype'].value_counts()['LTCT_DT'])]; tv2.insert(parent='', index=0, values=(Send2UI))
   print("HTCT-Feeder MI Data Received : ", mcr['connectiontype'].value_counts()['HTCT_FD'])
   Send2UI = [str("HTCT-Feeder MI Data Received : ") + str(mcr['connectiontype'].value_counts()['HTCT_FD'])]; tv2.insert(parent='', index=0, values=(Send2UI))

   Send2UI = [""]; tv2.insert(parent='', index=0, values=(Send2UI))
   Send2UI = ["*************SAP API Data Sync Status*************"]; tv2.insert(parent='', index=0, values=(Send2UI))
   SAP_API_Status_1 = getReset(SAP_API_Status_1)
   for ii in range(len(SAP_API_Status_1)):
       mmm = str(SAP_API_Status_1.loc[ii,'Final_Status']) + ' :  ' + str(SAP_API_Status_1.loc[ii,'UID'])
       Send2UI = [mmm]; tv2.insert(parent='', index=0, values=(Send2UI))
   
   Send2UI = [""]; tv2.insert(parent='', index=0, values=(Send2UI))
   Send2UI = ["MCR_SAP Final Data Saved to file : "+ str(Fname1) + str(',xlsx')]; tv2.insert(parent='', index=0, values=(Send2UI))
   Send2UI = ["Programme Comeplted..!!"]; tv2.insert(parent='', index=0, values=(Send2UI))


def Process_FD_MMR_Data(): 
   isUpdatedMI = IsUpdateMI.get()
   isUpdateSAP_API = IsUpdateSAPAPI.get()

   if isUpdatedMI == 'Yes':
      # get Feeder-DT MCR
      Send2UI = ["Capturing Updated FD MI Data: "]; tv2.insert(parent='', index=0, values=(Send2UI))
      mcr = getFDDT_MCR_Master('FD')
      mcr.reset_index(inplace=True)
      Send2UI = ["Feeder MCR Raw Data Received : "+ str(len(mcr))]; tv2.insert(parent='', index=0, values=(Send2UI))
      Data_Storage(1,'FD_mcrK',mcr)
      print("Feeder MCR Raw Data Saved to file : "+ str('FD_MCR_Updated.xlsx'))
      saveExcel(mcr,'FD_MCR_Updated','.xlsx')
      Send2UI = ["Feeder MCR Raw Data Saved to file : "+ str('FD_MCR_Updated.xlsx')]; tv2.insert(parent='', index=0, values=(Send2UI))
   else:
      mcr = Data_Storage(0,'FD_mcrK',"")
      print('Feeder MCR Raw Data Loaded..!!')
      tv2.insert(parent='', index=0, values=([str('MCR Raw Data Loaded..!!')]))
      mmm = "Stored FD MI Data Loaded : " + str(len(mcr))
      print(mmm)
      Send2UI = [mmm]; tv2.insert(parent='', index=0, values=(Send2UI))

   try: mcr["circle"] = mcr["circle"].replace(["Nagpur U"], "NAGPUR (U) CIRCLE")
   except: pass

   if isUpdateSAP_API == 'Yes':
      # get Consumer MCR
      Send2UI = ["Capturing Updated FD MMR Data: "]; tv2.insert(parent='', index=0, values=(Send2UI))
      df_FDDT_MMR_Data = getFDDT_MMR_Data()
      df_FDDT_MMR_Data = getReset(df_FDDT_MMR_Data)
      Data_Storage(1,'FDDT_MMR',df_FDDT_MMR_Data)
      Send2UI = ["Feeder MCR Raw Data Received : "+ str(len(df_FDDT_MMR_Data))]; tv2.insert(parent='', index=0, values=(Send2UI))
      print("Feeder MMR Raw Data Saved to file : "+ str('FD_MMR_Updated.xlsx'))
      saveExcel(df_FDDT_MMR_Data,'FD_MMR_Updated','.xlsx')
      Send2UI = ["Feeder MMR Raw Data Saved to file : "+ str('FD_MMR_Updated.xlsx')]; tv2.insert(parent='', index=0, values=(Send2UI))
   else:
      df_FDDT_MMR_Data = Data_Storage(0,'FDDT_MMR',"")
      print('     ')
      print('Feeder MMR Raw Data Loaded..!!')
      Send2UI = ['Feeder MMR Raw Data Loaded..!!']; tv2.insert(parent='', index=0, values=(Send2UI))
      print("Feeder MMR Tracker Table Data Loaded : ", len(df_FDDT_MMR_Data))
      Send2UI = ["Feeder MMR Tracker Table Data Loaded : "+ str(len(df_FDDT_MMR_Data))]; tv2.insert(parent='', index=0, values=(Send2UI))
   
   indx = df_FDDT_MMR_Data.index[(df_FDDT_MMR_Data["response"] == "")]

   # input('Check for Space in Feeder Codes in MCR & MMR Data and Click **Enter**')
   # mcr = pd.read_excel('FD_MCR_Updated.xlsx')
   # df_FDDT_MMR_Data = pd.read_excel('FD_MMR_Updated.xlsx')
   # df_FDDT_MMR_Data = getReset(df_FDDT_MMR_Data)
   indx = df_FDDT_MMR_Data.index[(df_FDDT_MMR_Data["response"] == "")]
   df_FDDT_MMR_Data.loc[indx,"response"] = df_FDDT_MMR_Data.loc[indx,"request_status"]

   print('Processing Feeder MCR & MMR Tracker Data..!!')
   Send2UI = ['Processing Feeder MCR & MMR Tracker Data..!!']; tv2.insert(parent='', index=0, values=(Send2UI))

   FDDT_Delete_IDs = pd.read_excel('FDDT_Delete_IDs.xlsx')
#    indx = df_FDDT_MMR_Data.index[(df_FDDT_MMR_Data["id"] == FDDT_Delete_IDs)]
   for ii in range(len(FDDT_Delete_IDs)):
        indx = df_FDDT_MMR_Data.index[(df_FDDT_MMR_Data["id"] == FDDT_Delete_IDs.loc[ii,'IDs'])]
        df_FDDT_MMR_Data.drop(indx,axis=0,inplace=True)
        df_FDDT_MMR_Data = getReset(df_FDDT_MMR_Data)

   indx = df_FDDT_MMR_Data.index[(df_FDDT_MMR_Data["category"] == "DT")]
   df_FDDT_MMR_Data.drop(indx,axis=0,inplace=True)
   df_FDDT_MMR_Data = getReset(df_FDDT_MMR_Data)  

   indx = mcr.index[(mcr["connectiontype"] == "LTCT_DT")]
   mcr.drop(indx,axis=0,inplace=True)
   mcr = getReset(mcr)

   indx = df_FDDT_MMR_Data.index[(df_FDDT_MMR_Data["request_status"] == "Success.")]
   df_FDDT_MMR_Data.loc[indx,'request_status'] = "Success"

   df_FDDT_MMR_Data["UID"] = df_FDDT_MMR_Data["feeder_dt_code"] # df_SAP_API_Data["kno"].astype(str) +"_"+ df_SAP_API_Data["meterno"]
   mcr["UID"]              = mcr["FD_DT_Code"]          # mcr["kno"].astype(str) +"_"+ mcr["newmeterno"]
   
   Pass_cases = pd.DataFrame()
   indx = df_FDDT_MMR_Data.index[(df_FDDT_MMR_Data["request_status"] == "Success")]
   Pass_cases['UID'] = df_FDDT_MMR_Data.loc[indx,'UID']
   Pass_cases['Pass_date'] = df_FDDT_MMR_Data.loc[indx,'response_time']
   Pass_cases.loc[:,'Pass_flag'] = 1
   Pass_cases = Pass_cases.drop_duplicates(subset='UID')
   Pass_cases = getReset(Pass_cases)
   
   mcr["isPush"] = 0
   mcr["Push_date"] = 0
   Pass_cases.set_index("UID",inplace=True)
   mcr["isPush"] = mcr.UID.map(Pass_cases["Pass_flag"]) ## Error if Pass_cases have duplicate UIDs
   mcr["Push_date"] = mcr.UID.map(Pass_cases["Pass_date"]) ## Error if Pass_cases have duplicate UIDs
   
   df_FDDT_MMR_Data["isPush"] = 0
   df_FDDT_MMR_Data["isPush"] = df_FDDT_MMR_Data.UID.map(Pass_cases["Pass_flag"]) ## Error if Pass_cases have duplicate UIDs

   data = df_FDDT_MMR_Data
   indx = data.index[(data["isPush"] == 1.0)]
   data.drop(indx,axis=0,inplace=True)
   data = getReset(data)

   data.sort_values(by = ['UID', 'response_time'], inplace=True, ascending=False); 
   data = getReset(data)

   data = data.drop_duplicates(subset='UID')
   data = getReset(data)

   mcr["Error"] = "-"
   mcr["Error_date"] = "-"
   data.set_index("UID",inplace=True)
   mcr["Error"] = mcr.UID.map(data["response"]) ## Error if Pass_cases have duplicate UIDs
   mcr["Error_date"] = mcr.UID.map(data["response_time"]) ## Error if Pass_cases have duplicate UIDs
   
   mcr["Final_Status"] = "Pending: Not Pushed"

   indx = mcr.index[(mcr["Error"] == "Connection Timeout")]
   indx = mcr[mcr['Error']== "Connection Timeout"].index
   mcr.loc[indx,"Final_Status"] = 'Error: Timeout/Error/Fail'
   
   indx = mcr.index[(mcr["Error"] == "ERROR")]
   mcr.loc[indx,"Final_Status"] = 'Error: Timeout/Error/Fail'

   indx = mcr.index[(mcr["Error"] == "Fail")]
   mcr.loc[indx,"Final_Status"] = 'Error: Timeout/Error/Fail'

   indx = mcr[mcr['Error'].str.contains("DB ERROR. TRY LATER")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: DB ERROR. TRY LATER'

   indx = mcr[mcr['Error'].str.contains("Meter Already Exists")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Meter Already Exists'

   indx = mcr[mcr['Error'].str.contains("READING_AT_DISCONN_IMP Should be greater then available meter reading")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: FR Should be greater then available meter reading'

   indx = mcr[mcr['Error'].str.contains("Replacement Date should be Greater Than Max Reading Available")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Replacement Date should be Greater Than Max Reading Available'

   indx = mcr[mcr['Error'].str.contains("Meter Initial Reading Import cannot be Empty")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Meter Initial Reading Import cannot be Empty'  

   indx = mcr[mcr['Error'].str.contains("Network Does not have old meter")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Network Does not have old meter' 

   indx = mcr[mcr['Error'].str.contains("Neumarator/Denominator")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Invalid CT-PT Ratios' 

   indx = mcr[mcr['Error'].str.contains("Bi-Directional Meter")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Meter Uni/Bi Directional Issue'

   indx = mcr[mcr['Error'].str.contains("Request Id Already Exists")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Request Id Already Exists - Repush'

   indx = mcr[mcr['Error'].str.contains("Invalid Meter Lab Number")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Invalid RST Details'

   indx = mcr[mcr['Error'].str.contains("Invalid Feeder-Code")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Invalid Feeder-Code'
   
   indx = mcr[mcr['Error'].str.contains("Feeder Already have more than 1 Main Meter.")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Feeder Already have more than 1 Main Meter.'
   
   indx = mcr[mcr['Error'].str.contains("From BU cannot be Same as TO_BU")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: From BU cannot be Same as TO_BU'
   
   indx = mcr[mcr['Error'].str.contains("From BU cannot be Same as TO_BU.FROM_TOWN_ID cannot be Same as TO_TOWN_ID.")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: From BU cannot be Same as TO_BU'
   
   indx = mcr[mcr['Error'].str.contains("Old Meter Already Active on Another Feeder")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Old Meter Already Active on Another Feeder'
   
   indx = mcr[mcr['Error'].str.contains("InValid From Town OR TO_Town for Main/Main+Boundary Meter")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: InValid From Town OR TO_Town for Main/Main+Boundary Meter'
   
   indx = mcr[mcr['Error'].str.contains("New Meter Type Id Should Be Same as Old Meter Type Id")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: New Meter Type Id Should Be Same as Old Meter Type Id'
   
   indx = mcr[mcr['Error'].str.contains("InValid From BU And TO_BU for Main/Main+Boundary Meter")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: InValid From BU And TO_BU for Main/Main+Boundary Meter'
   
   
   
   
   ManualFDStatus = ['S104018206','S164080205',	'S084016207',	'S164080203',	'S294052204',	'S084048202',	'S164019206',	'S164812202',	'S084078208',	'S162008206',	'S084053207',	'S314013205',	'S164814206',	'S164814208',	'S294053206',	'S043167206',	'S064045202',	'S294011206',	'S164075202',	'S314003203',	'S084078218',	'S084084202',	'S084016201',	'S064040204',	'S164005204',	'S164046203',	'S084016202',	'S064003201',	'S064003208',	'S084002205',	'S084016204',	'S084027201',	'S084048201',	'S084078204',	'S084090201',	'S084090202',	'S164019201',	'S164019202',	'S164019203',	'S164019205',	'S164041206',	'S164046206',	'S164055204',	'S164077206',	'S164080202',	'S164082201',	'S164082202',	'S164082203',	'S164801214',	'S164802206',	'S164814201',	'S164814202',	'S164814207',	'S164836202',	'S164836203',	'S164848203',	'S168005212',	'S294002201',	'S294002203',	'S294002204',	'S294011201',	'S294054203',	'S314003206',	'S314005201',	'S314019204',	'S314023202',	'S314023205']
   for ii in range(len(ManualFDStatus)):
      indx = mcr.index[(mcr["FD_DT_Code"] == ManualFDStatus[ii])]
      mcr.loc[indx,'isPush'] = 1.0
      mcr.loc[indx,'Push_date'] = mcr.loc[indx,'survey_timings']

   indx = mcr.index[(mcr["isPush"] == 1.0)]
   mcr.loc[indx,'Final_Status'] = 'Success: Pushed to NDMS'
   
   DD = datetime.now().day
   MM = datetime.now().month
   YYYY = datetime.now().year
   Fname1 = 'MCR_Feeder_MMR_Final' + str("_") + str(DD) + str(MM) + str(YYYY)
   saveExcel(mcr,Fname1,'.xlsx') # Saving Final MCR
   Send2UI = ["Feeder MCR_MMR Final Data Saved to file : "+ str(Fname1) + str(',xlsx')]; tv2.insert(parent='', index=0, values=(Send2UI))

   # table = pd.pivot_table(mcr, values='UID', index=['circle', 'subdivision'],columns=['connectiontype'], aggfunc="count", fill_value=0)
   MCR_Summary = pd.pivot_table(mcr, values='UID', index=['circle'],columns=['connectiontype'], aggfunc="count", fill_value=0)
   MMR_Status = pd.pivot_table(mcr, values='UID', index=['Final_Status'],columns=['circle'], aggfunc="count", fill_value=0)
   MMR_Status1 = pd.pivot_table(mcr, values='UID', index=['Final_Status'], aggfunc="count", fill_value=0)

   try: 
      Fname = 'MCR_Feeder_MMR_Summary_' + str(DD) + str(MM) + str(YYYY) + str('.txt')
      with open(Fname, 'w') as outfile:
         outfile.write('\n')

      with open(Fname, 'a') as outfile:
         outfile.write('******************************Circle Wise FD MMR / Exception Status********************************** \n')       

      with open(Fname, 'a') as outfile:
         MMR_Status.to_string(outfile)

      with open(Fname, 'a') as outfile:
         outfile.write('\n\n\n*************FD MCR Data Summary Circle Wise************* \n')       

      with open(Fname, 'a') as outfile:
         MCR_Summary.to_string(outfile)

      with open(Fname, 'a') as outfile:
         outfile.write('\n\n\n*************FD MCR Data Summary Circle Wise************* \n')       

      with open(Fname, 'a') as outfile:
         MMR_Status1.to_string(outfile)
      
      Send2UI = ["Feeder MCR_MMR Final Data Summary Saved to file : "+ str(Fname)]; tv2.insert(parent='', index=0, values=(Send2UI))
      Send2UI = ["Programme Comeplted..!!"]; tv2.insert(parent='', index=0, values=(Send2UI))
   except Exception as e:
      print(str('Error in Function: **processdata** with Error Message : "') + str(e) + str('"'))   
      Send2UI = ["Error while Creating Summary File with Error Message : "+ str(e)]; tv2.insert(parent='', index=0, values=(Send2UI))
      Send2UI = ["Programme Execution Completed..!!"];                               tv2.insert(parent='', index=0, values=(Send2UI))
   
   return mcr




def Process_DT_MMR_Data(): 
   isUpdatedMI = IsUpdateMI.get()
   isUpdateSAP_API = IsUpdateSAPAPI.get()
   if isUpdatedMI == 'Yes':
       Send2UI = ["Capturing Updated DT MI Data: "]; tv2.insert(parent='', index=0, values=(Send2UI))
       mcr = getFDDT_MCR_Master('DT')
       if mcr.empty:
           Send2UI = ["No DT MCR data found"]; tv2.insert(parent='', index=0, values=(Send2UI))
           return
       mcr.reset_index(inplace=True)
       Send2UI = ["DT MCR Raw Data Received: "+ str(len(mcr))]; tv2.insert(parent='', index=0, values=(Send2UI))
       Data_Storage(1,'DT_mcrK',mcr)
       print("DT MCR Raw Data Saved to file: "+ str('DT_MCR_Updated.xlsx'))
       saveExcel(mcr,'DT_MCR_Updated','.xlsx')
       Send2UI = ["DT MCR Raw Data Saved to file: "+ str('DT_MCR_Updated.xlsx')]; tv2.insert(parent='', index=0, values=(Send2UI))
   else:
       mcr = Data_Storage(0,'DT_mcrK',"")
       if mcr.empty:
           Send2UI = ["No stored DT MCR data found"]; tv2.insert(parent='', index=0, values=(Send2UI))
           return
       print('DT MCR Raw Data Loaded..!!')
       tv2.insert(parent='', index=0, values=([str('MCR Raw Data Loaded..!!')]))
       mmm = "Stored DT MI Data Loaded: " + str(len(mcr))
       print(mmm)
       Send2UI = [mmm]; tv2.insert(parent='', index=0, values=(Send2UI))
   try: 
       mcr["circle"] = mcr["circle"].replace(["Nagpur U"], "NAGPUR (U) CIRCLE")
   except: 
       pass
   if isUpdateSAP_API == 'Yes':
       Send2UI = ["Capturing Updated DT MMR Data: "]; tv2.insert(parent='', index=0, values=(Send2UI))
       df_FDDT_MMR_Data = getFDDT_MMR_Data()
       if df_FDDT_MMR_Data.empty:
           Send2UI = ["No DT MMR data found"]; tv2.insert(parent='', index=0, values=(Send2UI))
           return
       df_FDDT_MMR_Data = getReset(df_FDDT_MMR_Data)
       Data_Storage(1,'FDDT_MMR',df_FDDT_MMR_Data)
       Send2UI = ["DT MCR Raw Data Received: "+ str(len(df_FDDT_MMR_Data))]; tv2.insert(parent='', index=0, values=(Send2UI))
       print("DT MMR Raw Data Saved to file: "+ str('DT_MMR_Updated.xlsx'))
       saveExcel(df_FDDT_MMR_Data,'DT_MMR_Updated','.xlsx')
       Send2UI = ["DT MMR Raw Data Saved to file: "+ str('DT_MMR_Updated.xlsx')]; tv2.insert(parent='', index=0, values=(Send2UI))
   else:
       df_FDDT_MMR_Data = Data_Storage(0,'FDDT_MMR',"")
       if df_FDDT_MMR_Data.empty:
           Send2UI = ["No stored DT MMR data found"]; tv2.insert(parent='', index=0, values=(Send2UI))
           return
       print('DT MMR Raw Data Loaded..!!')
       Send2UI = ['DT MMR Raw Data Loaded..!!']; tv2.insert(parent='', index=0, values=(Send2UI))
       print("DT MMR Tracker Table Data Loaded: ", len(df_FDDT_MMR_Data))
       Send2UI = ["DT MMR Tracker Table Data Loaded: "+ str(len(df_FDDT_MMR_Data))]; tv2.insert(parent='', index=0, values=(Send2UI))
   
   # Rest of your function remains the same...
   
   indx = df_FDDT_MMR_Data.index[(df_FDDT_MMR_Data["response"] == "")]

   # input('Check for Space in Feeder Codes in MCR & MMR Data and Click **Enter**')
   # mcr = pd.read_excel('FD_MCR_Updated.xlsx')
   # df_FDDT_MMR_Data = pd.read_excel('FD_MMR_Updated.xlsx')
   # df_FDDT_MMR_Data = getReset(df_FDDT_MMR_Data)
   indx = df_FDDT_MMR_Data.index[(df_FDDT_MMR_Data["response"] == "")]
   df_FDDT_MMR_Data.loc[indx,"response"] = df_FDDT_MMR_Data.loc[indx,"request_status"]

   print('Processing DT MCR & MMR Tracker Data..!!')
   Send2UI = ['Processing DT MCR & MMR Tracker Data..!!']; tv2.insert(parent='', index=0, values=(Send2UI))

   FDDT_Delete_IDs = pd.read_excel('FDDT_Delete_IDs.xlsx')
#    indx = df_FDDT_MMR_Data.index[(df_FDDT_MMR_Data["id"] == FDDT_Delete_IDs)]
   for ii in range(len(FDDT_Delete_IDs)):
        indx = df_FDDT_MMR_Data.index[(df_FDDT_MMR_Data["id"] == FDDT_Delete_IDs.loc[ii,'IDs'])]
        df_FDDT_MMR_Data.drop(indx,axis=0,inplace=True)
        df_FDDT_MMR_Data = getReset(df_FDDT_MMR_Data)

   indx = df_FDDT_MMR_Data.index[(df_FDDT_MMR_Data["category"] == "FEEDER")]
   df_FDDT_MMR_Data.drop(indx,axis=0,inplace=True)
   df_FDDT_MMR_Data = getReset(df_FDDT_MMR_Data)  

   indx = mcr.index[(mcr["connectiontype"] == "HTCT_FD")]
   mcr.drop(indx,axis=0,inplace=True)
   mcr = getReset(mcr)

   indx = df_FDDT_MMR_Data.index[(df_FDDT_MMR_Data["request_status"] == "Success.")]
   df_FDDT_MMR_Data.loc[indx,'request_status'] = "Success"

   df_FDDT_MMR_Data["UID"] = df_FDDT_MMR_Data["feeder_dt_code"] # df_SAP_API_Data["kno"].astype(str) +"_"+ df_SAP_API_Data["meterno"]
   mcr["UID"]              = mcr["FD_DT_Code"]          # mcr["kno"].astype(str) +"_"+ mcr["newmeterno"]
   
   mcr["isPush"] = 0
   mcr["Push_date"] = 0
   df_FDDT_MMR_Data["isPush"] = 0

   Pass_cases = pd.DataFrame()
   indx = df_FDDT_MMR_Data.index[(df_FDDT_MMR_Data["request_status"] == "Success")]
   if len(indx) > 0:
      Pass_cases['UID'] = df_FDDT_MMR_Data.loc[indx,'UID']
      Pass_cases['Pass_date'] = df_FDDT_MMR_Data.loc[indx,'response_time']
      Pass_cases.loc[:,'Pass_flag'] = 1
      Pass_cases = Pass_cases.drop_duplicates(subset='UID')
      Pass_cases = getReset(Pass_cases)
   
      Pass_cases.set_index("UID",inplace=True)
      mcr["isPush"] = mcr.UID.map(Pass_cases["Pass_flag"]) ## Error if Pass_cases have duplicate UIDs
      mcr["Push_date"] = mcr.UID.map(Pass_cases["Pass_date"]) ## Error if Pass_cases have duplicate UIDs
      
      df_FDDT_MMR_Data["isPush"] = df_FDDT_MMR_Data.UID.map(Pass_cases["Pass_flag"]) ## Error if Pass_cases have duplicate UIDs

   data = df_FDDT_MMR_Data
   indx = data.index[(data["isPush"] == 1.0)]
   data.drop(indx,axis=0,inplace=True)
   data = getReset(data)

   data.sort_values(by = ['UID', 'response_time'], inplace=True, ascending=False); 
   data = getReset(data)

   data = data.drop_duplicates(subset='UID')
   data = getReset(data)

   mcr["Error"] = "-"
   mcr["Error_date"] = "-"
   data.set_index("UID",inplace=True)
   mcr["Error"] = mcr.UID.map(data["response"])
   mcr["Error_date"] = mcr.UID.map(data["response_time"]) 

   mcr["Final_Status"] = "Pending: Not Pushed"

   indx = mcr.index[(mcr["Error"] == "Connection Timeout")]
   indx = mcr[mcr['Error']== "Connection Timeout"].index
   mcr.loc[indx,"Final_Status"] = 'Error: Timeout/Error/Fail'
   
   indx = mcr.index[(mcr["Error"] == "ERROR")]
   mcr.loc[indx,"Final_Status"] = 'Error: Timeout/Error/Fail'

   indx = mcr.index[(mcr["Error"] == "Fail")]
   mcr.loc[indx,"Final_Status"] = 'Error: Timeout/Error/Fail'

   ## Grouping MMR Errors.....

   MMR_Error_Ref = pd.read_excel('Cons_MMR_Error_Ref.xlsx')

   for aa in range(len(MMR_Error_Ref)):
      indx = mcr[mcr['Error'].str.contains(MMR_Error_Ref.loc[aa,'Search_string'])==True].index
      mcr.loc[indx,'L1_Status'] = MMR_Error_Ref.loc[aa,'Error_Msg']

   indx = mcr[mcr['Error'].str.contains("DB ERROR. TRY LATER")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: DB ERROR. TRY LATER'

   indx = mcr[mcr['Error'].str.contains("Meter Already Exists")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Meter Already Exists'

   indx = mcr[mcr['Error'].str.contains("READING_AT_DISCONN_IMP Should be greater then available meter reading")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: FR Should be greater then available meter reading'

   indx = mcr[mcr['Error'].str.contains("Replacement Date should be Greater Than Max Reading Available")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Replacement Date should be Greater Than Max Reading Available'

   indx = mcr[mcr['Error'].str.contains("Meter Initial Reading Import cannot be Empty")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Meter Initial Reading Import cannot be Empty'  

   indx = mcr[mcr['Error'].str.contains("Network Does not have old meter")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Network Does not have old meter' 

   indx = mcr[mcr['Error'].str.contains("Neumarator/Denominator")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Invalid CT-PT Ratios' 

   indx = mcr[mcr['Error'].str.contains("Bi-Directional Meter")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Meter Uni/Bi Directional Issue'

   indx = mcr[mcr['Error'].str.contains("Request Id Already Exists")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Request Id Already Exists - Repush'

   indx = mcr[mcr['Error'].str.contains("Invalid Meter Lab Number")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Invalid RST Details'

   indx = mcr[mcr['Error'].str.contains("METER_REFERENCE cannot be Empty")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: METER_REFERENCE cannot be Empty'

   indx = mcr[mcr['Error'].str.contains("Meter MF cannot be Zero or Negative")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Invalid Meter MF / EMF / Old Calculated MF'

   indx = mcr[mcr['Error'].str.contains("Old Meter Number cannot be Empty.Meter Status")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Old Meter Number & Meter Status cannot be Empty.'

   indx = mcr[mcr['Error'].str.contains("Old Meter Number cannot be Empty")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Old Meter Number cannot be Empty'

   indx = mcr[mcr['Error'].str.contains("Substation Code cannot be Empty or Alphanumeric.Not a valid MSEDCL SS_NO.Invalid DTC Code")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Invalid SS / FD / DT Code'

   indx = mcr[mcr['Error'].str.contains("Meter Status at the time of disconnection cannot be Empty")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Meter Status cannot be Empty'

   indx = mcr[mcr['Error'].str.contains("Invalid DTC Code")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Invalid DTC Code'

   indx = mcr[mcr['Error'].str.contains("This is Unmetered DTC")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: This is Unmetered DTC. Set Add Replace Flag as N'

   indx = mcr[mcr['Error'].str.contains("Not a valid MSEDCL DTC_CODE")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Invalid DTC Code'

   indx = mcr[mcr['Error'].str.contains("Old Meter Is Not Available in the System")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Old Meter Is Not Available in the System. Set Old Meter Status At Disconn as 2'

   indx = mcr[mcr['Error'].str.contains("Old Meter Already Active on Another DTC")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Old Meter Already Active on Another DTC'

   indx = mcr[mcr['Error'].str.contains("For input string")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Error Not Clear, Please Repush'

   indx = mcr[mcr['Error'].str.contains("multiple points")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: Error Not Clear, Please Repush'
   
   indx = mcr[mcr['Error'].str.contains("DTC Already have more than 1 Main Meter")==True].index
   mcr.loc[indx,'Final_Status'] = 'Error: DTC Already have more than 1 Main Meter'
   
   
   
   ManualDTStatus = ['4384170','4384623','4384451','4381090','4381723','4381720','4381722','4381814','4381709','4384645','4384632','4384186','4381120','4381427','3841539','4384680','4381724','4381430','4381826','4384636','4381494','4381483','4381375','4381341','4381330','4381202','4381776','4381726','4381713','4381370','4326701','4326142','4329421','4326757','4329408','4329446','4326372','4329477','4329459','4327705','4326368','3281354','3271194','3260132','4387731','4387634','4390356','4390127','4390123','4390100','4388341','4387729','4387698','4387695','4387694','4387680','4387676','4387675','4387672','4387669','4387660','4387659','4387657','4387656','4387637','4387631','4387628','4387624','4387613','4387217','3902527','3902381','3902358','390037','4390383','4390035','4390130','4387653','4387612','4387683','4811622','4390088','4811661','4361700','4361699','4361603','4359952','4359925','4359866','4359805','4358898','4358897','4358894','4358885','4358881','3595651','3595640','3595639','3595638','3595636','3595634','3595633','3595632','3595631','3595625','3595624','3595622','3595620','3595621','3595615','3595614','3595613','3595612','3595366','3591657','3591655','3591599','3591598','3591597','3591596','3591591','3591590','3591589','3591586','3591577','3591473','3591465','3591456','3591431','361638','361598','361591','359134','359124','4359799','4359927','4359920','3591595','4359926','3595644','3595383','3595630','3595770','4358895','4358886','4358882','4357642','4357602','4357503','3595643','3591432','3591566','3595641','4349473','359268','3595362','4681288','4684072','4686071','4681206','4683371','4683553','4683049','4682029','4686295','4686043','4872266','4686180','4686427','4686404','4686037','4686009','4680103','4683250','4684195','4683331','4684058','4686636','4684416','4686635','4686260','4684383','562577','4688759','4562939','4688243','4679261','4682185','562579','562578','4688267','4684425','4686158','4686412','4686429','4686159','4686708','4686034','4686068','4683027','4678535','4679204','4684660','4686744','4688871','4685251','4686477','4686036','4686163','562575','4688689','4688476','6881159','4686148','562580','3956025','3956024','3956039','3956032','4686146','4683408','4682009','4679312','4685303','3956026','6781282','4678497','4686926','4686707','4686630','4686556','4686554','4686553','4686496','4686492','4686482','4686468','4686441','4686433','4686428','4686388','4686385','4686289','4686275','4686248','4686226','4686225','4686223','4686203','4686202','4686032','4686031','4685482','4685241','4685226','4685215','4685214','4685136','4685114','4685107','4685067','4685041','4685036','4684667','4684624','4684446','4684203','4684183','4684170','4684131','4684053','4684052','4684004','4684003','4683776','4683775','4683774','4683771','4683761','4683736','4683667','4683535','4683302','4872291','4683249','4683208','4683188','4683173','4683155','4683152','4683046','4682324','4682147','4682076','4682060','4681254','4681252','4681250','4681216','4681020','4681008','4680627','4680622','4679681','4679662','4678936','4678935','4678923','4678580','4678572','4678571','4678540','4678537','4678533','4678520','4686731','4678518','4678505','4678325','4678296','4678018','6781197','4872292','4683551','6781205','4678566','6881210','4688174','4688680','4688744','4679669','4678479','4688608','4685068','4679653','4680130','6881220','4688158','6881018','4688157','4685160','4678547','6781216','4678582','4686915','4872005','4688463','6881026','4688004','4686463','6781234','6781233','4683666','4680114','4678539','4683737','6881204','4688230','4688019','4681255','4683807','4683469','4688718','4683466','4684020','4683292','4685059','4681234','4685122','4685138','4685117','4686899','6881200','4686389','4678480','4683786','4688660','4872001','4686194','4683695','4683489','4686747','4686405','4688652','4688651','6881019','4688630','4688721','4872002','4688479','4688528','4688480','6881202','4688448','4688524','6881147','4684656','4683191','4683036','4683362','4683342','4683333','4683604','4683022','6881013','4683240','4683375','4683038','4683435','4683521','4683426','4683386','4683609','4683685','4683013','4683303','4683414','4683691','4683253','4683285','4683236','4686149','6881198','4688264','4683522','4683004','4686419','4688752','4686921','4688761','4686160','6881179','4688749','4684417','4678549','4686942','4683384','6881016','4688563','4678324','4683436','4688688','4683758','4367895','6881199','6881177','6881154','6881153','6881009','6781258','6781222','6781153','6781151','6781014','4688993','4688892','4688890','4688852','4688754','4688676','4688655','4688644','4688632','4688618','4688616','4688609','4688588','4688564','4688561','4688556','4688361','4688353','4688349','4688346','4688345','4688344','4688342','4688340','4688169','4688022','4688003','4686920','4686914','4686898','4686897','4686792','4686761','4686760','4372850','3701297','4376254','3701255','4376012']
   for ii in range(len(ManualDTStatus)):
      indx = mcr.index[(mcr["FD_DT_Code"] == ManualDTStatus[ii])]
      mcr.loc[indx,'isPush'] = 1.0
      mcr.loc[indx,'Push_date'] = mcr.loc[indx,'survey_timings']


   indx = mcr.index[(mcr["isPush"] == 1.0)]
   mcr.loc[indx,'Final_Status'] = 'Success: Pushed to NDMS'
   
   DD = datetime.now().day
   MM = datetime.now().month
   YYYY = datetime.now().year
   Fname1 = 'MCR_DT_MMR_Final' + str("_") + str(DD) + str(MM) + str(YYYY)
   saveExcel(mcr,Fname1,'.xlsx') # Saving Final MCR
   Send2UI = ["DT MCR_MMR Final Data Saved to file : "+ str(Fname1) + str(',xlsx')]; tv2.insert(parent='', index=0, values=(Send2UI))

   # table = pd.pivot_table(mcr, values='UID', index=['circle', 'subdivision'],columns=['connectiontype'], aggfunc="count", fill_value=0)
   MCR_Summary = pd.pivot_table(mcr, values='UID', index=['circle'],columns=['connectiontype'], aggfunc="count", fill_value=0)
   MMR_Status = pd.pivot_table(mcr, values='UID', index=['Final_Status'],columns=['circle'], aggfunc="count", fill_value=0)
   MMR_Status1 = pd.pivot_table(mcr, values='UID', index=['Final_Status'], aggfunc="count", fill_value=0)
   
   # try:
   #    print('     ')
   #    print('*************Circle Wise FD MMR / Exception Status*************')
   #    pprint(MMR_Status)

   #    print('     ')
   #    print('*************FD MCR Data Summary Circle Wise*************')
   #    print(MCR_Summary.to_markdown())

   #    print('     ')
   #    print('*************FD MMR / Exception Status*************')
   #    print(MMR_Status1.to_markdown())

   #    Send2UI = [MCR_Summary.to_string()]; 
   #    tv2.insert(parent='', index=0, values=(Send2UI))
   # except: 
   #    pass

   try: 
      Fname = 'MCR_DT_MMR_Summary_' + str(DD) + str(MM) + str(YYYY) + str('.txt')
      with open(Fname, 'w') as outfile:
         outfile.write('\n')

      with open(Fname, 'a') as outfile:
         outfile.write('******************************Circle Wise DT MMR / Exception Status********************************** \n')       

      with open(Fname, 'a') as outfile:
         MMR_Status.to_string(outfile)

      with open(Fname, 'a') as outfile:
         outfile.write('\n\n\n*************DT MCR Data Summary Circle Wise************* \n')       

      with open(Fname, 'a') as outfile:
         MCR_Summary.to_string(outfile)

      with open(Fname, 'a') as outfile:
         outfile.write('\n\n\n*************DT MCR Data Summary Circle Wise************* \n')       

      with open(Fname, 'a') as outfile:
         MMR_Status1.to_string(outfile)
      
      Send2UI = ["DT MCR_MMR Final Data Summary Saved to file : "+ str(Fname)]; tv2.insert(parent='', index=0, values=(Send2UI))
      Send2UI = ["Programme Comeplted..!!"]; tv2.insert(parent='', index=0, values=(Send2UI))
   except Exception as e:
      print(str('Error in Function: **processdata** with Error Message : "') + str(e) + str('"'))   
      Send2UI = ["Error while Creating Summary File with Error Message : "+ str(e)]; tv2.insert(parent='', index=0, values=(Send2UI))
      Send2UI = ["Programme Execution Completed..!!"];                               tv2.insert(parent='', index=0, values=(Send2UI))




def Process_Cons_MMR_Data(): 
   isUpdatedMI = IsUpdateMI.get()
   isUpdate_Master_Main = isUpdatedMI
   isUpdateMMRL1_API = IsUpdateSAPAPI.get()
   isUpdateMMRL2_API = isUpdateMMRL1_API

   if isUpdatedMI == 'Yes':
      # get Feeder-DT MCR
      tv2.insert(parent='', index=0, values=([str('Capturing LT & HT Consumer MI Data..!! ')])) 
      mcr = getMCR_Master('cons')
      mcr.reset_index(inplace=True)
      Data_Storage(1,'ConsMI_mcrK',mcr)

      mmm = (str('Consumer MI Data Captured and Stored, Total MI : ') + str(len(mcr)))
      print(mmm)
      tv2.insert(parent='', index=0, values=([mmm])) 

      saveExcel(mcr,'MCR_Cons_Updated','.xlsx')
   else:
      mcr = Data_Storage(0,'ConsMI_mcrK',"")
      
      print('     ')
      mmm = "Consumer MI Data Loaded, Total MI : " + str(len(mcr))
      print(mmm)
      Send2UI = [mmm]; tv2.insert(parent='', index=0, values=(Send2UI))

   mcr["circle"] = mcr["circle"].replace(["Nagpur U"], "NAGPUR (U) CIRCLE")

   if isUpdateMMRL1_API == 'Yes':
      # get Consumer MMR L1
      tv2.insert(parent='', index=0, values=([str('Capturing MMR L1 data for Consumers..!! ')])) 
      df_MMR_L1_Data = getConsumer_MMR1_Data()
      df_MMR_L1_Data = getReset(df_MMR_L1_Data)
      Data_Storage(1,'Cons_MMR_L1',df_MMR_L1_Data)
      
      print('     ')
      mmm = (str('Consumer MMR L1 Data Captured and Stored, Total L1 Data : ') + str(len(df_MMR_L1_Data)))
      print(mmm)
      tv2.insert(parent='', index=0, values=([mmm])) 

      # saveExcel(df_MMR_L1_Data,'MMR_L1_Data_Raw','.xlsx')
   else:
      df_MMR_L1_Data = Data_Storage(0,'Cons_MMR_L1',"")
      
      print('     ')
      mmm = str('Consumer MMR L1 Data Loaded, Total L1 Data : ') + str(len(df_MMR_L1_Data))
      print(mmm)
      Send2UI = [mmm]; tv2.insert(parent='', index=0, values=(Send2UI))

   if isUpdateMMRL2_API == 'Yes':
      # get Consumer MMR L2
      df_MMR_L2_Data = getConsumer_MMR2_Data()
      df_MMR_L2_Data = getReset(df_MMR_L2_Data)
      Data_Storage(1,'Cons_MMR_L2',df_MMR_L2_Data)
      
      print('     ')
      mmm = (str('Consumer MMR L2 Data Captured and Stored, Total L2 Data : ') + str(len(df_MMR_L2_Data)))
      print(mmm)
      tv2.insert(parent='', index=0, values=([mmm])) 

      saveExcel(df_MMR_L2_Data,'MMR_L2_Data_Raw','.xlsx')
   else:
      df_MMR_L2_Data = Data_Storage(0,'Cons_MMR_L2',"")

      print('     ')
      mmm = str('Consumer MMR L2 Data Loaded, Total L2 Data : ') + str(len(df_MMR_L2_Data))
      print(mmm)
      Send2UI = [mmm]; tv2.insert(parent='', index=0, values=(Send2UI))
   
   df_MMR_L2_Data = df_MMR_L2_Data.drop_duplicates(subset='kno')
   df_MMR_L2_Data = getReset(df_MMR_L2_Data)
   
   if isUpdate_Master_Main == 'Yes':
      pass

   indx = df_MMR_L1_Data.index[(df_MMR_L1_Data["request_status"] == "Success.")]
   df_MMR_L1_Data.loc[indx,'request_status'] = "Success"
      
   print('Processing MCR & MMR API Data..!!')
   Send2UI = ['Processing MCR & MMR API Data..!!']; tv2.insert(parent='', index=0, values=(Send2UI))
   df_MMR_L1_Data["UID"] = df_MMR_L1_Data["kno"].astype(str) +"_"+ df_MMR_L1_Data["meterno"]
   mcr["UID"]            = mcr["kno"].astype(str) +"_"+ mcr["newmeterno"]
   
   Pass_cases = pd.DataFrame()
   indx = df_MMR_L1_Data.index[(df_MMR_L1_Data["request_status"] == "Success")]
   Pass_cases['UID'] = df_MMR_L1_Data.loc[indx,'UID']
   Pass_cases['Pass_date'] = df_MMR_L1_Data.loc[indx,'response_time']
   Pass_cases['meter_flag'] = df_MMR_L1_Data.loc[indx,'L1_meter_flag']
   Pass_cases.loc[:,'Pass_flag'] = 1
   Pass_cases = Pass_cases.drop_duplicates(subset='UID')
   Pass_cases = getReset(Pass_cases)

   mcr["L1_meter_flag"] = "-"
   mcr["L1_meter_flag"] = mcr.UID.map(Pass_cases["meter_flag"]) ## Error if Pass_cases have duplicate UIDs

   mcr["isPush"] = 0
   mcr["Push_date"] = 0
   Pass_cases.set_index("UID",inplace=True)
   mcr["isPush"] = mcr.UID.map(Pass_cases["Pass_flag"]) ## Error if Pass_cases have duplicate UIDs
   mcr["Push_date"] = mcr.UID.map(Pass_cases["Pass_date"]) ## Error if Pass_cases have duplicate UIDs
   
   df_MMR_L1_Data["isPush"] = 0
   df_MMR_L1_Data["isPush"] = df_MMR_L1_Data.UID.map(Pass_cases["Pass_flag"]) ## Error if Pass_cases have duplicate UIDs

   data = df_MMR_L1_Data
   indx = data.index[(data["isPush"] == 1.0)]
   data.drop(indx,axis=0,inplace=True)
   data = getReset(data)

   data.sort_values(by = ['UID', 'response_time'], inplace=True, ascending=False); 
   data = getReset(data)

   data = data.drop_duplicates(subset='UID')
   data = getReset(data)

   mcr["Error"] = "-"
   mcr["Error_date"] = "-"
   
   data.set_index("UID",inplace=True)
   mcr["Error"] = mcr.UID.map(data["response"]) ## Error if Pass_cases have duplicate UIDs
   mcr["Error_date"] = mcr.UID.map(data["response_time"]) ## Error if Pass_cases have duplicate UIDs
   mcr["meter_flag"] = mcr.UID.map(data["L1_meter_flag"]) ## Error if Pass_cases have duplicate UIDs
   mcr["Push_Status"] = mcr.UID.map(data["request_status"])

   mcr["L1_Status"] = "Pending: Not Pushed"

   indx = mcr.index[(mcr["isPush"] == 1.0)]
   mcr.loc[indx,'L1_Status'] = 'Success: Pushed to NC'

   ## Grouping MMR Errors.....

   Cons_MMR_Error_Ref = pd.read_excel('Cons_MMR_Error_Ref.xlsx')

   for aa in range(len(Cons_MMR_Error_Ref)):
    # Escape special regex characters in the search string
    search_pattern = re.escape(str(Cons_MMR_Error_Ref.loc[aa,'Search_string']))
    # Fill NA values with empty string and then check contains
    indx = mcr[mcr['Error'].fillna('').str.contains(search_pattern, regex=True)].index
    mcr.loc[indx,'L1_Status'] = Cons_MMR_Error_Ref.loc[aa,'Error_Msg']

   mcr["L2_date"] = "-"
   mcr["L2_AppID"] = "-"
   mcr["L2_meter_flag"] = "-"
   mcr["L2_Status"] = "NA"

   mcr["Final_Status"] = "L1 Failed" 
   mcr["Curr_Date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
   mcr["DaysPending"] = 5 # (pd.to_datetime(mcr["Curr_Date"]) - pd.to_datetime(mcr["survey_timings"])).dt.days
   mcr["DelayRange"] = "6. < 7 Days"

   indx = mcr.index[(mcr["DaysPending"] >= 60)]
   mcr.loc[indx,"DelayRange"] = "1. >60 Days"

   indx = mcr.index[(mcr["DaysPending"] < 60) & (mcr["DaysPending"] >= 45)]
   mcr.loc[indx,"DelayRange"] = "2. Btw 45 to 60"

   indx = mcr.index[(mcr["DaysPending"] < 45) & (mcr["DaysPending"] >= 30)]
   mcr.loc[indx,"DelayRange"] = "3. Btw 30 to 45"

   indx = mcr.index[(mcr["DaysPending"] < 30) & (mcr["DaysPending"] >= 15)]
   mcr.loc[indx,"DelayRange"] = "4. Btw 15 to 30"

   indx = mcr.index[(mcr["DaysPending"] < 15) & (mcr["DaysPending"] >= 7)]
   mcr.loc[indx,"DelayRange"] = "5. Btw 7 to 15"

   # input('Check and reload L2 Data..')
   # df_MMR_L2_Data = pd.read_excel('MMR_L2_Data_Raw.xlsx') 
   
   indx = df_MMR_L2_Data.index[(df_MMR_L2_Data["application_id"] == '54977665')]
   df_MMR_L2_Data.drop(indx,axis=0,inplace=True)
   df_MMR_L2_Data = getReset(df_MMR_L2_Data)

   data = df_MMR_L2_Data
   data.set_index("kno",inplace=True)
   mcr["L2_date"] = mcr.kno.map(data["entry_date"]) 
   mcr["L2_AppID"] = mcr.kno.map(data["application_id"]) 
   mcr["L2_Status"] = mcr.kno.map(data["current_workflow_status"]).fillna('NA') 
   mcr["meter_flag"] = mcr.kno.map(data["L2_meter_flag"]) 

   lst_exp = ['430080009041','430047014705','430047014721','390010665615','419990029360','410017306824','410024464729','415399026860',
              '410019003691_D','410019011180','450019104000_D','111111111110','410039023810','410019004698','430019004460','420819027090','422459027760','222222222220','LD_410039013660','411359022410_1','410019006390_1','410039027560_D','420819015550_1','439319054390','410019010820','435529055630','410039011420','410039021840','412759022000','420819008470','450019104000','510019015870','396239023350','410039027830','414819027610','415399028030','510019005470','510019013150','410019008590','510019007910','333333333331','419999104380','396519022650','LD_410039005500','LD_430019002190','LD_410569023800','452750000973']
   for ii in range(len(lst_exp)):
      indx = mcr.index[(mcr["kno"] == lst_exp[ii])]
      mcr.loc[indx,"L1_Status"] = 'Success: Pushed to NC'
      mcr.loc[indx,"L2_Status"] = 'Replacement Submitted' 

   statuses = [
    'Replacement Approved',
    'Application Approved',
    'Status  Meter Assignment Approved',
    'Status First Bill Generated'
   ]
   indx = mcr.index[
       (mcr["L1_Status"] == 'Success: Pushed to NC') &
       (mcr["L2_Status"].isin(statuses))
   ]
   mcr.loc[indx, "Final_Status"] = "L1 & L2 Approved - Advised in Billing"

   
   statuses = [
    'Replacement Rejected',
    'Application Rejected',
    'Status  Meter Assignment Rejected'
   ]
   indx = mcr.index[
       (mcr["L1_Status"] == 'Success: Pushed to NC') &
       (mcr["L2_Status"].isin(statuses))
   ]
   mcr.loc[indx, "Final_Status"] = "L1 Approved - L2 Rejected by SDO"
   
   
   statuses = [
    'Replacement Submitted',
    'Application In Process',
    'Status Receipt Approved'
    'Status  Meter Assignment Saved'
   ]
   indx = mcr.index[
       (mcr["L1_Status"] == 'Success: Pushed to NC') &
       (mcr["L2_Status"].isin(statuses))
   ]
   mcr.loc[indx, "Final_Status"] = "L1 Approved - L2 Pending with SDO"
   
   
   indx = mcr.index[(mcr["L1_Status"] == 'Success: Pushed to NC') & (mcr["L2_Status"] == 'Status Application Cancelled')]
   mcr.loc[indx,"Final_Status"] = "L1 Approved - L2 Cancelled"


   indx = mcr.index[(mcr["L1_Status"] == 'Success: Pushed to NC') & (mcr["L2_Status"] == 'NA')]
   mcr.loc[indx,"Final_Status"] = "L1 Approved - Not in L2"


   indx = mcr.index[(mcr["L1_Status"] == 'Pending: Not Pushed')]
   mcr.loc[indx,"Final_Status"] = "L1 Validation Pending / Data Rejected" 

   # Create the pivot table with Dates as columns, and both Line Types and Status as rows
   pivot_table = pd.pivot_table(mcr, index=['consumer_type', 'Final_Status'], columns='circle', aggfunc='size', fill_value=0)
   pivot_table1 = pd.pivot_table(mcr, index=['consumer_type', 'L1_Status'], columns='DelayRange', aggfunc='size', fill_value=0)
   pivot_table2 = pd.pivot_table(mcr, index=['Final_Status'], columns='circle', aggfunc='size', fill_value=0)
   pivot_table3 = pd.pivot_table(mcr, index=['consumer_type', 'L1_Status'], columns='circle', aggfunc='size', fill_value=0)

   DD = datetime.now().day
   MM = datetime.now().month
   YYYY = datetime.now().year
   file_path = 'MCR_Consumer_MMR_Final_' + str(DD) + str(MM) + str(YYYY) + str('.xlsx')

   # Create an Excel writer object and write both the data and pivot table to the file
   with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
      mcr.to_excel(writer, sheet_name='ConsMMR_Raw_Data', index=False)
      pivot_table.to_excel(writer, sheet_name='FinSts_Delay', index=True)
      pivot_table1.to_excel(writer, sheet_name='L1Sts_Delay', index=True)
      pivot_table2.to_excel(writer, sheet_name='FinSts_Circle', index=True)
      pivot_table3.to_excel(writer, sheet_name='L1Sts_Circle', index=True)

   print(f"Data saved successfully to {file_path}")
   Send2UI = ['MMR Processing Completed & Data Saved Successfully..!!']; tv2.insert(parent='', index=0, values=(Send2UI))


def process_NDMS_MeterData():
   pass


def Process_NFMS_Data():
   isUpdatedMI = IsUpdateMI.get()

   if isUpdatedMI == 'Yes':
      # get NFMS Tracker Data
      Nod = 2
      NFMSraw = getNFMD_Data(Nod)
      NFMSraw.reset_index(inplace=True)
      Data_Storage(1,'NFMS_rawK',NFMSraw)
      print('NFMS Raw Data Stored..!!')
      # tv2.insert(parent='', index=0, values=([str('NFMS Raw Data Stored..!!')])) 
      saveExcel(NFMSraw,'NFMS_Updated','.xlsx')
   else:
      NFMSraw = Data_Storage(0,'NFMS_rawK',"")
      print('NFMS Raw Data Loaded..!!')
      # tv2.insert(parent='', index=0, values=([str('NFMS Raw Data Loaded..!!')]))
      mmm = "Cummulative NFMS Data Loaded : " + str(len(NFMSraw))
      print(mmm)
      # Send2UI = [mmm]; tv2.insert(parent='', index=0, values=(Send2UI))    

   df = pd.DataFrame()
   df['DD'] = NFMSraw['response_time']
   df['Date'] = pd.to_datetime(df['DD']).dt.date
   df['Data_Type'] = NFMSraw['data_type']
   df['Status'] = NFMSraw['message']

   # Create the pivot table with Dates as columns, and both Line Types and Status as rows
   pivot_table = pd.pivot_table(df, index=['Data_Type', 'Status'], columns='Date', aggfunc='size', fill_value=0)

   print(pivot_table)

   DD = datetime.now().day
   MM = datetime.now().month
   YYYY = datetime.now().year
   file_path = 'NFMS_Data_n_Summary' + str("_") + str(DD) + str(MM) + str(YYYY) + str('.xlsx')

   # Create an Excel writer object and write both the data and pivot table to the file
   with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
      # Save the original data to the first sheet
      NFMSraw.to_excel(writer, sheet_name='NFMS_Raw_Data', index=False)
      
      # Save the pivot table to a second sheet
      pivot_table.to_excel(writer, sheet_name='Summary')

   print(f"Data saved successfully to {file_path}")


def Process_NFMS_Data_Analysis():
   # Process_FD_MMR_Data() # To Fetch and Prepare Curr FD MCR-MMR Report
   DD = datetime.now().day
   MM = datetime.now().month
   YYYY = datetime.now().year
   Fname1 = 'MCR_Feeder_MMR_Final' + str("_") + str(DD) + str(MM) + str(YYYY) + str('.xlsx')
   
   FD_data = pd.read_excel(Fname1)
   FD_data = FD_data.drop(columns=['Unnamed: 0','circle','subdivision','sdocode','substation','feedername','connectiontype',
                                   'OldMrtNo_Master','OldMrtMake_Master','OldMrtNo_Field','OldMrtMake_Field','OldMtr_kWh_Imp',
                                   'OldMtr_kWh_Exp','NewMtr_kWh_Imp','NewMtr_kWh_exp','Rejection Reason','UID','Error','Error_date'])
   
   dtType = ['Event','Block Load Survey','Daily Load Profile','Real Time Alarms','Billing Profile']

   isUpdatedMI = IsUpdateMI.get()
   Nod = 30
   if isUpdatedMI == 'Yes':
      # get NFMS Tracker Data
      NFMSraw = getNFMD_Data(Nod,dtType[1])
      # NFMSraw.reset_index(inplace=True)
      NFMSraw = getReset(NFMSraw)
      Data_Storage(1,'NFMS_rawK',NFMSraw)
      print('NFMS Raw Data Stored..!!')
      # tv2.insert(parent='', index=0, values=([str('NFMS Raw Data Stored..!!')])) 
      saveExcel(NFMSraw,'NFMS_Updated','.xlsx')
   else:
      NFMSraw = Data_Storage(0,'NFMS_rawK',"")
      print('NFMS Raw Data Loaded..!!')
      tv2.insert(parent='', index=0, values=([str('NFMS Raw Data Loaded..!!')]))
      mmm = "Cummulative NFMS Data Loaded : " + str(len(NFMSraw))
      print(mmm)
      Send2UI = [mmm]; tv2.insert(parent='', index=0, values=(Send2UI))    

   df = pd.DataFrame()
   df['DD'] = NFMSraw['response_time']
   df['Date'] = pd.to_datetime(df['DD']).dt.date
   df['Data_Type'] = NFMSraw['data_type']
   df['Status'] = NFMSraw['message'] 

   ## Processing NFMS Data..
   NFMS_BL0 = pd.DataFrame()
   indx = NFMSraw.index[(NFMSraw["data_type"] == 'Block Load Survey')]
   NFMS_BL0 = NFMSraw.loc[indx,:]
   NFMS_BL0 = getReset(NFMS_BL0)

   indx = NFMS_BL0[NFMS_BL0['response_code'].str.contains("Success")==True].index
   NFMS_BL1 = NFMS_BL0.loc[indx,:]
   NFMS_BL1 = getReset(NFMS_BL1)
    
   # NFMS_BL = NFMS_BL1.drop(columns=['id','transid','feedercode','data_type','data_type_slot','max_id','response_code','message','details'])
   # NFMS_BL['MeterNo'] = NFMS_BL['mtrno'].str[-9:]

   # for ii in range(len(NFMS_BL)):
   #    json_data = NFMS_BL.loc[ii,'data']
   #    dt2 = pd.read_json(json_data)
   #    dt2['LoadProfile'] = dt2['LoadProfile'].astype(str).str.replace("'meter_rtc': '", "#", regex=False)
   #    dt2[['BeforeHash', 'AfterHash']] = dt2['LoadProfile'].str.split('#', expand=True)
   #    dt2['RTC'] = dt2['AfterHash'].str[:10]

   #    NFMS_BL.loc[ii,'mtrRTC'] = dt2.loc[0,'RTC']
   #    NFMS_BL.loc[ii,'IPCount'] = len(dt2)
   
   Data_Storage(1,'NFMS_BLP_Proc',NFMS_BL)
   NFMS_BL = Data_Storage(0,'NFMS_BLP_Proc',"")
   curr_date = datetime.now() 
   
   NFMS_BL_RTC = pd.DataFrame()
   NFMS_BL_RTC_summ = pd.DataFrame()
   for iii in range(Nod):   
      mtrRTC = (curr_date - timedelta(days=iii)).strftime('%Y-%m-%d')
      NFMS_BL_RTC_summ.loc[iii,'mtrRTC'] = mtrRTC

      indx = NFMS_BL.index[(NFMS_BL["mtrRTC"] == mtrRTC)]
      NFMS_BL_RTC0 = NFMS_BL.loc[indx,:]
      NFMS_BL_RTC0 = getReset(NFMS_BL_RTC0)
      
      NFMS_BL_RTC_summ.loc[iii,'totCount'] = 0
      if len(NFMS_BL_RTC0) > 0: 
         NFMS_BL_RTC = NFMS_BL_RTC._append(NFMS_BL_RTC0)
         NFMS_BL_RTC_summ.loc[iii,'totCount'] = len(NFMS_BL_RTC0)

   NFMS_BL_RTC = NFMS_BL_RTC.drop(columns=['time_stamp','mtrno','data'])
   DD = datetime.now().day
   MM = datetime.now().month
   YYYY = datetime.now().year
   file_path = 'NFMS_DataPush_BLP_' + str(DD) + str(MM) + str(YYYY) + str('.xlsx')

   with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
      FD_data.to_excel(writer, sheet_name='FD_data', index=False)
      NFMS_BL_RTC_summ.to_excel(writer, sheet_name= 'BLP_Summary', index=False)
      NFMS_BL_RTC.to_excel(writer, sheet_name= 'BLP_Total', index=False)
   
   print('NFMS BLP Data Processed and Saved to File ' + file_path)

   ## Processing NFMS Data for Daily Profile...
   # NFMS_DL0 = pd.DataFrame()
   # indx = NFMSraw.index[(NFMSraw["data_type"] == 'Daily Load Profile')]
   # NFMS_DL0 = NFMSraw.loc[indx,:]
   # NFMS_DL0 = getReset(NFMS_DL0)

   # indx = NFMS_DL0[NFMS_DL0['response_code'].str.contains("Success")==True].index
   # NFMS_DL1 = NFMS_DL0.loc[indx,:]
   # NFMS_DL1 = getReset(NFMS_DL1)

   # pushDate = "2025-02-28"
   # mtrRTC = "2025-02-26"

   # indx = NFMS_DL1[NFMS_DL1['response_time'].astype(str).str.contains(pushDate)==True].index
   # NFMS_DL = NFMS_DL1.loc[indx,:]
   # NFMS_DL = getReset(NFMS_DL)
   
   # NFMS_DL = NFMS_DL.drop(columns=['id','transid','feedercode','data_type','data_type_slot','max_id','response_code','message','details'])
   # NFMS_DL['MeterNo'] = NFMS_DL['mtrno'].str[-9:]
   # for ii in range(len(NFMS_DL)):
   #     json_data = NFMS_DL.loc[ii,'data']
   #     dt2 = pd.read_json(json_data)
   #     dt2.loc['meter_rtc','DailyProfile'] = dt2.loc['meter_rtc','DailyProfile'][:10]

   #     NFMS_DL.loc[ii,'mtrRTC'] = dt2.loc['meter_rtc','DailyProfile']
   #     NFMS_DL.loc[ii,'IPCount'] = 1
   
   # indx = NFMS_DL.index[(NFMS_DL["mtrRTC"] == mtrRTC)]
   # NFMS_DL_RTC = NFMS_DL.loc[indx,:]
   # NFMS_DL_RTC = getReset(NFMS_DL_RTC)

   
   # NFMS_DL_RTC = NFMS_DL_RTC.drop(columns=['time_stamp','mtrno','data'])
		
   # DD = datetime.now().day
   # MM = datetime.now().month
   # YYYY = datetime.now().year
   # file_path = 'NFMS_DataPush_BLDP_' + str(DD) + str(MM) + str(YYYY) + str('.xlsx')

   # with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
   #    FD_data.to_excel(writer, sheet_name='FD_data', index=False)
   #    # NFMS_BL_RTC.to_excel(writer, sheet_name='NFMS_BL_RTC', index=False)
   #    # NFMS_DL_RTC.to_excel(writer, sheet_name='NFMS_DL_RTC', index=True)


def ProcessNDM_API_data():
   FD_MCR = Process_FD_MMR_Data()
   FD_MCR = FD_MCR.drop(columns=['newmetermake','connectiontype','OldMrtNo_Master','OldMrtMake_Master','OldMrtNo_Field',	'OldMrtMake_Field','OldMtr_kWh_Imp','OldMtr_kWh_Exp','NewMtr_kWh_Imp','NewMtr_kWh_exp','Rejection Reason','UID','Error','Error_date','Final_Status','circle','subdivision','substation','feedername'])
   YYYY = 2025
   MM = 1
   DD = 1
   tday = 24 #datetime.now().day

   ## Processing NDM API Data - BLP
   dt_type = 'BLP'   
   clmn = ['Push_date','BLP_date','meter_number','category','response_msg','count']
   df = pd.DataFrame(columns=clmn)
   while DD <= tday:
      dt = datetime(YYYY, MM, DD)
      date_data = dt + timedelta(days=2)
      df0 = getNDM_API_data(dt_type,dt)
      if len(df0) > 0:
         df0['Push_date'] = date_data.strftime("%Y-%m-%d")
         df0['BLP_date'] =  dt.strftime("%Y-%m-%d")
         df0['category'] =  'Feeder'
         df = df._append(df0) 
      print(len(df0),' Meters Data Processed for ', dt.strftime("%Y-%m-%d"), ' at ', date_data.strftime("%Y-%m-%d"))       
      DD = DD + 1
   
   df['response_sts'] =  df['response_msg']

   Uniq_sts = df['response_sts'].unique()
   processed_values = [ 'Success' if item == 'success' else 
                           'Not Pushed' if item is None else 
                           'Data Error' for item in Uniq_sts]
   df['response_sts'] = df['response_sts'].replace(dict(zip(Uniq_sts, processed_values)))

   # MDAS_BLP = pd.pivot_table(df, values='meter_number', index=['BLP_date','category'],columns=['response_sts'], aggfunc="count", fill_value=0)
   # print(MDAS_BLP)
   
   # FD_MCR = pd.read_excel('MCR_Feeder_Final.xlsx')
   FD_MCR = FD_MCR.rename(columns={'newmeterno': 'meter_number'})
   FD_MCR['Tot_Tg'] = 0
   FD_MCR['Tot_Success'] = 0
   FD_MCR['Tot_Error'] = 0
   FD_MCR['Tot_NP'] = 0
   # df = pd.read_excel('NDM_BLP_12_2024.xlsx')
   dt = datetime(YYYY, MM, DD)

   for ii in range(len(FD_MCR)):
      try:
         if FD_MCR.loc[ii,'Push_date'] < dt:
            FD_MCR['Tot_Tg'] = 31*96
         elif FD_MCR.loc[ii,'Push_date'] > dt:
            ddiff = FD_MCR.loc[ii,'Push_date'] - dt
            FD_MCR['Tot_Tg'] = ddiff*96
      except:
         FD_MCR['Tot_Tg'] = 31*96

   for _, row in df.iterrows():
      # Construct the column name in MCR based on date and status
      column_name = f"{row['BLP_date']}({row['response_sts']})"
      # Update the corresponding cell in MCR
      FD_MCR.loc[FD_MCR["meter_number"] == row["meter_number"], column_name] = row["count"]
   
   # Filter columns containing 'XYZ'
   S_columns = FD_MCR.filter(like='Success').columns
   F_columns = FD_MCR.filter(like='Error').columns
   NP_columns = FD_MCR.filter(like='Not').columns

   FD_MCR['Tot_Success'] = FD_MCR[S_columns].sum(axis=1)
   FD_MCR['Tot_Error'] = FD_MCR[F_columns].sum(axis=1)
   FD_MCR['Tot_NP'] = FD_MCR[NP_columns].sum(axis=1)

   fname = 'NDM_BLP_' + str(MM) + str('_') + str(YYYY) + str('.xlsx')
   FD_MCR.to_excel(fname)


def ProcessHTCT_Monthly_Bill():
    pass


def process_MMR_data(): 
   ## Checking if DB is accessible..!!
   DBFlag = 1
   # try:
   #    tv2.insert(parent='', index=0, values=([str('Checking for VPN / DB Connectivity..!!')])) 
   #    ErrMsg = 'DB is Not reachable, Please check VPN / DB Connectivity and try again..!!'
   #    qry = "select max(survey_timings) from ami_master.survey_output"
   #    df = fetchDBData('MSEDCL','WFM',qry,['MIDate']) # Fetching Data from survey_output table
   #    if len(df) == 0:
   #       tv2.insert(parent='', index=0, values=([str(ErrMsg)])) 
   #       print(ErrMsg) 
   #       DBFlag = 0 
   #    else:
   #       tv2.insert(parent='', index=0, values=([str('VPN / DB Connected Successfully..!!')])) 
   # except:
   #    DBFlag = 0 
   #    tv2.insert(parent='', index=0, values=([str(ErrMsg)])) 
   #    print(ErrMsg)
   
   # try:
   if DBFlag == 1:   
      MMR_Type = procType.get()
      if MMR_Type == 'Consumer MMR':        Process_Cons_MMR_Data()
      if MMR_Type == 'Feeder MMR':          Process_FD_MMR_Data()
      if MMR_Type == 'DT MMR':              Process_DT_MMR_Data()
      if MMR_Type == 'SAP Data Sync':       ProcessSAP_API_Data()
      if MMR_Type == 'NFMS Push Data':      Process_NFMS_Data_Analysis()
      if MMR_Type == 'MDAS Push Data':      ProcessMDAS_API_data()
   # except Exception as e:
   #    print(str('Error in Function: **processdata** with Error Message : "') + str(e) + str('"'))   
   #    Send2UI = ["Error while Processing data with Error Message : "+ str(e)]; tv2.insert(parent='', index=0, values=(Send2UI))
   #    Send2UI = ["Programme Execution Ends with Error..!!"];                         tv2.insert(parent='', index=0, values=(Send2UI))
      

def process_dummy():
    current_working_directory = os.getcwd()
    print(current_working_directory)

### ***************************** Front End Code***************************************




def Login():
    Udata = {
            "Uname": ['admin','appuser','a',""],
            "Pword": ['Y@dav3021','app@123','b',""]
            }
    Userdata = pd.DataFrame(Udata)

    CDT = datetime.now()
    DD = CDT.day
    MM = CDT.month
    YY = CDT.year
    SecPatch = (YY*100+MM)*100+DD
    
    if SecPatch <= 20251231:
        E_Umane = entry1.get()
        E_Pword = entry2.get() 

        entry_flag = 0 # 0: No Entry, 1: Admin Usr, 2: App User
        try:
            Indx = Userdata.index[Userdata['Uname'] == E_Umane]
            Indx = int(Indx[0])
            if Userdata.loc[Indx,'Pword'] == E_Pword:
                if E_Umane == 'admin':
                    entry_flag = 1
                    welcomemsg.set('Welcome Admin..!!')
                else:
                    entry_flag = 2
                    welcomemsg.set('Welcome App User..!!')
                windows.deiconify() #Unhides the root window
                top.destroy() #Removes the toplevel window
            else:
                Msg = 'Password is Wrong..!!'
                logmsg.set(Msg)
        except:
            Msg = 'User Name or Password is Wrong..!!'
            logmsg.set(Msg)
        # print(entry_flag)
        # Dt1 = Data_Storage(1,'etflag',entry_flag)
    else:
        Msg = 'Login Expired..!!'
        logmsg.set(Msg)








def Cancel():
    top.destroy() #Removes the toplevel window
    windows.destroy() #Removes the hidden root window
    sys.exit() #Ends the script

windows = Tk()
windows.wm_title("Comman DashBoard Tool - AMISP")
# Gets the requested values of the height and widht.
windowWidth = windows.winfo_reqwidth()
windowHeight = windows.winfo_reqheight()
# Gets both half the screen width/height and window width/height
positionRight = int(windows.winfo_screenwidth()/2 - windowWidth/2)
positionDown = int(windows.winfo_screenheight()/2 - windowHeight/2)

# Positions the window in the center of the page.
# windows.geometry("1300x600+{}+{}".format(positionRight, positionDown))
windows.geometry("1260x630+100+100")
windows.resizable(False, False)
# p1 = PhotoImage(file='bot.png')
# windows.iconphoto(False, p1)
BG_Clr = 'spring green'
windows.configure(bg=BG_Clr)

## Login Page

top = Toplevel(bg=BG_Clr) #Creates the toplevel window
# Gets the requested values of the height and widht.
windowWidth = top.winfo_reqwidth()
windowHeight = top.winfo_reqheight()
# Gets both half the screen width/height and window width/height
positionRight = int(top.winfo_screenwidth()/2 - windowWidth/2)
positionDown = int(top.winfo_screenheight()/2 - windowHeight/2)
# Positions the window in the center of the page.
top.geometry("300x200+{}+{}".format(positionRight, positionDown))
# top.geometry("300x200+300+300")
# set window color

Tl = Label(top,text = "User Name : ", font="Arial 10", bg=BG_Clr, anchor=CENTER); Tl.pack(); Tl.place(x=30,y=30)
Tl1 = Label(top,text = "Password   : ", font="Arial 10", bg=BG_Clr, anchor=CENTER); Tl1.pack(); Tl1.place(x=30,y=60)
entry1 = Entry(top, font="Arial 10"); entry1.pack(); entry1.place(x=120,y=30) #Username entry
entry2 = Entry(top, font="Arial 10", show='*'); entry2.pack(); entry2.place(x=120,y=60) #Password entry
button1 = Button(top, width=10, text="Login", command=lambda:Login()); button1.pack(); button1.place(x=60,y=100) #Login button
button2 = Button(top, width=10, text="Cancel", command=lambda:Cancel()); button2.pack(); button2.place(x=150,y=100) #Cancel button
logmsg = StringVar()
Tl2 = Label(top,text = "Password   : ", font="Arial 10", width=32, bg=BG_Clr, textvariable=logmsg, anchor=CENTER); Tl2.pack(); Tl2.place(x=20,y=135)

l0 = Label(windows,text = "Comman DashBoard Tool - MSEDCL Project", relief=FLAT , font="Helvetica 16 bold italic", anchor=CENTER, fg = "dark green",bg = BG_Clr)
l0.pack(); l0.place(x=450,y=3)

welcomemsg = StringVar()
l001 = Label(windows, textvariable = welcomemsg, relief=FLAT , font="Helvetica 10 bold italic", anchor=CENTER, fg = "gray27", bg = BG_Clr)
l001.pack(); l001.place(x=10,y=5)

timevar = StringVar()
l01 = Label(windows, textvariable=timevar, relief=FLAT , font="Helvetica 10 bold italic", anchor=CENTER, fg = "gray27", bg = BG_Clr)
l01.pack(); l01.place(x=950,y=7)

Curr_Path = str(os.getcwd()) + str("\\") + str(os.path.basename(__file__))
l110 = Label(windows,text = Curr_Path, relief=FLAT , font="Helvetica 12 bold italic", anchor=CENTER, fg = "dark green",bg = BG_Clr)
l110.pack(); l110.place(x=10,y=595)

tab_input = ttk.Notebook(windows) 
# tab1 = ttk.Frame(tab_input, width=700, height=150); tab_input.add(tab1, text = 'SLA DB')
tab2 = ttk.Frame(tab_input, width=700, height=150); tab_input.add(tab2, text = 'MI-MMR Status')

tab_input.pack(expand=1, fill="both")
tab_input.place(x=10,y=35)

# SLA_Fr = LabelFrame(tab1, text="");              SLA_Fr.pack();  SLA_Fr.place(x = 5, y = 5, width=430, height=217)
DDB_Fr = LabelFrame(tab2, text="User-Input"); DDB_Fr.pack();  DDB_Fr.place(x = 5, y = 5, width=500, height=217)

YN_List = ['Yes','No']
Curr_MM = int(datetime.now().strftime("%m"))
Curr_YY = int(datetime.now().strftime("%Y"))

li2 = Label(DDB_Fr,text = "Update MI Data :", relief=FLAT ); li2.pack(); li2.place(x=5,y=10)
IsUpdateMI = StringVar()
IsUpdateMI.set(YN_List[0])
SymblDrop = OptionMenu(DDB_Fr , IsUpdateMI , *YN_List); SymblDrop.pack(); SymblDrop.place(x=140,y=4)

li8 = Label(DDB_Fr,text = "Update MMR Data :", relief=FLAT ); li8.pack(); li8.place(x=5,y=45) 
IsUpdateSAPAPI = StringVar()
IsUpdateSAPAPI.set(YN_List[0])
OrdTypeDrop = OptionMenu(DDB_Fr , IsUpdateSAPAPI , *YN_List ); OrdTypeDrop.pack(); OrdTypeDrop.place(x=140,y=39)

Opt1_List = ['Consumer MMR','Feeder MMR','DT MMR','SAP Data Sync', 'NFMS Push Data', 'MDAS Push Data Analysis']

li8 = Label(DDB_Fr,text = "MI Type :", relief=FLAT ); li8.pack(); li8.place(x=250,y=10)
procType = StringVar()
procType.set(Opt1_List[0])
OrdTypeDrop = OptionMenu(DDB_Fr , procType , *Opt1_List ); OrdTypeDrop.pack(); OrdTypeDrop.place(x=330,y=4)

DDB_bt = Button(DDB_Fr, text = "GET MMR STATUS",width = 20, relief='raised', font=("Arial", 11, "bold"), bg = 'gray', command = thd.Thread(target = process_MMR_data).start); 
DDB_bt.pack(); DDB_bt.place(x=50,y=90)          

DDB_bt1 = Button(DDB_Fr, text = "Open Folder",width = 10, relief='raised', font=("Arial", 9, "bold"), bg = 'green', command = thd.Thread(target = process_dummy).start); 
DDB_bt1.pack(); DDB_bt1.place(x=278,y=90)     

tab_trade = ttk.Notebook(windows) 
tab11 = ttk.Frame(tab_trade, width=700, height=337); 
tab_trade.add(tab11, text = '   SLA_BLP  ')
tab12 = ttk.Frame(tab_trade); 
tab_trade.add(tab12, text = '   SLA_DLP   ')
# tab13 = ttk.Frame(tab_trade); tab_trade.add(tab13, text = 'Terminal Data')

tab_trade.pack(expand=1, fill="both")
tab_trade.place(x=10,y=220)						

tv0 = ttk.Treeview(tab11, column=[1,2,3,4,5,6,7,8,9], show='headings',height=15)
tv0.heading(1, text="SLA_Date")
tv0.column(1, anchor="center", width=80)
tv0.heading(2, text="MI_Date")
tv0.column(2, anchor="center", width=80)
tv0.heading(3, text="Tot_Meters")
tv0.column(3, anchor="center", width=75)
tv0.heading(4, text="D_Tot_BLP")
tv0.column(4, anchor="center", width=75)
tv0.heading(5, text="D_Act_BLP")
tv0.column(5, anchor="center", width=75)
tv0.heading(6, text="D_BLP_%")
tv0.column(6, anchor="center", width=75)
tv0.heading(7, text="M_Tot_BLP")
tv0.column(7, anchor="center", width=75)
tv0.heading(8, text="M_Act_BLP")
tv0.column(8, anchor="center", width=75)
tv0.heading(9, text="M_BLP_%")
tv0.column(9, anchor="center", width=75)

tv0.pack(); tv0.place(x=1,y=1)


tv1 = ttk.Treeview(tab12, column=[1,2,3,4,5,6,7,8,9], show='headings',height=15)
tv1.heading(1, text="SLA_Date")
tv1.column(1, anchor="center", width=80)
tv1.heading(2, text="MI_Date")
tv1.column(2, anchor="center", width=80)
tv1.heading(3, text="Tot_Meters")
tv1.column(3, anchor="center", width=75)
tv1.heading(4, text="D_Tot_DLP")
tv1.column(4, anchor="center", width=75)
tv1.heading(5, text="D_Act_DLP")
tv1.column(5, anchor="center", width=75)
tv1.heading(6, text="D_DLP_%")
tv1.column(6, anchor="center", width=75)
tv1.heading(7, text="M_Tot_DLP")
tv1.column(7, anchor="center", width=75)
tv1.heading(8, text="M_Act_DLP")
tv1.column(8, anchor="center", width=75)
tv1.heading(9, text="M_DLP_%")
tv1.column(9, anchor="center", width=75) 


tv1.pack(); tv1.place(x=1,y=1)


tv2 = ttk.Treeview(windows, column=[1], show='headings',height=50)
tv2.heading(1, text='                Message Dashboard              ')
tv2.column(1, anchor="w", width=520)


tv2.pack(); tv2.place(x=720,y=35)


msgBoard=StringVar()
s11 = Label(windows, textvariable = msgBoard , width = 64, height=6, relief=FLAT, bg="light cyan", font=("Arial", 10), anchor = 'w'); 
s11.pack(); s11.place(x=722,y=530)
msgBoard.set('-------------------------------------------------------------------')

windows.withdraw() #This hides the main window, it's still present it just can't be seen or interacted with
windows.mainloop() #Starts the event loop for the main window