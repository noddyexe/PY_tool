import psycopg2 #pip install psycopg2
import os
import pandas as pd
from datetime import datetime
from datetime import timedelta
import math
# os.system('cls')

# connect to the PostgreSQL server

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


def fetchDBData(Prj_in,Module,qry,clmn):
   Project =  {'MSEDCL':{'IP':'10.255.26.14',
                        'Port':'6412',
                        'uid':'praveen',
                        'pwd':'$2024praveen',
                        'HES':'hes_mppkvvcl_live',
                        'WFM':'masterdata',
                        'MDM':'mdm_mppkvvcl_live'}
               }

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


def fetchDBData_inparts(Prj_in,Module,qry,clmn):
   Project = {'TS1506':{'IP':'10.48.166.203',
                        'Port':'6412',
                        'uid':'praveen',
                        'pwd':'bcits@123',
                        'HES':'hes_mppkvvcl_live',
                        'MDM':'mdm_mppkvvcl_live'},
            'TS1507':{'IP':'""',
                        'Port':'',
                        'uid':'',
                        'pwd':'',
                        'HES':'HES2507',
                        'MDM':'MDM1507'}}

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


# Get Cummulative MI Data 
def getMCR_Master(dtype):
## Get MCR from WFM..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'
   clmn = ['circle','subdivision','sdocode','feedercode','kno','survey_timings','newmeterno','newmetermake','connectiontype','verify_status','consumer_type']
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
            msg_body = str("LT Consumer MI Data Received for ") + str(YY) + str("-") + str(MM) + str(' :')
            try:
               qry1 = "select circle,subdivision,sdocode,feedercode,kno,survey_timings,newmeterno,newmetermake,connectiontype,verify_status,consumer_type \
                        from ami_master.survey_output where survey_timings like '"
               qry = qry1 + str(YY) + str("-") + str(MM) + "%'"
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

   # Fetching HTCT Consumer
   qry = "select circle,subdivision,sdocode,feedercode,kno,survey_timings,newmeterno,newmetermake,	\
            kno as connectiontype,verify_status,consumer_category as consumer_type from ami_master.htct_meter_installation_details"
   df0 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   df0['connectiontype'] = '3-Phase'
   df0['consumer_type'] = 'HTCT_Cons'
   
   df = df._append(df0)
   print("HTCT Consumer MI Data Received : ", len(df0), ', Total Count : ', len(df))

   # Fetching LTCT Consumer
   # qry = "select circle,subdivision,sdocode,feedercode,kno,survey_timings,newmeterno,newmetermake,	\
   #        kno as connectiontype,verify_status,consumer_category as consumer_type from ami_master.htct_meter_installation_details"
   # df1 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   # df1['connectiontype'] = '3-Phase'
   # df1['consumer_type'] = 'LTCT_Cons'
   # print("LTCT Consumer MI Data Received : ", len(df1))
   # df = df._append(df1)

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

# getMCR_Master("dtype")

# Get Feeder-DT MI Data 
def getFDDT_MCR_Master(dtype):
## Get MCR from WFM..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'

   clmn = ['circle','subdivision','sdocode','substation','feedername','FD_DT_Code','survey_timings','newmeterno','newmetermake','connectiontype','verify_status'
            ,'OldMrtNo_Master','OldMrtMake_Master','OldMrtNo_Field','OldMrtMake_Field','OldMtr_kWh_Imp','OldMtr_kWh_Exp','NewMtr_kWh_Imp','NewMtr_kWh_exp','Rejection Reason']
   df = pd.DataFrame()
   if dtype == 'FD': 
      qry = "select circle,subdivision,sdocode,substation,feedername,fdrcode,surveyed_date,new_meter_serial_number,new_meter_make,  \
             new_meter_phase,verify_status, feeder_meter_serial_number,feeder_meter_make,oldmtrno_in_field,oldmtrmake_in_field,\
			    kwh_old_meter_reading,kwh_old_meter_reading_export,kwh_new_meter_reading,kwh_new_meter_reading_export,rej_reason\
             from ami_master.feeder_meter_installation_data"
      df1 = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from dt_meter_replacement_details table
      df1['connectiontype'] = 'HTCT_FD'
      print("Feeder MI Data Received : ", len(df1))
      df = df._append(df1)

   if dtype == 'DT': 
      qry = "select circle,subdivision,sdocode,substation,feedername,dtcode,surveyed_date,new_meter_serial_number,new_meter_make,\
             new_meter_make,verify_status,mtr_sr_no,mtr_make,oldmtrno_in_field,oldmtrmake_in_field,old_mtrreading_kwh,\
             old_mtrreading_kwh,new_mtr_rdgkwh,new_mtr_rdgkwh,remark\
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

   qry = "SELECT 'id',	'time_stamp',	'api_name',	'request_body',	'response_body',	'kno',	'meterno',	'flag' FROM ami_master.sap_api_tracker WHERE api_name='pushAllInstallDataNew' order by time_stamp desc"
   df = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   print("SAP API Tracker Table Data Received : ", len(df))
   return df
    

def getFDDT_MMR_Data():
## Get Tracker Data for WFM-SAP Data Sync..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'

   clmn = ['id','feeder_dt_code','survey_time','request_time','response_time','response','request_data','request_status','category','entry_by']

   qry = "select * from ami_master.arms_feeder_dt_replacement_tracking  order by response_time desc"
   df = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   print("SAP API Tracker Table Data Received : ", len(df))

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

   clmn = ['id','kno','meterno','survey_time','response_time','response','request_status','old_applicationid','old_response', \
         'old_entrydate','new_applicationid','new_response','new_entrydate','old_payload','new_payload','L1_meter_flag']
   df = pd.DataFrame(columns=clmn)

   for tmi in range(aa1):
      r1 = tmi * 10000
      r2 = r1 + 10000
      # print(r1,"....",r2)
      Flag = True
      while Flag:
         isPass = 0
         # Fetching LT Consumer
         if True: # dtype == 'cons':
            msg_body = str("Consumer MMR L1 API Tracker Data Received for Lot ") + str(tmi+1) + str(' :')
            try:
               qry1 = "select id,kno,meterno,survey_time,response_time,response,request_status,old_applicationid,meter_flag,old_entrydate, \
                        new_applicationid,meter_flag,new_entrydate,meter_flag,meter_flag, meter_flag \
                        from ami_master.arms_mtr_replacement_tracking where id >= "

               qry = qry1 + str(int(r1)) + str(" and id < ") + str(int(r2)) + str(" order by meterno, request_time")
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


# getConsumer_MMR1_Data()

def getConsumer_MMR2_Data_old():
## Get Tracker Data for Consumer MMR L2 API..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'

   clmn = ['smart_meter_flag',	'api_execution_status_code',	'api_execution_status_message',	'application_id',	'sdocode',	'kno', \
           	'current_workflow_status',	'current_workflow_status_id',	'remark',	'replacement_date',	'entry_date',	'mastertable_status','L2_meter_flag']

   qry = "select smart_meter_flag,api_execution_status_code,api_execution_status_message,application_id,sdocode,kno, current_workflow_status, \
            current_workflow_status_id,remark,replacement_date,entry_date,mastertable_status,meter_flag \
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
                           current_workflow_status_id,remark,replacement_date,entry_date,mastertable_status,meter_flag \
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


def getNFMD_Data(Nod):
## Get Tracker Data for NFMS Data Push..!!
   Prj_in = 'MSEDCL'
   Module = 'WFM'

   frmDate1 = datetime.now() - timedelta(days=Nod)
   frmDate = frmDate1.strftime('%Y-%m-%d')

   dtType = ['Event',	'Block Load Survey',	'Daily Load Profile',	'Real Time Alarms',	'Billing Profile']
   clmn = ['id','time_stamp','transid','feedercode','mtrno','data_type','data_type_slot','data','max_id','response_time','response_code','message','details']

   qry = str('select * from ami_master.nfms_tracker where response_time >= ') + str("'") + str(frmDate) + str("'") + str(' order by response_time desc')
   
   df = fetchDBData(Prj_in,Module,qry,clmn) # Fetching Data from survey_output table
   print("Consumer MMR L2 API Tracker Data Received : ", len(df))

   return df