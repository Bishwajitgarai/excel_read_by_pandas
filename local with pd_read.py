import openpyxl
import os
import sqlalchemy
import schedule
import pyodbc
import pandas as pd
from pandas.io import sql
from datetime import datetime
import time
import xlrd
conn = pyodbc.connect('Driver={SQL Server};'
                  'Server=LAPTOP-CP199H1L\SQLEXPRESS10;'
                  'Database=britishairwaysdb;'
                  'Trusted_Connection=yes;')
cursor = conn.cursor()
conn1 = sqlalchemy.create_engine(f'mssql+pyodbc://LAPTOP-CP199H1L\SQLEXPRESS10/britishairwaysdb?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server')
court="'"

def data_upload():
    path = "C:/Users/BISWAJIT/OneDrive/Desktop/test7/"
    # log_folder_path='C:/Users/BISWAJIT/OneDrive/Desktop/log/'
    directoty_changer="C:/Users/BISWAJIT/OneDrive/Desktop/test123/"

    dir_list = os.listdir(path)
    for files in dir_list:
        file=path+files
        reading_status="FAILED"
        print("FILE CURRENT IN PROGRESS : "+files)
        # print(file.format)
        
        # file1=str(files).replace("_"," ").replace("-"," ")
        file1=files.split("_")
        file_name=file1[0]
        error_file_name=files
        legal_id=file1[1]
        user_id=file1[2]
        create_value=file_name+"_"+legal_id+"_"+user_id
        
        timestamp=datetime.now()
        now = str(timestamp).replace(".","").replace(":","-")
        # wb="ggh"
        # xlreader="fhgh"
        # with open (file,"r") as file_open:
        #     file_t=file_open
        try:
            
            # global wb ,xlreader
            

            # xlreader = pd.ExcelFile(file)
            # xlreader=xlrd.open_workbook(file)
            # # wb = openpyxl.load_workbook(filename=file, read_only=False)
            # reading_status="SUCESSS"
            # # no_of_sheets = xlreader.sheet_names
            # # print(no_of_sheets)
            sheet_no=-1
            # # for sheet_name in no_of_sheets:
            # for sheet_name in (xlreader.sheet_names()):
            xlreader=pd.read_excel(file,sheet_name=None)
            sheet_list=xlreader.keys()
            # print(sheet_list)
            for sheet in sheet_list:
                sheet_status="FAILED"
                error_description=[]
                # sheet_name=sheet_name.strip()
                # f_sheet_name=sheet_name.replace("'","")
                sheet=sheet.strip()
                f_sheet_name=sheet.replace("'","")
                sheet_no=sheet_no+1
                table_query="SELECT TARGET_TABLE FROM [LOOKUP].[LTMC_GENERAL_SHEET_INFO3] WHERE OBJECT_NAME ="+str(court+file_name+court)+" AND SHEET_NAME LIKE "+str(court+f_sheet_name+court)
                # print(file_name,sheet_name_ws)
                # print(table_query)
                cursor.execute(table_query)
                table=cursor.fetchall()
                conn.commit()
                # print(table[0])
                if str(table) !="[]":
                    target_table_l=list(table[0])
                    target_table= target_table_l[0]
                    if str(target_table) != "None":
                        # print(target_table)
                        sheet_status="SUCESSS"
                        # sheet_name=sheet_name.strip()


                        column_index_query="SELECT COLUMN_INDEX FROM [LOOKUP].[LTMC_GENERAL_SHEET_INFO3] WHERE OBJECT_NAME ="+str(court+file_name+court)+" AND SHEET_NAME = "+str(court+f_sheet_name+court)+" AND TARGET_TABLE = "+str(court+target_table+court)
                        cursor.execute(column_index_query)
                        column_index=cursor.fetchone()
                        column_index=column_index[0]
                        data_index_query="SELECT DATA_INDEX FROM [LOOKUP].[LTMC_GENERAL_SHEET_INFO3] WHERE OBJECT_NAME ="+str(court+file_name+court)+" AND SHEET_NAME = "+str(court+f_sheet_name+court)+" AND TARGET_TABLE = "+str(court+target_table+court)
                        cursor.execute(data_index_query)
                        data_index=cursor.fetchone()
                        data_index=data_index[0]
                        type_index_query="SELECT TYPE FROM [LOOKUP].[LTMC_GENERAL_SHEET_INFO3] WHERE OBJECT_NAME ="+str(court+file_name+court)+" AND SHEET_NAME = "+str(court+f_sheet_name+court)+" AND TARGET_TABLE = "+str(court+target_table+court)
                        cursor.execute(type_index_query)
                        type_index=cursor.fetchone()
                        type_index=type_index[0]
                        conn.commit()
                        # print("column_index "+str(column_index)+" data_index "+str(data_index))
                        print(sheet_no,sheet)
                        # print(sheet_name,type_index)
                        type=int(type_index)
                        


                        if type==1:



                            data = pd.read_excel(file,sheet_name=sheet,header=int(column_index)-1)
                            data = data.loc[:,~data.columns.str.contains('^Unnamed')]
                            distance_list=[]
                            try:
                                for i in range(0,data_index-int(column_index)-1):
                                    distance_list.append(i)
                                # print(distance_list,sheet_name)
                                data.drop(distance_list,axis=0,inplace=True)

                                target_table_list=target_table.split(".")
                                f_target_table=target_table_list[1].strip('[]')
                                # print(f_target_table)
                                data["CREATED_BY"]=create_value
                                data["CREATION_DATE"]=timestamp
                                # print(data)
                                try:
                                    data.to_sql(f_target_table,con=conn1,if_exists="append", schema="PRE_TGT",index=False)
                                except Exception as e:
                                    # error_description.append("COLUMN MISMATCH")
                                    # error_description.append(str(e.__cause__))
                                    # print("column_mismatch")
                                    sheet_status="FAILED"
                                    # print(e)
                                    try:
                                        error_l=str(e.__cause__).split("[SQL Server]")
                                        errorl1=error_l[1].split("(")
                                        f_error=str(errorl1[0])
                                        error_description.append(str(f_error))
                                        print(f_error)
                                    except:
                                        error_description.append(str(e.__cause__))

                            except Exception as ex:
                                error_description.append("columns should be at row "+column_index)
                                error_description.append("Data should be starting from row "+str(data_index))
                                # print(ex.__cause__)


                        if type==2:
                            try:
                                # print("type2")
                                # print(sheet_no,sheet_name)
                                data = pd.read_excel(file,sheet_name=sheet,header=int(column_index)-1)
                                data = data.loc[:,~data.columns.str.contains('^Unnamed')]
                                distance_list=[]
                                for i in range(0,data_index-int(column_index)-1):
                                    distance_list.append(i)
                                # print(distance_list,sheet_name)
                                data.drop(distance_list,axis=0,inplace=True)
                                # print(data)
                                data.drop(data.columns[0],axis=1,inplace=True)
                                data["CREATED_BY"]=create_value
                                data["CREATION_DATE"]=timestamp
                                # print(data)
                                target_table_list=target_table.split(".")
                                f_target_table=target_table_list[1].strip('[]')

                                try:
                                    data.to_sql(f_target_table,con=conn1,if_exists="append", schema="PRE_TGT",index=False)
                                except Exception as e:
                                    error_description.append(str(e.__cause__))

                                    # error_description.append("COLUMN MISMATCH")
                                    sheet_status="FAILED"
                                    # print("column_mismatch")
                                    print(e)
                            except Exception as ex:
                                error_description.append("columns should be at row "+column_index)
                                error_description.append("Data should be starting from row "+str(data_index))
                                sheet_status="FAILED"
                                # print(ex.__cause__)
                        if type==3:
                            
                            column_index=column_index.strip()[1:-1].replace("'","")
                            column_index_list=column_index.split(",")
                            # column_index=list(column_index)
                            # print(column_index_list)
                            # data = pd.read_excel(xlreader,sheet_no,names=['BANKS','BANKL','BANKA','PROVZ','STRAS','ORT01','BRNCH','SWIFT','XPGRO','BNKLZ','BGRUP'])
                            try: 
                                data = pd.read_excel(file,sheet_name=sheet,names=column_index_list)
                                # print(data)
                            
                                distance_list=[]
                                for i in range(0,data_index-2):
                                    distance_list.append(i)
                                # print(distance_list)
                                # print(sheet_name)
                                # print(data)
                                try:
                                    data.drop(distance_list,axis=0,inplace=True)
                                except  Exception as fg:
                                    print(str(fg.__cause__))
                                # print(data)
                                target_table_list=target_table.split(".")
                                f_target_table=target_table_list[1].strip('[]')
                                data["CREATED_BY"]=create_value
                                data["CREATION_DATE"]=datetime.now()
                                # print(data)
                                try:
                                    # print(data)
                                    data.to_sql(f_target_table,con=conn1,if_exists="append", schema="PRE_TGT",index=False)
                                except Exception as e:
                                    error_description.append(str(e.__cause__))
                                    # print("hello"+e)
                                    # error_description.append("Value Error")
                                    sheet_status="FAILED"
                            except Exception as ex:
                                # print("hello",ex)
                                error_description.append("Either Number of Columns Mismatch OR Data should be starting from row "+str(data_index))
                                sheet_status="FAILED"
                        if type==4:
                            try:

                                # print(file_name,sheet_name)
                                column_index=column_index.strip()[1:-1].replace("'","")
                                column_index_list=column_index.split(",")
                                data = pd.read_excel(file,sheet_name=sheet,names=column_index_list)
                                distance_list=[]
                                for i in range(0,data_index-2):
                                    distance_list.append(i)
                                # print(column_index_list)
                                data.drop(distance_list,axis=0,inplace=True)
                                data.drop(data.columns[0],axis=1,inplace=True)
                                target_table_list=target_table.split(".")
                                f_target_table=target_table_list[1].strip('[]')
                                data["CREATED_BY"]=create_value
                                data["CREATION_DATE"]=datetime.now()
                                # print(data)
                                try:
                                    data.to_sql(f_target_table,con=conn1,if_exists="append", schema="PRE_TGT",index=False)
                                except Exception as e:
                                    print(e)
                                    # error_description.append("COLUMN_NUMBER_MISMATCH")

                                    error_description.append(str(e.__cause__))
                                    sheet_status="FAILED"
                            except Exception as ex:
                                # print(ex.__cause__)
                                error_description.append("Data should be starting from row "+str(data_index))
                                sheet_status="FAILED"
                                pass




                else:
                    error_description.append("NO table Found to Insert")
                    # print(file_name+sheet_name+"table not found")
                # error_description="IF ANY COLUMN NOT INSERTED THEN PLEASE CHECK COLUMN NAME AS TABLE COLUMN_NAME"

                # print(error_file_name)
                df12 = pd.DataFrame()
                df12['FILE_NAME']=[error_file_name]
                df12['SHEET_NAME']=[str(f_sheet_name)]
                df12['LEGAL_ENTITY']=[legal_id]
                df12['OBJECT_ID']=[file_name]
                df12['UPLOAD_DATE']=[str(timestamp)]
                df12['STATUS']=[reading_status]
                df12['UPLOADED_BY']=[user_id]
                df12['SHEET_STATUS']=[sheet_status]
                if str(error_description)=="[]":
                    df12['DESCRIPTION']=("")
                else:
                    df12['DESCRIPTION']=(str(error_description))
                
                df12.to_sql('STSNDERED_LTMC_UPLOAD_STATUS',con=conn1,if_exists="append", schema="PRE_TGT",index=False)
                # print("success for "+sheet_name)

                

           

                # error_value_list=[]
                # error_value_list.append(error_file_name)
                # error_value_list.append(str(f_sheet_name))
                # error_value_list.append(legal_id)
                # error_value_list.append(file_name)
                # error_value_list.append(str(timestamp))
                # error_value_list.append(reading_status)
                # error_value_list.append(user_id)
                # error_value_list.append(sheet_status)
                # if str(error_description)=="[]":
                #     error_value_list.append("")
                # else:
                #     error_value_list.append(error_description)
                # # error_value_list.append(error_description)
                # print(error_value_list)
                # error_value_list=str(error_value_list).replace("[","(")
                # error_value_list=error_value_list.replace("]",")")
                # error_table_query="INSERT INTO [PRE_TGT].[STSNDERED_LTMC_UPLOAD_STATUS] ([FILE_NAME],[SHEET_NAME],[LEGAL_ENTITY],[OBJECT_ID],[UPLOAD_DATE],[STATUS],[UPLOADED_BY],[SHEET_STATUS],[DESCRIPTION]) VALUES"+error_value_list
                # # print(error_table_query) 
                # cursor.execute(error_table_query)  


                # print("data start-----------------------------------------------------------")
                # # conn.commit() 
                # print(df12)
                # print("data stop-----------------------------------------------------------")
            
            
            # xlreader.close()
            # xlreader
            
            # print(directoty_changer+change_dic_file_name_go)
            # wb.save(directoty_changer+change_dic_file_name_go)
            # wb.close()
            # os.remove(file)
            change_dic_file_name_go=now+"_"+files
            os.replace(file, directoty_changer+change_dic_file_name_go)
        
        except Exception as ex:
            print(ex)
            # wb.close()
            # xlreader.close()
            print(file)
            
            sts="FAILED"
            error_description="FAILED TO READ PLEASE CHECK EXCEL FILE"
            error_table_query2="INSERT INTO [PRE_TGT].[STSNDERED_LTMC_UPLOAD_STATUS] ([FILE_NAME],[UPLOADED_BY],[LEGAL_ENTITY],[UPLOAD_DATE],[STATUS],[DESCRIPTION]) VALUES ("+court+error_file_name+court+","+court+create_value+court+","+court+legal_id+court+","+court+str(timestamp)+court+","+court+sts+court+","+court+error_description+court+")"
            cursor.execute(error_table_query2)  
            conn.commit()
            change_dic_file_name_go=now+"_"+files

            os.replace(file, directoty_changer+change_dic_file_name_go)
            
            # os.remove(file)
    
    print("done")





                    

        # data = pd.read_excel(xlreader,5,header=4)

schedule.every(5).seconds.do(data_upload)

while True:
	schedule.run_pending()
	time.sleep(1)

# [ProgrammingError('42S22', "[42S22] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Invalid column name 'LEBRE'.
#   (207) (SQLExecDirectW); [42S22] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Statement(s) could not be prepared. (8180)")]

#   [ProgrammingError('42000', "[42000] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]String or binary data would be truncated in table 
#   'britishairwaysdb.PRE_TGT.PROCUREMENT_MM_MD03_BANK_DETAILS', column 'BKVID'. Truncated value: 'Bank'. (2628) (SQLExecDirectW);
#   [42000] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]The statement has been terminated. (3621)")]