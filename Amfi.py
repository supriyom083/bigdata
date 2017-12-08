import requests
import os
import calendar
import datetime  

for j in range(100):
    url = "http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf="+str(j)+"&tp=1&frmdt=01-Jan-1999&todt=7-Dec-2017"
    r = requests.get(url)
    data=r.text
    handel=open(str(j)+".txt","w",encoding='utf8')
    handel.write(data)
    file_name=handel.name
    handel.close()      
    import sqlite3 as sql
    import logging
    import  pyparsing as pp
    import glob
    num_field=pp.Word(pp.nums+".")
    logger = logging.getLogger('ftpuploader')
    conn=sql.connect("nav.db")
    cur=conn.cursor()
    data=[]
    file_write=open('process.txt','w+')
    
    with open(str(j)+".txt",'r') as file_handel:
        for _ in range(8):
            next(file_handel)
        for j in file_handel.readlines():
            data=j.rstrip().split(';')
            if len(data)==6 and data[0]!='':
                try:
                    k=num_field.parseString(data[2])
                    data_2=k[0]
                except:
                    data_2=str('0.0')
                try:
                    k=num_field.parseString(data[3])
                    data_3=k[0]
                except:
                    data_3=str('0.0')
                try:
                    k=num_field.parseString(data[3])
                    data_4=k[0]
                except:
                    data_3=str('0.0')                
                try:
                    Day_name=calendar.day_name[datetime.datetime.strptime(data[5] , '%d-%b-%Y').weekday()]
                    #print(Day_name)
                    sql_ins="insert into scheme_nav_setails(Scheme_Code,Scheme_Name,\
                                                   Repurchase_Price,Net_Asset_Value,Sale_Price,record_Date,Day_name) \
                    values("+data[0]+",'"+str(data[1].replace("'",""))+"',"+data_2+","+data_3+","+data_4+",'"+data[5]+"','"+Day_name+"');"
                    #print(sql_ins)
                    cur.execute(sql_ins)                
                except Exception as e:
                    file_write.write(sql_ins)
                    print(data)
                    print(sql_ins)
                    logger.exception('Failed: ' + str(e))  
                   
    conn.commit()
    cur.execute("VACUUM scheme_nav_setails")
    conn.close()
    file_write.close()
    #file_name=str(j)+".txt"
    print(file_name)
    os.remove(file_name)         
    
    
        
