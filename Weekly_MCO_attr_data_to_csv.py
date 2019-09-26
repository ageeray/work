import pandas as pd
import pyodbc
import os
import re
from datetime import datetime
import time
import os.path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import csv
import traceback


def get_most_recent_modified_file(directory):
    cwd = os.getcwd()
    os.chdir(directory)
    dir_list = os.listdir(directory)
    dir_list.sort(key=os.path.getmtime)
    os.chdir(cwd)
    return dir_list[-1]

now = datetime.now()
pyfilename = 'Weekly_MCO_attr_ata_to_csv.py'
get_date = now.strftime("%Y-%m-%d")


try:
    os.chdir(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\External Data Repository\MCO Attribution Files\BCBS')
    lastmodifiedBCBSfile = get_most_recent_modified_file(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\External Data Repository\MCO Attribution Files\BCBS')
    BCBSmdate = datetime.strptime(time.ctime(os.path.getmtime(lastmodifiedBCBSfile)), '%a %b %d %H:%M:%S %Y')
    BCBSdatediff = now - BCBSmdate 

    if BCBSdatediff.days < 1:
        
        bcbsdata = pd.read_excel(lastmodifiedBCBSfile, skiprows=[0])
        bcbsdata['LastModifiedDate'] = get_date
        #bcbsdata['Address'] = bcbsdata['Address'].str.replace(r' +', '')
        bcbsdata['Address'] = bcbsdata['Address'].str.replace(r'\n', '')
    
        os.chdir(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\WeeklyMCOHLAttrStatusUpdate\Output')
        bcbsdata.to_csv('BCBSWeeklyAttrList.txt', index = False, sep='\t')
        
    else:
        pass
    
    
    os.chdir(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\External Data Repository\MCO Attribution Files\UHC')
    lastmodifiedUHCfile = get_most_recent_modified_file(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\External Data Repository\MCO Attribution Files\UHC')
    UHCmdate = datetime.strptime(time.ctime(os.path.getmtime(lastmodifiedUHCfile)), '%a %b %d %H:%M:%S %Y')
    UHCdatediff = now- UHCmdate 
    
    if UHCdatediff.days < 1:
        
        uhcdata = pd.read_excel(lastmodifiedUHCfile, skiprows=range(0,6))
        uhcdata['LastModifiedDate'] = get_date
    
        os.chdir(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\WeeklyMCOHLAttrStatusUpdate\Output')
        uhcdata.to_csv('UHCWeeklyAttrList.txt', index = False, sep='\t')
    
    else:
        pass
    
    
    os.chdir(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\External Data Repository\MCO Attribution Files\Amerigroup')
    lastmodifiedAmerigroupfile = get_most_recent_modified_file(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\External Data Repository\MCO Attribution Files\Amerigroup')
    Amerigroupmdate = datetime.strptime(time.ctime(os.path.getmtime(lastmodifiedAmerigroupfile)), '%a %b %d %H:%M:%S %Y')
    Amerigroupdatediff = now - Amerigroupmdate 
    
    if Amerigroupdatediff.days < 1:
        
        Amerigroupdata = pd.read_excel(lastmodifiedAmerigroupfile, skiprows=range(0,6))
        Amerigroupdata['LastModifiedDate'] = get_date
        
        os.chdir(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\WeeklyMCOHLAttrStatusUpdate\Output')
        Amerigroupdata.to_csv('AmerigroupWeeklyAttrList.txt', index = False, sep='\t')
    
    else:
        pass
    
    conn_str = (
    r'Driver={SQL Server};'
    r'Server=cstnismdb27.centerstone.lan;'
    r'Database=ndw3nfdb;'
    r'Trusted_Connection=yes;'
    #r'MARS_Connection=yes;'
    )
    
    cnxn = pyodbc.connect(conn_str, autocommit=True)
    
    os.chdir(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\WeeklyMCOHLAttrStatusUpdate\Output')
    
    query = "EXEC ('dbo.usp_HealthLinkWeeklyAttributionListsImport')"
    cnxn.execute(query)
        
    try:
        query = "EXEC ('dbo.usp_HealthLinkAttributionListsMapMail')"
        output = pd.read_sql_query(query, cnxn)
        output.to_csv('MCOHLClientStatusUpdate.csv', index = False, sep=',', quoting=csv.QUOTE_ALL)
        
        msg = MIMEMultipart()
        sender='analytics@centerstone.org'
        #,mandi.ryan@centerstone.org
        recipients='angus.gray@centerstone.org,mandi.ryan@centerstone.org,alicia.allen@centerstone.org'
        server=smtplib.SMTP('csmail.centerstone.lan')
        
        msg['Subject']='MCO Health Link Client Status Update'
        msg['From']=sender
        msg['To']=recipients
        
        filename = r'MCOHLClientStatusUpdate.csv'
        attachment = open(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\WeeklyMCOHLAttrStatusUpdate\Output\MCOHLClientStatusUpdate.csv', 'rb')
        csv = MIMEBase('application','octet-stream')
        csv.set_payload(attachment.read())
        encoders.encode_base64(csv)
        csv.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(csv)
        server.send_message(msg)
        server.quit()
        attachment.close()
    
    except Exception as e:
        pass
    #, 'claudius.ibine@centerstone.org'
except Exception as e:
    s = smtplib.SMTP('csmail.centerstone.lan')
    msg = MIMEText("There was an error while executing " + pyfilename + " on " + now.strftime("%Y-%m-%d")  + ".\n\n" + str(traceback.format_exc()))
    sender = 'analytics@centerstone.org'
    recipients = ['angus.gray@centerstone.org', 'claudius.ibine@centerstone.org']
    #recipients = ['angus.gray@centerstone.org']
    msg['Subject'] = "RDS01 PYTHON FAILURE"
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)
    s.sendmail(sender, recipients, msg.as_string())