from difflib import SequenceMatcher
import pandas as pd
import pyodbc
import difflib
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import os
from functools import partial
import datetime
import traceback
from sqlqueries import client_id_ssn, payor_id, cc_info, previous_service_info, next_service_info, loc_info, status_info, two_payor_info


# gets the similarity ratio between the two clientname columns
def apply_sm(merged, c1, c2):
    return difflib.SequenceMatcher(None, merged[c1], merged[c2]).ratio()


pd.set_option('display.float_format', lambda x: '%.3f' % x)  # stops phone numbers from appearing in scientific notation
os.chdir(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\External Data Repository\HL Altruitsa Pulls\Raw files')
now = datetime.datetime.now()
pyfilename = 'HLAttrMap.py'
year_month = now.strftime("%Y-%m")

hold_file_dir_list = os.listdir(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\HealthLinkAttribution\project\Hold Files')

hold_file_ind = len([s for s in hold_file_dir_list if year_month in s])

try:
    if hold_file_ind == 0:
        conn_str = (
            r'Driver={SQL Server};'
            r'Server=cstnismdb27.centerstone.lan;'
            r'Database=ndw3nfdb;'
            r'Trusted_Connection=yes;'
        )
        
        cnxn = pyodbc.connect(conn_str, autocommit=True)
        
        dir_list = os.listdir(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\External Data Repository\HL Altruitsa Pulls\Raw files')
        oslist = [s for s in dir_list if now.strftime("%Y-%m-%d") in s]
        
        attr = pd.read_excel(oslist[0])
        attr['CLIENT NAME'] = attr['LAST_NAME'] + ', ' + attr['FIRST_NAME']
        attr['SSN'] = attr['SSN'].apply(str)
        
        client_id_ssn = pd.DataFrame(pd.read_sql(client_id_ssn(), cnxn))
        
        merged = pd.merge(attr, client_id_ssn, on='SSN', how='left')
        merged = merged.fillna('')
        
        # create column that shows the similarity ratio between the two name columns.  (calling defined apply_sm function from above)
        merged['NameMatchRatio'] = merged.apply(partial(apply_sm, c1='CLIENT NAME', c2='ClientName'), axis=1)
        
        merged['PHONE NUMBER'] = merged['PHONE NUMBER'].apply(lambda x: str(x)[:10])
        merged['PHONE NUMBER'].replace(to_replace='nan', value='', inplace=True)
        
        merged.drop(['LAST_NAME', 'FIRST_NAME',
                     'INSURANCE ID', 'LAST_CLAIM', 'LAST_VISIT_DATE',
                     'NEXT_VISIT_DATE', 'ER_VISITS', 'APP_VISITS',
                     'ADTDAYS_COUNT', 'DUE_DAYS', 'ClientName',
                     'ASSIGNED DATE/ATTRIBUTED DATE', 'RISK_SCORE'], axis=1,
                    inplace=True)
        
        payor_id_data = pd.DataFrame(pd.read_sql(payor_id(), cnxn))
        main_data = pd.merge(merged, payor_id_data, on='Client_ID', how='left')
        
        cc_info_data = pd.DataFrame(pd.read_sql(cc_info(), cnxn))
        main_data = pd.merge(main_data, cc_info_data, on='Client_ID', how='left')
        
        previous_service_data = pd.DataFrame(pd.read_sql(previous_service_info(), cnxn))
        main_data = pd.merge(main_data, previous_service_data, on='Client_ID', how='left')
        
        next_service_data = pd.DataFrame(pd.read_sql(next_service_info(), cnxn))
        main_data = pd.merge(main_data, next_service_data, on='Client_ID', how='left')
        
        loc_data = pd.DataFrame(pd.read_sql(loc_info(), cnxn))
        main_data = pd.merge(main_data, loc_data, on='Client_ID', how='left')
        
        status_data = pd.DataFrame(pd.read_sql(status_info(), cnxn))
        main_data = pd.merge(main_data, status_data, on='Client_ID', how='left')
        
        # print('Data processing is finishing...')
        
        main_data = main_data.fillna('')
        main_data['Payor_ID_Number'] = main_data['Payor_ID_Number'].str.strip('ZEC|D')  # Strip ZEC or ZED from beginning of payor_id.  ( at the request of Mandi)
        main_data.loc[main_data['Payor_ID_Number'].str.startswith('M'), 'Payor_ID_Number'] += '01'
        main_data['RunDate'] = now.strftime("%Y-%m-%d")
        
        main_data = main_data[['RunDate', 'HEALTH PLAN', 'Payor_ID_Number',  'Altruista ID', 'Client_ID', 'SSN',
                               'PATIENT_DOB', 'CLIENT NAME', 'ADDRESS', 'PHONE NUMBER', 'PCP_NAME', 'MemberStatus',
                               'THL_STATUS', 'RISK_CATEGORY_NAME', 'CC_Name', 'CCLocation', 'LastServiceDate', 'LastServiceLocation',
                               'LastServiceActivityCode', 'LastServiceActivity', 'NextServiceDate', 'NextServiceLocation',
                               'NextServiceActivityCode', 'NextServiceActivity', 'NameMatchRatio', 'HLink_LOC', 'PROGRAM_NAMES']]
        
        # creates a dataframe of only matched clients.  this has no use except to calculate the number of matched vs unmatched attributed clients
        match = main_data[(main_data['Client_ID'] != '') & (main_data['NameMatchRatio'] >= 0.70)]
        
        main_data.drop(['NameMatchRatio'], axis=1, inplace=True)
        
        os.chdir(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\HealthLinkAttribution\project\Output\Mapped Attributed Lists')
        main_data.to_csv('HLAltruistaAttrClientsMapped.txt', sep=";", index=False)
        os.chdir(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\HealthLinkAttribution\project\Output\Mapped Attributed Lists\txt Archive')
        main_data.to_csv('HLAltruistaAttrClientsMapped'+ ' ' + now.strftime("%Y-%m-%d") + '.txt', sep=";", index=False)
        os.chdir(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\HealthLinkAttribution\project\Output\Mapped Attributed Lists\Archive')
        main_data.to_csv('HLAltruistaAttrClientsMapped' + ' ' + now.strftime("%Y-%m-%d") + '.csv', index=False)
        
        ########################################################################## Creation of Extra dataframe output requested by Mandi.
        ########################################################################## Checks to see if the clients with THL status of inactive have another payor_id & exports to csv
        
        no_bh = main_data[main_data['THL_STATUS'] == ' Inactive No BH Treatment']
        
        two_payor_data = pd.DataFrame(pd.read_sql(two_payor_info(), cnxn))
        no_bh = pd.merge(no_bh, two_payor_data, on='Client_ID', how='left')
        
        no_bh.drop(
            ['HEALTH PLAN', 'Payor_ID_Number', 'SSN', 'PATIENT_DOB', 'ADDRESS', 'PHONE NUMBER', 'PCP_NAME', 'MemberStatus',
             'CC_Name', 'CCLocation', 'LastServiceDate', 'LastServiceLocation', 'Altruista ID',
             'LastServiceActivityCode', 'LastServiceActivity', 'NextServiceDate', 'NextServiceLocation',
             'NextServiceActivityCode', 'NextServiceActivity', 'RISK_CATEGORY_NAME', 'PROGRAM_NAMES'], axis=1, inplace=True)
        
        no_bh = no_bh[['RunDate', 'Client_ID', 'CLIENT NAME', 'THL_STATUS', 'MoreThanOnePayorInd', 'HLink_LOC']]
        
        os.chdir(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\HealthLinkAttribution\project\Output\THL Inactive w Two Or More Payors')
        no_bh.to_csv('THLInactiveWithTwoOrMorePayors' + ' ' + now.strftime("%Y-%m-%d") + '.csv', sep=',', index=False)
        
        
        msg = MIMEMultipart()
        sender='analytics@centerstone.org'
        recipients='mandi.ryan@centerstone.org,angus.gray@centerstone.org'
        server=smtplib.SMTP('csmail.centerstone.lan')
        
        msg['Subject']='Altruista HL Monthly Attribution List'
        msg['From']=sender
        msg['To']=recipients
        message = 'no match: {no_match} \nmatch: {match}'.format(no_match=str(int(main_data.shape[0]) - int(match.shape[0])), match=str(match.shape[0]))
        
        msg.attach(MIMEText(message))
        filename1 = 'HLAltruistaAttrClientsMapped' + ' ' + now.strftime("%Y-%m-%d") + '.csv'
        filename2 = 'THLInactiveWithTwoOrMorePayors' + ' ' + now.strftime("%Y-%m-%d") + '.csv'
        attachment1 = open(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\HealthLinkAttribution\project\Output\Mapped Attributed Lists\Archive' + '\\' + filename1, 'rb')
        attachment2 = open(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\HealthLinkAttribution\project\Output\THL Inactive w Two Or More Payors' + '\\' + filename2, 'rb')
        csv1 = MIMEBase('application','octet-stream')
        csv2 = MIMEBase('application','octet-stream')
        csv1.set_payload(attachment1.read())
        csv2.set_payload(attachment2.read())
        encoders.encode_base64(csv1)
        encoders.encode_base64(csv2)
        csv1.add_header('Content-Disposition', 'attachment', filename=filename1)
        csv2.add_header('Content-Disposition', 'attachment', filename=filename2)
        msg.attach(csv1)
        msg.attach(csv2)
        server.send_message(msg)
        server.quit()
        attachment1.close()
        attachment2.close()
        
        cnxn.close()
        
        os.chdir(r'\\centerstone.lan\ent\GroupDrive\Angus_Shared\Python Projects\HealthLinkAttribution\project\Hold Files')
        f = open('AltruistaAttrHoldFile ' + year_month +'.txt', 'w')
        f.close()
    else:
        pass
except Exception as e:
    s = smtplib.SMTP('csmail.centerstone.lan')
    msg = MIMEText("There was an error while executing " + pyfilename + " on " + now.strftime("%Y-%m-%d") + ".\n\n" + str(traceback.format_exc()))
    sender = 'analytics@centerstone.org'
    recipients = ['angus.gray@centerstone.org', 'claudius.ibine@centerstone.org']
    msg['Subject'] = "RDS01 PYTHON FAILURE"
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)
    s.sendmail(sender, recipients, msg.as_string())