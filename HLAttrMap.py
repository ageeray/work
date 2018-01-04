from difflib import SequenceMatcher
import pandas as pd
import pyodbc
import difflib
import os
from functools import partial
import datetime
from sqlqueries import client_id_ssn, payor_id, cc_info, previous_service_info, next_service_info, loc_info, status_info, two_payor_info


# gets the similarity ratio between the two clientname columns
def apply_sm(merged, c1, c2):
    return difflib.SequenceMatcher(None, merged[c1], merged[c2]).ratio()


print('Data has started processing.  Allow a bit of time for this to complete.')
pd.set_option('display.float_format', lambda x: '%.3f' % x)  # stops phone numbers from appearing in scientific notation
os.chdir(r'H:\HL Altruitsa Pulls\Raw files')
now = datetime.datetime.now()

conn_str = (
    r'Driver={SQL Server};'
    r'Server=cstnismdb27.centerstone.lan;'
    r'Database=ndw3nfdb;'
    r'Trusted_Connection=yes;'
)
cnxn = pyodbc.connect(conn_str)

attr = pd.read_excel('AltruistaMemberAssignment20180104.xlsx')
attr['CLIENT NAME'] = attr['LAST_NAME'] + ', ' + attr['FIRST_NAME']
attr['SSN'] = attr['SSN'].apply(str)

client_id_ssn = pd.DataFrame(pd.read_sql(client_id_ssn(), cnxn))

merged = pd.merge(attr, client_id_ssn, on='SSN', how='left')
merged = merged.fillna('')

# create column that shows the similarity ratio between the two name columns.  (calling defined apply_sm function from above)
merged['NameMatchRatio'] = merged.apply(partial(apply_sm, c1='CLIENT NAME', c2='ClientName'), axis=1)

merged['PHONE NUMBER'] = merged['PHONE NUMBER'].apply(lambda x: str(x)[:10])
merged['PHONE NUMBER'].replace(to_replace='nan', value='', inplace=True)

merged.drop(['LAST_NAME', 'FIRST_NAME', 'Altruista ID',
             'INSURANCE ID', 'LAST_CLAIM', 'LAST_VISIT_DATE',
             'NEXT_VISIT_DATE', 'ER_VISITS', 'APP_VISITS',
             'ADTDAYS_COUNT', 'DUE_DAYS', 'ClientName',
             'ASSIGNED DATE/ATTRIBUTED DATE', 'PROGRAM_NAMES', 'RISK_CATEGORY_NAME', 'RISK_SCORE'], axis=1,
            inplace=True)

payor_id_data = pd.DataFrame(pd.read_sql(payor_id(), cnxn))
main_data = pd.merge(merged, payor_id_data, on='Client_ID', how='left')

cc_info_data = pd.DataFrame(pd.read_sql(cc_info(), cnxn))
main_data = pd.merge(main_data, cc_info_data, on='Client_ID', how='left')

print('Data still processing...')
previous_service_data = pd.DataFrame(pd.read_sql(previous_service_info(), cnxn))
main_data = pd.merge(main_data, previous_service_data, on='Client_ID', how='left')

next_service_data = pd.DataFrame(pd.read_sql(next_service_info(), cnxn))
main_data = pd.merge(main_data, next_service_data, on='Client_ID', how='left')

loc_data = pd.DataFrame(pd.read_sql(loc_info(), cnxn))
main_data = pd.merge(main_data, loc_data, on='Client_ID', how='left')

status_data = pd.DataFrame(pd.read_sql(status_info(), cnxn))
main_data = pd.merge(main_data, status_data, on='Client_ID', how='left')

print('Data processing is finishing...')

main_data = main_data.fillna('')
main_data['Payor_ID_Number'] = main_data['Payor_ID_Number'].str.strip('ZEC|D')  # Strip ZEC or ZED from beginning of payor_id.  ( at the request of Mandi)
main_data.loc[main_data['Payor_ID_Number'].str.startswith('M'), 'Payor_ID_Number'] += '01'
main_data['RunDate'] = now.strftime("%Y-%m-%d")

main_data = main_data[['RunDate', 'HEALTH PLAN', 'Payor_ID_Number', 'Client_ID', 'SSN',
                       'PATIENT_DOB', 'CLIENT NAME', 'ADDRESS', 'PHONE NUMBER', 'PCP_NAME', 'MemberStatus',
                       'THL_STATUS', 'CC_Name', 'CCLocation', 'LastServiceDate', 'LastServiceLocation',
                       'LastServiceActivityCode', 'LastServiceActivity', 'NextServiceDate', 'NextServiceLocation',
                       'NextServiceActivityCode', 'NextServiceActivity', 'NameMatchRatio', 'HLink_LOC']]

# creates a dataframe of only matched clients.  this has no use except to calculate the number of matched vs unmatched attributed clients
match = main_data[(main_data['Client_ID'] != '') & (main_data['NameMatchRatio'] >= 0.70)]

main_data.drop(['NameMatchRatio'], axis=1, inplace=True)

os.chdir(r'H:\HL Altruitsa Pulls\Mapped Attributed Lists')  # change directory to where file belongs.
main_data.to_csv('HLAltruistaAttrClientsMapped' + ' ' + now.strftime("%Y-%m-%d") + '.csv', index=False)

########################################################################## Creation of Extra dataframe output requested by Mandi.
########################################################################## Checks to see if the clients with THL status of inactive have another payor_id & exports to csv

no_bh = main_data[main_data['THL_STATUS'] == ' Inactive No BH Treatment']

two_payor_data = pd.DataFrame(pd.read_sql(two_payor_info(), cnxn))
no_bh = pd.merge(no_bh, two_payor_data, on='Client_ID', how='left')

no_bh.drop(
    ['HEALTH PLAN', 'Payor_ID_Number', 'SSN', 'PATIENT_DOB', 'ADDRESS', 'PHONE NUMBER', 'PCP_NAME', 'MemberStatus',
     'CC_Name', 'CCLocation', 'LastServiceDate', 'LastServiceLocation',
     'LastServiceActivityCode', 'LastServiceActivity', 'NextServiceDate', 'NextServiceLocation',
     'NextServiceActivityCode', 'NextServiceActivity'], axis=1, inplace=True)

no_bh = no_bh[['RunDate', 'Client_ID', 'CLIENT NAME', 'THL_STATUS', 'MoreThanOnePayorInd', 'HLink_LOC']]

os.chdir(r'H:\HL Altruitsa Pulls\THL Inactive w Two Or More Payors')
no_bh.to_csv('THLInactiveWithTwoOrMorePayors' + ' ' + now.strftime("%Y-%m-%d") + '.csv', index=False)

print('Data processing finished.')
print('no match: ' + str(int(main_data.shape[0]) - int(match.shape[0])))  # prints the number of clients that did not get associated with a Payor_ID_Number
print('match: ' + str(match.shape[0]))  # prints the number of clients that DID get associated with a Payor_ID_Number
