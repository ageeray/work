def client_id_ssn():
    return '''
SELECT DISTINCT
        X.Client_ID
       ,X.SSN
       ,X.ClientName
FROM    ( SELECT    n.SourceClient_ID AS Client_ID
                   ,ssn.SSN
                   ,n.LastName + ', ' + n.FirstName AS ClientName
                   ,MAX(s.ServiceDate) AS maxservicedate
                   ,ROW_NUMBER() OVER ( PARTITION BY ssn.SSN ORDER BY s.ServiceDate DESC ) AS rnum3
          FROM      limiteddb.dbo.Client_SSN AS ssn WITH ( NOLOCK )
                    INNER JOIN limiteddb.dbo.ClientName AS n WITH ( NOLOCK ) ON n.Client_ID = ssn.Client_ID
                                                              AND n.ORG_ID = 1
                    LEFT JOIN ndw3nfdb.dbo.Service AS s WITH ( NOLOCK ) ON s.Client_ID = ssn.Client_ID
          WHERE     s.ServiceDate <= GETDATE()
          GROUP BY  n.SourceClient_ID
                   ,ssn.SSN
                   ,s.ServiceDate
                   ,n.LastName + ', ' + n.FirstName
        ) X
WHERE   rnum3 = 1;
'''


def payor_id():
    return '''
SELECT  X.Client_ID
       ,X.Payor_ID_Number
FROM    ( SELECT DISTINCT
                    c.SourceClient_ID AS Client_ID
                   ,cp.Payor_ID_Number
                   ,MAX(cp.BeginDate) AS maxbegindate
                   ,ROW_NUMBER() OVER ( PARTITION BY c.SourceClient_ID ORDER BY cp.PayorCode_ID DESC ) AS rnum
          FROM      ndw3nfdb.dbo.ClientPayor AS cp WITH ( NOLOCK )
                    INNER JOIN ndw3nfdb.dbo.PayorCode AS pc WITH ( NOLOCK ) ON pc.PayorCode_ID = cp.PayorCode_ID
                    LEFT JOIN ndw3nfdb.dbo.PayorGroup AS pg WITH ( NOLOCK ) ON pg.PayorGroup_ID = pc.Payor_GRP_ID
                    INNER JOIN ndw3nfdb.dbo.Client AS c WITH ( NOLOCK ) ON c.Client_ID = cp.Client_ID
          WHERE     cp.EndDate IS NULL
                    AND pg.Tenncare = 1
                    AND cp.ORG_ID = 1
          GROUP BY  c.SourceClient_ID
                   ,cp.Payor_ID_Number
                   ,pc.PayorName
                   ,cp.PayorCode_ID
        ) X
WHERE   X.rnum = 1
        AND X.Payor_ID_Number IS NOT NULL;
'''


def cc_info():
    return '''
SELECT  X1.SourceClient_ID AS Client_ID
        ,X1.CC_Name
        ,X1.LOC_Name AS CCLocation
FROM    ( -- X1
            SELECT    C.SourceClient_ID
                    ,DATEPART(YYYY , CS.BeginDate) Year
                    ,DATEPART(MM , CS.BeginDate) Month
                    ,CS.LOC_ID
                    ,CS.Staff_ID
                    ,QST.SourceStaff_ID
                    ,QST.EMP_ID
                    ,QST.FirstName + ' ' + QST.LastName AS CC_Name
                    ,l.LOC_Name
                    ,CC_RANK_DESC = ROW_NUMBER() OVER ( PARTITION BY C.Client_ID ORDER BY CS.PrimaryRecord DESC, CS.ClientStaff_ID DESC, CS.BeginDate DESC, CS.EndDate DESC )
            FROM      ndw3nfdb.dbo.Client C
                    INNER JOIN ndw3nfdb.dbo.ClientStaff CS WITH ( NOLOCK ) ON C.Client_ID = CS.Client_ID
                    INNER JOIN ndw3nfdb.dbo.QV_Staff QST WITH ( NOLOCK ) ON QST.Staff_ID = CS.Staff_ID
                    LEFT JOIN ndw3nfdb.dbo.QV_StaffHistory QSH WITH ( NOLOCK ) ON QST.Staff_ID = QSH.Staff_ID
                                                        AND CAST(QSH.BeginDate AS DATE) <= CAST(GETDATE() AS DATE)
                                                        AND ISNULL(CAST(QSH.EndDate AS DATE) ,
                                                        CAST(GETDATE() AS DATE)) >= CAST(GETDATE() AS DATE)
                    INNER JOIN ndw3nfdb.dbo.JobTitle JT WITH ( NOLOCK ) ON QST.JobTitle_ID = JT.JobTitle_ID
                    INNER JOIN ndw3nfdb.dbo.Location AS l ON l.LOC_ID = QST.LOC_ID
            WHERE     CAST(CS.BeginDate AS DATE) <= CAST(GETDATE() AS DATE)
                    AND ISNULL(CAST(CS.EndDate AS DATE) ,
                                CAST(GETDATE() AS DATE)) >= CAST(GETDATE() AS DATE)
                    AND CS.PrimaryRecord = 1
                    AND C.ORG_ID = 1
        ) X1
WHERE   CC_RANK_DESC = 1;
'''


def previous_service_info():
    return '''
SELECT  X.Client_ID
       ,X.LastServiceDate
       ,X.LastServiceLocation
       ,X.LastServiceActivityCode
       ,X.LastServiceActivity
FROM    ( SELECT    c.SourceClient_ID AS Client_ID
                   ,s.ServiceDate AS LastServiceDate
                   ,l.LOC_Name AS LastServiceLocation
                   ,a.ActivityCode AS LastServiceActivityCode
                   ,a.Activity AS LastServiceActivity
                   ,ROW_NUMBER() OVER ( PARTITION BY c.SourceClient_ID ORDER BY s.servicedate DESC ) AS rnum1
          FROM      Service AS s
                    LEFT JOIN Location AS l WITH ( NOLOCK ) ON l.LOC_ID = s.LOC_ID
                    LEFT JOIN dbo.Activity AS a WITH ( NOLOCK ) ON a.Activity_ID = s.Activity_ID
                    INNER JOIN Client AS c WITH ( NOLOCK ) ON c.Client_ID = s.Client_ID
          WHERE     s.ServiceDate < GETDATE()
                    AND s.ORG_ID = 1
                    AND s.ServiceStatus_ID = 14
                    AND s.DeletedFlag = 0
                    AND a.ActivityCode != 'MEMO'
        ) X
WHERE   X.rnum1 = 1;
'''


def next_service_info():
    return '''
SELECT  X.Client_ID
       ,X.NextServiceDate
       ,X.NextServiceLocation
       ,X.NextServiceActivityCode
       ,X.NextServiceActivity
FROM    ( SELECT    c.SourceClient_ID AS Client_ID
                   ,s.ServiceDate AS NextServiceDate
                   ,l.LOC_Name AS NextServiceLocation
                   ,a.ActivityCode AS NextServiceActivityCode
                   ,a.Activity AS NextServiceActivity
                   ,ROW_NUMBER() OVER ( PARTITION BY c.SourceClient_ID ORDER BY s.servicedate ASC ) AS rnum2
          FROM      Service AS s
                    LEFT JOIN Location AS l WITH ( NOLOCK ) ON l.LOC_ID = s.LOC_ID
                    LEFT JOIN dbo.Activity AS a WITH ( NOLOCK ) ON a.Activity_ID = s.Activity_ID
                    INNER JOIN Client AS c WITH ( NOLOCK ) ON c.Client_ID = s.Client_ID
          WHERE     s.ServiceDate > GETDATE()
                    AND s.ORG_ID = 1
                    AND s.DeletedFlag = 0
                    AND a.ActivityCode != 'MEMO'
        ) X
WHERE   X.rnum2 = 1;
'''



def loc_info():
    return '''
SELECT  X.SourceClient_ID as Client_ID
,       X.HLink_LOC
FROM    ( SELECT    c.SourceClient_ID
          ,         cp.HLink_LOC
          ,         ROW_NUMBER() OVER ( PARTITION BY c.SourceClient_ID ORDER BY cp.BeginDate DESC ) AS rnum4
          FROM      dbo.ClientProgram AS cp
                    LEFT JOIN Client AS c ON c.Client_ID = cp.Client_ID
          WHERE     cp.PROG_ID = 6101
                    AND cp.ORG_ID = 1
                    AND cp.EndDate IS NULL
        ) X
WHERE   X.rnum4 = 1;
'''


def status_info():
    return '''
SELECT  c.SourceClient_ID as Client_ID
,       c.ClientStatus AS MemberStatus
FROM    Client AS c
WHERE   c.ORG_ID = 1;
'''


def two_payor_info():
    return '''
SELECT DISTINCT
    X.SourceClient_ID AS Client_ID
  , CASE WHEN X.openpayorcount > 1 THEN 1
         ELSE 0
    END AS MoreThanOnePayorInd
FROM
    ( SELECT
        c.SourceClient_ID
      , COUNT(c.SourceClient_ID) AS openpayorcount
      FROM
        dbo.ClientPayor AS cp
      INNER JOIN Client AS c ON c.Client_ID = cp.Client_ID
                                AND cp.ORG_ID = 1
                                AND cp.EndDate IS NULL
      GROUP BY
        c.SourceClient_ID
    ) X; 
'''
