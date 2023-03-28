IF OBJECT_ID(N'[ODS].[dbo].[Intermediate_PharmacyDispenseAsAtDate]', N'U') IS NOT NULL
DROP TABLE [ODS].[dbo].[Intermediate_PharmacyDispenseAsAtDate];
BEGIN
With PharmacyDispenseAsAtDate AS (
    SELECT row_number() OVER (PARTITION BY SiteCode,PatientPK ORDER BY DispenseDate DESC) AS NUM,
           PatientID ,
           SiteCode,
           PatientPK,
           PatientPKHash,
           PatientIDHash,
           DispenseDate as LastDispenseDate,
           CASE WHEN ExpectedReturn IS NULL THEN DATEADD(dd,30,DispenseDate) ELSE ExpectedReturn End AS ExpectedReturn,
           cast(getdate() as date) as LoadDate
    FROM ODS.dbo.CT_PatientPharmacy
)
Select PharmacyDispenseAsAtDate.*
INTO [ODS].[dbo].[Intermediate_PharmacyDispenseAsAtDate]
from PharmacyDispenseAsAtDate
where NUM=1 and  LastDispenseDate<=EOMONTH(DATEADD(mm,-1,GETDATE()))
END