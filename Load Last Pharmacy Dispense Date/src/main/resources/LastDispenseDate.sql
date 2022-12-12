
truncate table dbo.Intermediate_LastPharmacyDispenseDate;
With LastPharmacyDispenseDate AS (
    SELECT  row_number() OVER (PARTITION BY PatientID ,SiteCode,PatientPK ORDER BY DispenseDate DESC) AS NUM,
            PatientID ,
            SiteCode,
            PatientPK,
            DispenseDate as LastDispenseDate,
            CASE WHEN ExpectedReturn IS NULL THEN DATEADD(dd,30,DispenseDate) ELSE ExpectedReturn End AS ExpectedReturn,
            cast(getdate() as date) as LoadDate
    FROM ODS.dbo.CT_PatientPharmacy
)
insert into dbo.Intermediate_LastPharmacyDispenseDate
Select LastPharmacyDispenseDate.* from LastPharmacyDispenseDate where NUM=1
