IF OBJECT_ID(N'[ODS].[dbo].[Intermediate_PrepRefills]', N'U') IS NOT NULL
DROP TABLE [ODS].[dbo].[Intermediate_PrepRefills];

BEGIN
With PrepPatients AS (
    SELECT distinct

        Patients.PrepNumber,
        Patients.PrepNumberHash,
                  Patients.PatientPk,
                  Patients.PatientPKHash,
                  Patients.PrepEnrollmentDate,
                  Patients.SiteCode

    FROM ODS.dbo.PrEP_Patient Patients
    where Patients.PrepNumber is not null
),
     prep_refills_ordered as (
         select
             ROW_NUMBER () OVER (PARTITION BY PrepNumber, PatientPk, SiteCode ORDER BY DispenseDate Asc) As RowNumber,
             PrepNumber
              ,PatientPk
              ,SiteCode
              ,HtsNumber
              ,RegimenPrescribed
              ,DispenseDate
         from ODS.dbo.PrEP_Pharmacy
     ),
     PrepRefil1stMonth As (
         select
             Refil.PrepNumber
              ,Refil.PatientPk
              ,Refil.SiteCode
              ,Refil.HtsNumber
              ,RegimenPrescribed
              ,Refil.DispenseDate
              ,Patients.PrepEnrollmentDate
              ,DATEDIFF(dd, Patients.PrepEnrollmentDate, Refil.DispenseDate)AS Refil1DiffInDays
              ,Tests.TestDate
              ,Tests.FinalTestResult
              ,Case
                   when Tests.FinalTestResult in ('Inconclusive','Negative','Positive') THEN 'Yes'
                   Else 'No '
             End as Tested
         from prep_refills_ordered as  Refil
                  left join ODS.dbo.PrEP_Patient Patients on Refil.PrepNumber=Patients.PrepNumber and Refil.PatientPk=Patients.PatientPk and Refil.SiteCode=Patients.SiteCode
                  left join ODS.dbo.HTS_ClientTests Tests on Refil.PatientPk=Tests.PatientPk and Refil.SiteCode=Tests.SiteCode and Refil.DispenseDate=Tests.TestDate
         where Refil.PrepNumber is not null
           and DATEDIFF(dd, Patients.PrepEnrollmentDate, Refil.DispenseDate) between 30 and 37
           and Refil.RowNumber = 1
     ),
     PrepRefil3rdMonth As (
         SELECT
             Refil.PrepNumber
              ,Refil.PatientPk
              ,Refil.SiteCode
              ,Refil.HtsNumber
              ,Refil.RegimenPrescribed
              ,Refil.DispenseDate
              ,Patients.PrepEnrollmentDate
              ,DATEDIFF(dd, Patients.PrepEnrollmentDate, Refil.DispenseDate) AS Refil3DiffInDays
              ,Tests.TestDate
              ,Tests.FinalTestResult
              ,Case
                   when Tests.FinalTestResult in ('Inconclusive','Negative','Positive') THEN 'Yes'
                   Else 'No '
             End as Tested
         FROM ODS.dbo.PrEP_Pharmacy Refil
                  left join ODS.dbo.PrEP_Patient Patients on Refil.PrepNumber=Patients.PrepNumber and Refil.PatientPk=Patients.PatientPk and Refil.SiteCode=Patients.SiteCode
                  left join ODS.dbo.HTS_ClientTests Tests on Refil.PatientPk=Tests.PatientPk and Refil.SiteCode=Tests.SiteCode and Refil.DispenseDate=Tests.TestDate
                  left join PrepRefil1stMonth on PrepRefil1stMonth.PatientPk = Refil.PatientPk and PrepRefil1stMonth.SiteCode = Refil.SiteCode
         where Refil.PrepNumber is not null and DATEDIFF(dd, PrepRefil1stMonth.DispenseDate, Refil.DispenseDate) between 60 and 67
     )
SELECT
    distinct PrepPatients.PrepNumber
           ,PrepPatients.PatientPk
           ,PrepPatients.PatientPkHash
           ,PrepPatients.PrepNumberHash
           ,PrepPatients.SiteCode
           ,PrepRefil1stMonth.Refil1DiffInDays As Refil1DiffInDays
           ,PrepRefil1stMonth.FinalTestResult As TestResultsMonth1
           ,PrepRefil1stMonth.TestDate As TestDateMonth1
           ,PrepRefil1stMonth.DispenseDate As DispenseDateMonth1
           ,PrepRefil3rdMonth.Refil3DiffInDays As  Refil3DiffInDays
           ,PrepRefil3rdMonth.FinalTestResult As TestResultsMonth3
           ,PrepRefil3rdMonth.TestDate As TestDateMonth3
           ,PrepRefil3rdMonth.DispenseDate As DispenseDateMonth3
           ,cast(getdate() as date) as LoadDate
INTO ODS.dbo.Intermediate_PrepRefills
from PrepPatients
         LEFT JOIN PrepRefil1stMonth on PrepPatients.PatientPk=PrepRefil1stMonth.PatientPk and PrepPatients.SiteCode=PrepRefil1stMonth.SiteCode
         LEFT JOIN PrepRefil3rdMonth on PrepPatients.PatientPk=PrepRefil3rdMonth.PatientPk and PrepPatients.SiteCode=PrepRefil3rdMonth.SiteCode
END