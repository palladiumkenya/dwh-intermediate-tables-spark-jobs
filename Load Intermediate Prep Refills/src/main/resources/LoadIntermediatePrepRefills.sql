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


     PrepRefil1stMonth As (

         SELECT ROW_NUMBER () OVER (PARTITION BY  Refil.PrepNumber,Refil.PatientPk,Refil.SiteCode ORDER BY Refil.DispenseDate Asc) As RowNumber,

             Refil.PrepNumber

              ,Refil.PatientPk

              ,Refil.SiteCode

              ,Refil.HtsNumber

              ,RegimenPrescribed

              ,Refil.DispenseDate

              ,Patients.PrepEnrollmentDate

              ,DATEDIFF(mm, Patients.PrepEnrollmentDate, Refil.DispenseDate)AS RefilMonth

              ,Tests.TestDate

              ,Tests.FinalTestResult

              ,Case when Tests.FinalTestResult in ('Inconclusive','Negative','Positive') THEN 'Yes' Else 'No '

             End as Tested

         FROM ODS.dbo.PrEP_Pharmacy Refil

                  left join ODS.dbo.PrEP_Patient Patients on Refil.PrepNumber=Patients.PrepNumber and Refil.PatientPk=Patients.PatientPk and Refil.SiteCode=Patients.SiteCode

                  left join ODS.dbo.HTS_ClientTests Tests on Refil.PatientPk=Tests.PatientPk and Refil.SiteCode=Tests.SiteCode and Refil.DispenseDate=Tests.TestDate

         where Refil.PrepNumber is not null and DATEDIFF(mm, Patients.PrepEnrollmentDate, Refil.DispenseDate)=1

     ),

     PrepRefil3rdMonth As (

         SELECT ROW_NUMBER () OVER (PARTITION BY  Refil.PrepNumber,Refil.PatientPk,Refil.SiteCode ORDER BY Refil.DispenseDate Asc) As RowNumber,

             Refil.PrepNumber

              ,Refil.PatientPk

              ,Refil.SiteCode

              ,Refil.HtsNumber

              ,RegimenPrescribed

              ,Refil.DispenseDate

              ,Patients.PrepEnrollmentDate

              ,DATEDIFF(mm, Patients.PrepEnrollmentDate, Refil.DispenseDate)AS RefilMonth

              ,Tests.TestDate

              ,Tests.FinalTestResult

              ,Case when Tests.FinalTestResult in ('Inconclusive','Negative','Positive') THEN 'Yes' Else 'No '

             End as Tested

         FROM ODS.dbo.PrEP_Pharmacy Refil

                  left join ODS.dbo.PrEP_Patient Patients on Refil.PrepNumber=Patients.PrepNumber and Refil.PatientPk=Patients.PatientPk and Refil.SiteCode=Patients.SiteCode

                  left join ODS.dbo.HTS_ClientTests Tests on Refil.PatientPk=Tests.PatientPk and Refil.SiteCode=Tests.SiteCode and Refil.DispenseDate=Tests.TestDate

         where Refil.PrepNumber is not null and DATEDIFF(mm, Patients.PrepEnrollmentDate, Refil.DispenseDate)=3

     )

SELECT distinct

    PrepPatients.PrepNumber

              ,PrepPatients.PatientPk
              ,PrepPatients.PatientPkHash
              ,PrepPatients.PrepNumberHash
              ,PrepPatients.SiteCode

              ,PrepRefil1stMonth.RefilMonth As  RefilMonth1

              ,PrepRefil1stMonth.FinalTestResult As TestResultsMonth1

              ,PrepRefil1stMonth.TestDate As TestDateMonth1

              ,PrepRefil1stMonth.DispenseDate As DispenseDateMonth1

              ,PrepRefil3rdMonth.RefilMonth As  RefilMonth3

              ,PrepRefil3rdMonth.FinalTestResult As TestResultsMonth3

              ,PrepRefil3rdMonth.TestDate As TestDateMonth3

              ,PrepRefil3rdMonth.DispenseDate As DispenseDateMonth3

INTO ODS.dbo.Intermediate_PrepRefills

from PrepPatients

         LEFT JOIN PrepRefil1stMonth on PrepPatients.PatientPk=PrepRefil1stMonth.PatientPk and PrepPatients.SiteCode=PrepRefil1stMonth.SiteCode

         LEFT JOIN PrepRefil3rdMonth on PrepPatients.PatientPk=PrepRefil3rdMonth.PatientPk and PrepPatients.SiteCode=PrepRefil3rdMonth.SiteCode

END