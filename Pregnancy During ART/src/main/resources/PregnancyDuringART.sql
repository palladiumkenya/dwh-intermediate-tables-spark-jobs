IF OBJECT_ID(N'[ODS].[dbo].[Intermediate_PregnancyDuringART]', N'U') IS NOT NULL
DROP TABLE [ODS].[dbo].[Intermediate_PregnancyDuringART];
BEGIN

with visit_dates_ordering as (
    select
        PatientPK,
        SiteCode,
        VisitDate,
        row_number() over(partition by PatientPK, SiteCode order by VisitDate asc) as rnk
    from ODS.dbo.CT_PatientVisits
    where Pregnant in ('YES',  'Y') and
            VOIDED = 0
),
     dates_check as (
         SELECT
             Patients.PatientPK,
             Patients.PatientID,
             Patients.SiteCode,
             Patients.PatientIDHash,
             Patients.PatientPKHash,
             CASE WHEN VisitDate > ART.StartARTDate THEN 1 ELSE 0 END AS PregnantDuringART,
             cast(getdate() as date) as LoadDate
         FROM visit_dates_ordering as visits
                  INNER JOIN ODS.dbo.CT_Patient Patients ON  visits.PatientPK=Patients.PatientPK AND Patients.SiteCode=visits.SiteCode
                  INNER JOIN ODS.dbo.CT_ARTPatients ART ON ART.PatientPK=Patients.PatientPK AND Patients.SiteCode=ART.SiteCode
         WHERE Patients.Gender = 'Female' and
                 visits.rnk = 1 and
                 Patients.VOIDED = 0 and
                 ART.VOIDED = 0
     )
select
    dates_check.PatientPK ,
    dates_check.PatientID,
    dates_check.PatientPKHash,
    dates_check.PatientIDHash,
    dates_check.SiteCode,
    PregnantDuringART,
    dates_check.LoadDate
into [ODS].[dbo].[Intermediate_PregnancyDuringART]
from  dates_check
END