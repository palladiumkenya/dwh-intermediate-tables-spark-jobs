truncate table dbo.Intermediate_ARTOutcomes;

With ARTOutcomes AS (
    Select
        DISTINCT
        Patients.PatientID,
        Patients.PatientPK,
        Patients.PatientPKHash,
        Patients.PatientIDHash,
        ART.startARTDate,
    YEAR(ART.startARTDate) AS Cohort,
    Exits.ExitReason,
    Exits.ExitDate,
    LastPatientEncounter.LastEncounterDate,
    LastPatientEncounter.NextAppointmentDate,
    CASE WHEN ISNULL(LastPatientEncounter.LastEncounterDate, ART.LastVisit) <= GETDATE()
    THEN
   (CASE
    WHEN  Exits.ExitDate IS NOT NULL and Exits.ExitReason<>'DIED' and coalesce(Exits.EffectiveDiscontinuationDate, LastPatientEncounter.NextAppointmentDate) > EOMONTH(DATEADD(mm,-1,GETDATE()))  THEN 'V'--When a TO and LFTU has an discontinuationdate > Last day of Previous month
    When Exits.ExitDate IS NOT NULL and Exits.ExitReason<>'DIED' and Exits.ReEnrollmentDate between  DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0) and DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1) THEN 'V'
    When Exits.ExitDate >EOMONTH(DATEADD(mm,-1,GETDATE())) and Exits.ExitReason='DIED' THEN 'V'
    WHEN Exits.ExitDate IS NOT NULL THEN SUBSTRING(Exits.ExitReason,1,1)--When exit date is available then Inserts the exit reasons , Extracts 1 character from Exit reasons starting from position 1
    WHEN ART.startARTDate> DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE()),0)) THEN 'NP'-- When StartARTDate is after Last Day of Previous EOM
    WHEN EOMONTH(DATEADD(mm,-1,GETDATE())) < ISNULL(LastPatientEncounter.NextAppointmentDate,ART.ExpectedReturn)-- When last day of previous month is less than TCA
    OR DATEDIFF( dd, ISNULL(LastPatientEncounter.NextAppointmentDate,ART.ExpectedReturn), EOMONTH(DATEADD(mm,-1,GETDATE()))) <=30 THEN 'V'-- Date diff btw TCA  and LAst day of Previous month-- must not be late by 30 days
    WHEN DATEDIFF( dd, ISNULL(LastPatientEncounter.NextAppointmentDate,ART.ExpectedReturn), EOMONTH(DATEADD(mm,-1,GETDATE()))) >30 THEN 'uL'--Date diff btw TCA  and Last day of Previous month
    WHEN LastPatientEncounter.NextAppointmentDate IS NULL and ART.ExpectedReturn IS NULL THEN 'NV'

    END
    )
    ELSE 'FV' END
AS ARTOutcome,
   cast (Patients.SiteCode as nvarchar) As SiteCode,
    Patients.Emr,
     Patients.Project


FROM ODS.dbo.CT_Patient Patients
INNER JOIN ODS.dbo.CT_ARTPatients  ART  ON  Patients.PatientPK=ART.PatientPK and Patients.Sitecode=ART.Sitecode
LEFT JOIN ODS.dbo.Intermediate_LastPatientEncounter  LastPatientEncounter ON   Patients.PatientPK COLLATE Latin1_General_CI_AS =LastPatientEncounter.PatientPK   AND Patients.SiteCode COLLATE Latin1_General_CI_AS  =LastPatientEncounter.SiteCode COLLATE Latin1_General_CI_AS
LEFT JOIN ODS.dbo.CT_PatientStatus Exits   ON  Patients.PatientPK=Exits.PatientPK  and Patients.Sitecode=Exits.Sitecode


  WHERE  ART.startARTDate IS NOT NULL
),
LatestUpload AS (
select
    cast (SiteCode as nvarchar)As SiteCode ,
    Max(DateRecieved) As DateUploaded
 from DWAPICentral.dbo.FacilityManifest
  group by SiteCode
),

LatestVisits AS (
    Select
    distinct sitecode,
     Max(Visitdate) As SiteAbstractionDate
     from ODS.dbo.CT_PatientVisits
     group by SiteCode

)
insert into dbo.Intermediate_ARTOutcomes
Select
    ARTOutcomes.PatientID,
    ARTOutcomes.PatientPK,
    ARTOutcomes.startARTDate,
    YEAR(ARTOutcomes.startARTDate) AS Cohort,
    ARTOutcomes.ExitReason,
    ARTOutcomes.ExitDate,
    ARTOutcomes.LastEncounterDate,
    ARTOutcomes.NextAppointmentDate,
    ARTOutcomes.ARTOutcome,
    ARTOutcomes.SiteCode,
    ARTOutcomes.Emr,
    ARTOutcomes.Project,
    LatestUpload.DateUploaded,
    LatestVisits.SiteAbstractionDate,
    cast(getdate() as date) as LoadDate
from ARTOutcomes
    left join LatestUpload ON LatestUpload.SiteCode = ARTOutcomes.SiteCode
    left  join  LatestVisits  ON  LatestVisits.SiteCode = ARTOutcomes.SiteCode