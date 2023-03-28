IF OBJECT_ID(N'[ODS].[dbo].[Intermediate_ARTOutcomes]', N'U') IS NOT NULL
DROP TABLE [ODS].[dbo].[Intermediate_ARTOutcomes];
BEGIN
With Exits As (
    Select
        ROW_NUMBER() over (PARTITION by PatientPk, Sitecode ORDER by ExitDate DESC) as RowNum,
        PatientPK,
        SiteCode,
        ExitDate,
        ExitReason,
        ExitDescription,
        EffectiveDiscontinuationDate,
        ReasonForDeath,
        ReEnrollmentDate
    from ODS.dbo.CT_PatientStatus
),
     Latestexits As (
         select
             PatientPK,
             SiteCode,
             ExitDate,
             ExitReason,
             ExitDescription,
             EffectiveDiscontinuationDate,
             ReEnrollmentDate,
             ReasonForDeath
         from Exits As Exits
         where RowNum=1

     ),

     ARTOutcomes AS (
         Select
             DISTINCT
             Patients.PatientID,
             Patients.PatientPK,
             Patients.PatientIDHash,
             Patients.PatientPKHash,
             ART.startARTDate,
    YEAR(ART.startARTDate) AS Cohort,
    LatestExits.ExitReason,
    LatestExits.ExitDate,
    LastPatientEncounter.LastEncounterDate,
    LastPatientEncounter.NextAppointmentDate,
    CASE WHEN ISNULL(LastPatientEncounter.LastEncounterDate, ART.LastVisit) <= GETDATE()
    THEN
    (CASE
    WHEN  LatestExits.ExitDate IS NOT NULL and LatestExits.ExitReason<>'DIED' and LatestExits.EffectiveDiscontinuationDate > EOMONTH(DATEADD(mm,-1,GETDATE()))  THEN 'V'--When a TO and LFTU has an discontinuationdate > Last day of Previous month
    WHEN  LatestExits.ExitReason<>'DIED' and LatestExits.EffectiveDiscontinuationDate between  DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0) and DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1)  THEN SUBSTRING(LatestExits.ExitReason,1,1)--When a TO and LFTU has an discontinuationdate during the reporting Month
    WHEN LatestExits.ExitDate IS NOT NULL and LatestExits.ExitReason<>'DIED' and   LatestExits.EffectiveDiscontinuationDate >=LastPatientEncounter.LastEncounterDate THEN SUBSTRING(LatestExits.ExitReason,1,1)--When Effective discontinuation date is aafter Last encounter date  then Inserts the exit reasons , Extracts 1 character from Exit reasons starting from position 1
    WHEN  LatestExits.ExitDate IS NOT NULL and LatestExits.ExitReason<>'DIED' and  LastPatientEncounter.NextAppointmentDate > EOMONTH(DATEADD(mm,-1,GETDATE()))  THEN 'V'--When a TO and LFTU has an discontinuationdate > Last day of Previous month
    When LatestExits.ExitDate IS NOT NULL and LatestExits.ExitReason<>'DIED' and LastPatientEncounter.NextAppointmentDate between DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0) and DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1) THEN 'V'
    When LatestExits.ExitDate IS NOT NULL and LatestExits.ExitReason<>'DIED' and LatestExits.ExitDate >EOMONTH(DATEADD(mm,-1,GETDATE()))  THEN 'V'
    When LatestExits.ExitDate >EOMONTH(DATEADD(mm,-1,GETDATE())) and LatestExits.ExitReason='DIED' THEN 'V'
     --WHEN LatestExits.ExitDate IS NOT NULL THEN SUBSTRING(LatestExits.ExitReason,1,1)--When exit date is available then Inserts the exit reasons , Extracts 1 character from Exit reasons starting from position 1
    WHEN ART.startARTDate> DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE()),0)) THEN 'NP'-- When StartARTDate is after Last Day of Previous EOM
    WHEN LastPatientEncounter.NextAppointmentDate is NULL THEN 'NV'
    WHEN  ISNULL(LastPatientEncounter.NextAppointmentDate,ART.ExpectedReturn) > EOMONTH(DATEADD(mm,-1,GETDATE()))   Then 'V'   -- When last day of previous month is less than TCA
    When  DATEDIFF( dd, ISNULL(LastPatientEncounter.NextAppointmentDate,ART.ExpectedReturn), EOMONTH(DATEADD(mm,-1,GETDATE()))) <=30 THEN 'V'-- Date diff btw TCA  and LAst day of Previous month-- must not be late by 30 days
    WHEN DATEDIFF( dd, ISNULL(LastPatientEncounter.NextAppointmentDate,ART.ExpectedReturn), EOMONTH(DATEADD(mm,-1,GETDATE()))) >30 THEN 'uL'--Date diff btw TCA  and Last day of Previous month
    WHEN LastPatientEncounter.NextAppointmentDate IS NULL and ART.ExpectedReturn IS NULL THEN 'NV'
    ELSE SUBSTRING(LatestExits.ExitReason,1,1)

    END
    )
    ELSE 'FV' END
	AS ARTOutcome, 
	   cast (Patients.SiteCode as nvarchar) As SiteCode,
		Patients.Emr,
		 Patients.Project
    
	FROM ODS.dbo.CT_Patient Patients
	INNER JOIN ODS.dbo.CT_ARTPatients  ART  ON  Patients.PatientPK=ART.PatientPK and Patients.Sitecode=ART.Sitecode
	LEFT JOIN ODS.dbo.Intermediate_LastPatientEncounter  LastPatientEncounter ON   Patients.PatientPK  =LastPatientEncounter.PatientPK   AND Patients.SiteCode  =LastPatientEncounter.SiteCode
	LEFT JOIN  LatestExits   ON  Patients.PatientPK=Latestexits.PatientPK  and Patients.Sitecode=Latestexits.Sitecode

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
Select
    ARTOutcomes.PatientID,
    ARTOutcomes.PatientPK,
    ARTOutcomes.PatientIDHash,
    ARTOutcomes.PatientPKHash,
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
INTO  [ODS].[dbo].[Intermediate_ARTOutcomes]
from ARTOutcomes
    left join LatestUpload ON LatestUpload.SiteCode = ARTOutcomes.SiteCode
    left  join  LatestVisits  ON  LatestVisits.SiteCode = ARTOutcomes.SiteCode
END