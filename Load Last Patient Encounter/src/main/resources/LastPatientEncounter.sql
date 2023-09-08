IF OBJECT_ID(N'[ODS].[dbo].[Intermediate_LastPatientEncounter]', N'U') IS NOT NULL
DROP TABLE [ODS].[dbo].[Intermediate_LastPatientEncounter]
BEGIN

--Pick the latest LastVisit and Next Appointment dates from Pharmacy
WITH Pharmacy AS (
    SELECT   ROW_NUMBER()OVER (PARTITION by SiteCode,PatientPK  ORDER BY DispenseDate Desc ) As NUM ,
             SiteCode,
             PatientPK ,
             DispenseDate As LastEncounterDate,
             Case When DATEDIFF(dd,GETDATE(),ExpectedReturn) >= 365 or ExpectedReturn ='1900-01-01' or  ExpectedReturn is null  THEN DATEADD(dd,30,DispenseDate) ELSE ExpectedReturn End as NextAppointmentDate
    FROM ODS.dbo.CT_PatientPharmacy  As LastEncounter
    where DispenseDate <= EOMONTH(DATEADD(mm,-1,GETDATE()))
),
--Pick Expected return and Lastvisit  dates from ARTPatient only if Expected return is <365days and add 30 days to Last visit if it is null
     ART_expected_dates_logic AS (
         SELECT
             PatientID,
             SiteCode,
             PatientPK ,
             LastVisit,
             ExpectedReturn,
             CASE
                 WHEN DATEDIFF(dd,GETDATE(),ExpectedReturn) <= 365 THEN ExpectedReturn Else DATEADD(day, 30, LastVisit)
                 END AS expected_return_on_365,
             case when LastVisit is null Then DATEADD(day, 30, LastVisit) else LastVisit End AS last_visit_plus_30_days
         FROM ODS.dbo.CT_ARTPatients
         where LastVisit <= EOMONTH(DATEADD(mm,-1,GETDATE()))
     ),
--Pick latestVisit and TCA from the visits Table
     LatestVisit As (
         Select ROW_NUMBER()OVER (PARTITION by SiteCode,PatientPK  ORDER BY VisitDate Desc ) As NUM,
                SiteCode,
                PatientPK ,
                VisitDate as LastVisitDate,
                Case When NextAppointmentDate is NULL THEN DATEADD(dd,30,VisitDate) ELSE NextAppointmentDate End as NextAppointmentDate
         from ODS.dbo.CT_PatientVisits
         where VisitDate <= EOMONTH(DATEADD(mm,-1,GETDATE()))
     ),
     Patients As (
         Select
             PatientId,
             PatientPK,
             sitecode
         from ODS.dbo.CT_ARTPatients
     ),
--Compare Pharmacy and ART last visits and expected return dates  and Pick the higher of the 2
     PharmacyART_Visits As (
         SELECT
             Patients.PatientID,
             Patients.PatientPK,
             Patients.PatientIDHash,
             Patients.PatientPKHash,
             Patients.SiteCode,
             Case when Pharmacy.LastencounterDate >=ART_expected_dates_logic.Lastvisit or ART_expected_dates_logic.Lastvisit is null
                      Then Pharmacy.LastEncounterDate Else ART_expected_dates_logic.Lastvisit End As LastVisitART_Pharmacy,
             Case when Pharmacy.NextAppointmentdate>=ART_expected_dates_logic.expectedReturn or ART_expected_dates_logic.expectedReturn is null  Then Pharmacy.NextAppointmentdate else ART_expected_dates_logic.expectedReturn End as NextappointmentDate
         from Patients
                  left join Pharmacy on  Patients.PatientPk=Pharmacy.PatientPk and Patients.Sitecode=Pharmacy.Sitecode and Num=1
                  left join ART_expected_dates_logic on Patients.PatientPk=ART_expected_dates_logic.PatientPk and Patients.Sitecode=ART_expected_dates_logic.Sitecode
     ),
--compare the results of the Pharmacy and ART above with when date add has been applied for the patients mising  TCAs and pick the greater
     PharmacyART_Computed As (
         SELECT
             OrderedVisits.PatientID,
             OrderedVisits.PatientPK,
             OrderedVisits.PatientIDHash,
             OrderedVisits.PatientPKHash,
             OrderedVisits.SiteCode,
             Case When OrderedVisits.LastVisitART_Pharmacy >=ART_expected_dates_logic.last_visit_plus_30_days Then
                      OrderedVisits.LastVisitART_Pharmacy Else ART_expected_dates_logic.last_visit_plus_30_days  End As LastEncounterDate,
             NextappointmentDate
         from PharmacyART_Visits
                  left join ART_expected_dates_logic on  PharmacyART_Visits.PatientPk=ART_expected_dates_logic.PatientPk and PharmacyART_Visits.Sitecode=ART_expected_dates_logic.Sitecode
     ),
     CombinedVisits As (
         Select
             PharmacyART_Computed.PatientID,
             PharmacyART_Computed.PatientPK,
             PharmacyART_Computed.PatientIDHash,
             PharmacyART_Computed.PatientPKHash,
             PharmacyART_Computed.Sitecode ,
             Case When PharmacyART_Computed.LastEncounterDate >= coalesce(LatestVisit.LastVisitDate, PharmacyART_Computed.LastEncounterDate) THEN PharmacyART_Computed.LastEncounterDate ELSE LatestVisit.LastVisitDate  END AS LastEncounterDate,
             Case  When PharmacyART_Computed.NextappointmentDate>= coalesce (LatestVisit.NextappointmentDate, PharmacyART_Computed.NextappointmentDate) THEN  PharmacyART_Computed.NextappointmentDate else LatestVisit.NextAppointmentDate end as NextAppointmentDate
         from PharmacyART_Computed
                  left join LatestVisit on PharmacyART_Computed.PatientPk=LatestVisit.PatientPk and PharmacyART_Computed.Sitecode=LatestVisit.Sitecode and Num=1
     ),
     VistsWithLastEncounter as (
         select
             *
         from CombinedVisits
         where LastEncounterDate is not null
     )
Select distinct
    PatientID,
    SiteCode,
    PatientPK ,
    PatientPKHash,
    PatientIDHash,
    LastEncounterDate,
    CASE
        WHEN DATEDIFF(dd,GETDATE(),NextAppointmentDate) <= 365 THEN NextAppointmentDate Else DATEADD(day, 30, LastEncounterDate)
        END AS NextAppointmentDate,
    cast (getdate() as DATE) as LoadDate
INTO ODS.dbo.Intermediate_LastPatientEncounter
from CombinedVisits
where LastEncounterDate <= EOMONTH(DATEADD(mm,-1,GETDATE()))

END