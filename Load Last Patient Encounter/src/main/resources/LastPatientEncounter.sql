truncate table ODS.dbo.Intermediate_LastPatientEncounter;

--Pick the highest LastVisit and Next Appointment from Pharmacy
WITH tbl AS (

    SELECT DISTINCT
        PatientID,
        SiteCode,
        PatientPK ,
        EncounterDateAsAt As LastEncounterDate,
        AppointmentDateAsAt As NextAppointmentDate
    FROM ODS.dbo.Intermediate_LastPatientEncounterAsAt As LastEncounter

),
--Pick Expected return and Lastvisit from ARTPatient only if Expected return is <365days and add 30 days to Last visit if it is null
     ART_expected_dates_logic AS (
         SELECT
             PatientID,
             SiteCode,
             PatientPK ,
             LastVisit,
             ExpectedReturn,
             CASE
                 WHEN DATEDIFF(dd,GETDATE(),ExpectedReturn) <= 365 THEN ExpectedReturn
                 END AS expected_return_on_365,
             case when LastVisit is null Then DATEADD(day, 30, LastVisit) else LastVisit End AS last_visit_plus_30_days
         FROM ODS.dbo.CT_ARTPatients

     )
insert into ODS.dbo.Intermediate_LastPatientEncounter
SELECT
    tbl.PatientID,
    tbl.SiteCode,
    tbl.PatientPK,
    tbl.LastEncounterDate,
    tbl.NextAppointmentDate,
    cast(getdate() as date) as LoadDate
FROM tbl
    LEFT JOIN ART_expected_dates_logic ON  ART_expected_dates_logic.SiteCode = tbl.SiteCode
    AND ART_expected_dates_logic.PatientID = tbl.PatientID
    AND ART_expected_dates_logic.PatientPK = tbl.PatientPK