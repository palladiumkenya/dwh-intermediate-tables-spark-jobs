---Load_LatestVisit
truncate table dbo.Intermediate_LastVisitDate;
With LatestVisit AS (
    SELECT  row_number() OVER (PARTITION BY PatientID ,SiteCode,PatientPK ORDER BY VisitDate DESC) AS NUM,
            PatientID,
            SiteCode,
            PatientPK,
            VisitDate as LastVisitDate,
            CASE WHEN NextAppointmentDate IS NULL THEN DATEADD(dd,30,VisitDate) ELSE NextAppointmentDate End AS NextAppointment,
            cast(getdate() as date) as LoadDate

    FROM ODS.dbo.CT_PatientVisits
)
insert into dbo.Intermediate_LastVisitDate
Select LatestVisit.* from LatestVisit
where NUM=1