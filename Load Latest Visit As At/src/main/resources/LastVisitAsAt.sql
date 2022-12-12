--Load_LastVisitAsAt
truncate table dbo.Intermediate_LastVisitAsAt;

With LastVisitAsAt AS (
    SELECT  row_number() OVER (PARTITION BY PatientID ,SiteCode,PatientPK ORDER BY VisitDate DESC) AS NUM,
            PatientID ,
            SiteCode,
            PatientPK,
            VisitDate AS VisitDateAsAt,
            CASE WHEN NextAppointmentDate IS NULL THEN DATEADD(dd,30,VisitDate) ELSE NextAppointmentDate End AS AppointmentDateAsAt ,
            cast(getdate() as date) as LoadDate
    FROM ODS.dbo.CT_PatientVisits
)
insert into dbo.Intermediate_LastVisitAsAt
Select LastVisitAsAt.* from LastVisitAsAt where NUM=1 and VisitDateAsAt<=EOMONTH(DATEADD(mm,-1,GETDATE()))