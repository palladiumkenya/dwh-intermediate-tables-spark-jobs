IF OBJECT_ID(N'[ODS].[dbo].[Intermediate_LastVisitDate]', N'U') IS NOT NULL
DROP TABLE [ODS].[dbo].[Intermediate_LastVisitDate];
BEGIN

	---Load_LatestVisit
With source_data as (
    SELECT  row_number() OVER (PARTITION BY SiteCode,PatientPK ORDER BY VisitDate DESC) AS NUM,
            PatientID,
            SiteCode,
            PatientPK,
            PatientPKHash,
            PatientIDHash,
            VisitDate as LastVisitDate,
            visitID,
            BP,
            CASE WHEN NextAppointmentDate IS NULL THEN DATEADD(dd,30,VisitDate) ELSE NextAppointmentDate End AS NextAppointment
    FROM ODS.dbo.CT_PatientVisits
    WHERE VOIDED=0
),
     controlled_BP as (
         select
             PatientPK,
             SiteCode,
             BP
         from source_data
         where
                 charindex('/', BP) > 0
           and isnumeric(left(BP, charindex('/', BP) - 1)) = 1
           and isnumeric(right(BP, len(BP) - charindex('/', BP))) = 1
           and try_cast(left(BP, charindex('/', BP) - 1) as float) < 140.0
           and try_cast(right(BP, len(BP) - charindex('/', BP)) as float) < 90.0
           and num = 1
     )
select
    source_data.*,
    case when source_data.BP is not null then 1 else 0 end as ScreenedBPLastVisit,
    case when controlled_BP.PatientPK is not null then 1 else 0 end as IsBPControlledAtLastVisit,
    cast(getdate() as date) as LoadDate
into [ODS].[dbo].[Intermediate_LastVisitDate]
from source_data as source_data
    left join controlled_BP on controlled_BP.PatientPK = source_data.PatientPK
    and controlled_BP.SiteCode = source_data.SiteCode
where NUM = 1

END