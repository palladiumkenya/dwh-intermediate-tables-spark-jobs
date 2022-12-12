truncate table dbo.Intermediate_LastestWeightHeight;

with source_LatestWeightHeight as (
    select
        row_number() over (partition by PatientID ,SiteCode,PatientPK order by VisitDate desc) as rank,
        VisitDate,
        PatientID ,
        SiteCode,
        PatientPK,
        VisitID,
        Weight,
        Height,
        VisitBy
    from ODS.dbo.CT_PatientVisits
    where Weight is not null
)
insert into dbo.Intermediate_LastestWeightHeight
select
    source_LatestWeightHeight.*,
    cast(getdate() as date) as LoadDate
from source_LatestWeightHeight
where rank = 1