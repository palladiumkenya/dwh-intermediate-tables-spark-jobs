IF OBJECT_ID(N'[ODS].[DBO].[Intermediate_LastestWeightHeight]', N'U') IS NOT NULL
DROP TABLE [ODS].[DBO].[Intermediate_LastestWeightHeight];
BEGIN
with source_LatestWeightHeight as (
    select
        row_number() over (partition by SiteCode,PatientPK order by VisitDate desc) as rank,
        VisitDate,
        PatientID ,
        SiteCode,
        PatientPK,
        PatientPKHash,
        PatientIDHash,
        VisitID,
        Weight,
        Height,
        VisitBy
    from ODS.dbo.CT_PatientVisits
    where Weight is not null and  VOIDED=0
)
select
    source_LatestWeightHeight.*,
    cast(getdate() as date) as LoadDate
into [ODS].[DBO].[Intermediate_LastestWeightHeight]
from source_LatestWeightHeight
where rank = 1
END