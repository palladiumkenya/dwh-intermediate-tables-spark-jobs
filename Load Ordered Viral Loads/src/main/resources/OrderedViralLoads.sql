IF OBJECT_ID(N'[ODS].[dbo].[Intermediate_OrderedViralLoads]', N'U') IS NOT NULL
DROP TABLE [ODS].[dbo].[Intermediate_OrderedViralLoads];
BEGIN
with source_OrderedViralLoads as (
    select
        row_number() over(partition by  SiteCode, PatientPK order by OrderedbyDate desc) as rank,
        PatientID,
        SiteCode,
        PatientPK,
        PatientPKHash,
        PatientIDHash,
        VisitID,
   [OrderedbyDate],
   [ReportedbyDate],
   [TestName],
    TestResult,
   [Emr],
   [Project],

    Reason
from ODS.dbo.CT_PatientLabs
where TestName = 'Viral Load'
  and TestName <>'CholesterolLDL (mmol/L)' and TestName <> 'Hepatitis C viral load'
  and TestResult is not null
    )
select
    source_OrderedViralLoads.*,
    cast(getdate() as date) as LoadDate
into [ODS].[dbo].[Intermediate_OrderedViralLoads]
from source_OrderedViralLoads
where rank <= 10
END