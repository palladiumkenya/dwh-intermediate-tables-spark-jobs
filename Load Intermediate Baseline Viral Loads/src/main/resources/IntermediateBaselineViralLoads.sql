truncate table dbo.Intermediate_BaseLineViralLoads;

with source_BaseLineViralLoads as (
    select
        row_number() over(partition by PatientID, SiteCode, PatientPK order by OrderedbyDate asc) as rank,
        PatientID,
        SiteCode,
        PatientPK,
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

insert into dbo.Intermediate_BaseLineViralLoads
select
    source_BaseLineViralLoads.*,
    cast(getdate() as date) as LoadDate
from source_BaseLineViralLoads
where rank = 1