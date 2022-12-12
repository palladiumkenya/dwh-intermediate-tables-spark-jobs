truncate table dbo.Intermediate_OrderedViralLoads;
with source_OrderedViralLoads as (
    select
        row_number() over(partition by PatientID, SiteCode, PatientPK order by OrderedbyDate desc) as rank,
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
insert into dbo.Intermediate_OrderedViralLoads
select
    source_OrderedViralLoads.*,
    cast(getdate() as date) as LoadDate
from source_OrderedViralLoads
where rank <= 10