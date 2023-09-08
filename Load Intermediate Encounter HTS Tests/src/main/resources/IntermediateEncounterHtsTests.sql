IF OBJECT_ID(N'[ODS].[dbo].[Intermediate_EncounterHTSTests]', N'U') IS NOT NULL
DROP TABLE [ODS].[dbo].[Intermediate_EncounterHTSTests];

BEGIN
with source_data as (
    select
        /* partition for the same SiteCode, PatientPK, TestDate and pick the latest Encounter ID */
        row_number() over (partition by SiteCode,PatientPK,TestDate,TestType order by EncounterId desc) as num,
        TestDate,
        EncounterId,
        SiteCode,
        PatientPK,
        PatientPKHash,
        EMR,
        Project,
        DateExtracted,
        EverTestedForHiv,
        MonthsSinceLastTest,
        ClientTestedAs ,
        EntryPoint,
        TestStrategy,
        TestResult1,
        TestResult2 ,
        FinalTestResult,
        PatientGivenResult ,
        TbScreening,
        ClientSelfTested,
        CoupleDiscordant,
        TestType,
        Setting,
        Consent
    from ODS.dbo.HTS_ClientTests
    where FinalTestResult is not null and TestDate is not null and EncounterId is not null
      and  TestDate >= cast('2015-01-01' as date) and TestDate <= getdate()
)
select
    source_data.*,cast(getdate() as date) as LoadDate
into [ODS].[dbo].[Intermediate_EncounterHTSTests]
from source_data
where num=1

END