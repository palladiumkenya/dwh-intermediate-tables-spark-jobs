IF OBJECT_ID(N'[ODS].[dbo].[Intermediate_ViralLoadsIntervals]', N'U') IS NOT NULL
DROP TABLE [ODS].[dbo].[Intermediate_ViralLoadsIntervals];

BEGIN

with source_viral_loads as (
    select
        labs.PatientID,
        labs.SiteCode,
        labs.PatientPK,
        art.PatientPKHash,
        art.PatientIDHash,
        VisitID,
   [OrderedbyDate],
   [ReportedbyDate],
   [TestName],
    TestResult,
    case
    when isnumeric([TestResult]) = 1 then
    case
    when cast(replace([TestResult], ',', '') as  float) < 200.00 then 1
    else 0
end
else
				case
					when [TestResult]  in ('undetectable','NOT DETECTED','0 copies/ml','LDL','Less than Low Detectable Level') then 1
					else 0
end
end as VLSup,
		StartARTDate
	FROM ODS.dbo.CT_PatientLabs as labs
	INNER join ODS.dbo.CT_ARTPatients art on art.PatientPK = labs.Patientpk
		and art.SiteCode = labs.SiteCode
	where TestName = 'Viral Load'
			and TestName <>'CholesterolLDL (mmol/L)' and TestName <> 'Hepatitis C viral load'
			and TestResult is not null AND labs.VOIDED =0 AND art.VOIDED = 0
),
_6monthVL_data as (
	select
		row_number() over(partition by  SiteCode, PatientPK order by OrderedbyDate asc) as rank,
		SiteCode,
		PatientPK,
		TestResult as _6monthVL,
		OrderedbyDate as _6monthVLDate,
		VLSup as _6MonthVLSup
	from
	source_viral_loads
	where datediff(mm, startARTDate, OrderedbyDate) = 6
),
_12monthVL_data as (
	select
		row_number() over(partition by  SiteCode, PatientPK order by OrderedbyDate asc) as rank,
		SiteCode,
		PatientPK,
		TestResult as _12monthVL,
		OrderedbyDate as _12monthVLDate,
		VLSup as _12MonthVLSup
	from
	source_viral_loads
	where datediff(mm, startARTDate, OrderedbyDate) = 12
),
_18monthVL_data as (
	select
		row_number() over(partition by  SiteCode, PatientPK order by OrderedbyDate asc) as rank,
		SiteCode,
		PatientPK,
		TestResult as _18monthVL,
		OrderedbyDate as _18monthVLDate,
		VLSup as _18MonthVLSup
	from
	source_viral_loads
	where datediff(mm, startARTDate, OrderedbyDate) = 18
),
_24monthVL_data as (
	select
		row_number() over(partition by  SiteCode, PatientPK order by OrderedbyDate asc) as rank,
		SiteCode,
		PatientPK,
		TestResult as _24monthVL,
		OrderedbyDate as _24monthVLDate,
		VLSup as _24MonthVLSup
	from
	source_viral_loads
	where datediff(mm, startARTDate, OrderedbyDate) = 24
),
distinct_viral_load_clients as (
	select
		distinct Sitecode,
		PatientPK,
		PatientID,
		PatientPKHash,
		PatientIDHash
	from source_viral_loads
)
select
    /* filter for rank = 1 to pick the latest result
        because a client can have more than one result in a month
    */
    --cast( '' as nvarchar(100)) PatientPKHash,
    distinct_viral_load_clients.PatientPk,
    distinct_viral_load_clients.PatientID,
    distinct_viral_load_clients.SiteCode,
    distinct_viral_load_clients.PatientPKHash,
    distinct_viral_load_clients.PatientIDHash,
    _6monthVL_data._6monthVL,
    _6monthVL_data._6monthVLDate,
    _6monthVL_data._6MonthVLSup,
    _12monthVL_data._12monthVL,
    _12monthVL_data._12monthVLDate,
    _12monthVL_data._12MonthVLSup,
    _18monthVL_data._18monthVL,
    _18monthVL_data._18monthVLDate,
    _18monthVL_data._18MonthVLSup,
    _24monthVL_data._24monthVL,
    _24monthVL_data._24monthVLDate,
    _24monthVL_data._24MonthVLSup,
    cast(getdate() as date) as LoadDate
into [ODS].[dbo].[Intermediate_ViralLoadsIntervals]
from distinct_viral_load_clients
    left join _6monthVL_data on _6monthVL_data.PatientPk = distinct_viral_load_clients .PatientPK
    and _6monthVL_data.SiteCode = distinct_viral_load_clients.SiteCode
    and _6monthVL_data.rank = 1
    left join _12monthVL_data on _12monthVL_data.PatientPk = distinct_viral_load_clients .PatientPK
    and _12monthVL_data.SiteCode = distinct_viral_load_clients.SiteCode
    and _12monthVL_data.rank = 1
    left join _18monthVL_data on _18monthVL_data.PatientPk = distinct_viral_load_clients .PatientPK
    and _18monthVL_data.SiteCode = distinct_viral_load_clients.SiteCode
    and _18monthVL_data.rank = 1
    left join _24monthVL_data on _24monthVL_data.PatientPk = distinct_viral_load_clients .PatientPK
    and _24monthVL_data.SiteCode = distinct_viral_load_clients.SiteCode
    and _24monthVL_data.rank = 1

END