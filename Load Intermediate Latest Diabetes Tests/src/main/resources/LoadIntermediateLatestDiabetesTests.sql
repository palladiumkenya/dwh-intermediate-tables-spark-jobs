IF OBJECT_ID(N'[ODS].[dbo].[Intermediate_LatestDiabetesTests]', N'U') IS NOT NULL
DROP TABLE [ODS].[dbo].[Intermediate_LatestDiabetesTests];
BEGIN

with diabetes_tests_ordering as (
    /* get all Diabetes tests and order by date*/
    select
        ROW_NUMBER() OVER (PARTITION BY PatientPKHash, Sitecode ORDER BY OrderedbyDate DESC) AS RowNum,
        PatientPKHash,
        SiteCode,
        TestName,
        TRY_CAST(TestResult AS NUMERIC(18, 2)) AS NumericTestResult
    from ODS.dbo.CT_PatientLabs
    where
        (TestName in ('HgB', 'HbsAg', 'HBA1C')
            or TestName in ('FBS', 'Blood Sugar')
            )
      and voided = 0
),
     latest_diabetes_test as  (
         select
             *
         from diabetes_tests_ordering where RowNum = 1
     ),
     latest_diabetes_test_controlled as (
         /* get all last Diabetes tests that are within the controlled range*/
         select
             latest_diabetes_test.*
         from latest_diabetes_test
         where (TestName in ('HgB', 'HbsAg', 'HBA1C')  and NumericTestResult > 0.0 and NumericTestResult <= 6.5 )
            or (TestName in ('FBS', 'Blood Sugar') and NumericTestResult> 0.0 and NumericTestResult < 7.0  )
     )
select
    latest_diabetes_test.PatientPKHash,
    latest_diabetes_test.SiteCode,
    latest_diabetes_test.NumericTestResult,
    latest_diabetes_test.TestName,
    case when latest_diabetes_test.PatientPKHash is not null then 1 else 0 end as ScreenedDiabetes,
    case when latest_diabetes_test_controlled.PatientPKHash is not null then 1 else 0 end as IsDiabetesControlledAtLastTest
into ODS.dbo.Intermediate_LatestDiabetesTests
from  latest_diabetes_test
          left join latest_diabetes_test_controlled on latest_diabetes_test_controlled.PatientPKHash = latest_diabetes_test.PatientPKHash
    and latest_diabetes_test_controlled.SiteCode = latest_diabetes_test.SiteCode
END