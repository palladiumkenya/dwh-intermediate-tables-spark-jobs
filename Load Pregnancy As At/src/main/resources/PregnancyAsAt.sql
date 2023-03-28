IF OBJECT_ID(N'[ODS].[dbo].[Intermediate_PregnancyAsATInitiation]', N'U') IS NOT NULL
DROP TABLE [ODS].[dbo].[Intermediate_PregnancyAsATInitiation];
BEGIN
With PregnancyAsATInitiation As (
    SELECT

        DISTINCT Patients.PatientID ,Patients.PatientPK ,Patients.PatientIDHash,Patients.PatientPKHash, Patients.SiteCode,
                 CASE WHEN SUM(CASE WHEN YEAR(ART.StartARTDate) = YEAR(VisitDate) THEN 1 ELSE 0 END) > 1 THEN 1 ELSE 0 END  AS PregnantARTStart ,
                 CASE WHEN SUM(CASE WHEN YEAR(Patients.RegistrationAtCCC) = YEAR(VisitDate) THEN 1  ELSE 0 END) > 1 THEN 1 ELSE 0 END  AS PregnantAtEnrol,
                 cast(getdate() as date) as LoadDate

    FROM ODS.dbo.CT_PatientVisits Visits
             INNER JOIN ODS.dbo.CT_Patient Patients ON  Visits.PatientPK=Patients.PatientPK AND Patients.SiteCode=Visits.SiteCode
             INNER JOIN ODS.dbo.CT_ARTPatients ART ON ART.PatientPK=Patients.PatientPK AND Patients.SiteCode=ART.SiteCode
    WHERE (Visits.Pregnant = 'YES' or Visits.Pregnant = 'Y')  AND Patients.Gender= 'F'
    GROUP BY Patients.PatientID ,Patients.PatientPK ,Patients.PatientIDHash,Patients.PatientPKHash,Patients.SiteCode, ART.StartARTDate, Visits.VisitDate
)
Select
    PregnancyAsATInitiation.PatientID ,
    PregnancyAsATInitiation.PatientPK ,
    PregnancyAsATInitiation.PatientPKHash,
    PregnancyAsATInitiation.PatientIDHash,
    PregnancyAsATInitiation.SiteCode,
    PregnancyAsATInitiation.PregnantARTStart,
    PregnancyAsATInitiation.PregnantAtEnrol,
    PregnancyAsATInitiation.LoadDate
INTO [ODS].[dbo].[Intermediate_PregnancyAsATInitiation]
FROM  PregnancyAsATInitiation
END