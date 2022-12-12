truncate table dbo.Intermediate_PregnancyDuringART;

With PregnancyDuringART AS (
    SELECT
        PatientID ,
        PatientPK ,
        SiteCode,
        MAX(PregnantDuringART)AS PregnantDuringART,
        X.VisitDate,
        StartARTDate,
        cast(getdate() as date) as LoadDate
    FROM
        (
            SELECT DISTINCT
                Patients.PatientID ,
                Patients.PatientPK ,
                Patients.SiteCode,
                VisitDate,
                StartARTDate,
                CASE WHEN VisitDate >= ART.StartARTDate THEN 1 ELSE 0 END AS PregnantDuringART
            FROM CT_PatientVisits Visits
                     INNER JOIN CT_Patient Patients ON Visits.PatientID=Patients.PatientID AND Visits.PatientPK=Patients.PatientPK AND Patients.SiteCode=Visits.SiteCode
                     INNER JOIN CT_ARTPatients ART ON ART.PatientID=Patients.PatientID AND ART.PatientPK=Patients.PatientPK AND Patients.SiteCode=ART.SiteCode
            WHERE Visits.Pregnant = 'Yes' OR Visits.Pregnant = 'Y'

        ) X
    GROUP BY PatientID ,PatientPK ,SiteCode,VisitDate,SiteCode,StartARTDate
)

insert into dbo.Intermediate_PregnancyDuringART
Select
    PregnancyDuringART.PatientID ,
    PregnancyDuringART.PatientPK ,
    PregnancyDuringART.SiteCode,
    PregnancyDuringART.PregnantDuringART,
    PregnancyDuringART.LoadDate
FROM  PregnancyDuringART