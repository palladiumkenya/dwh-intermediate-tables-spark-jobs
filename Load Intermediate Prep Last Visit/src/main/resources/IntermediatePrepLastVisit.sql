IF OBJECT_ID(N'[ODS].[dbo].[Intermediate_PrepLastVisit]', N'U') IS NOT NULL
DROP TABLE [ODS].[dbo].[Intermediate_PrepLastVisit];

BEGIN

with source_data as (
    select
        distinct row_number() over (partition by SiteCode, PatientPK order by VisitDate desc) as num,
                 PatientPk,
                 PatientPKHash,
                 SiteCode,
                 EncounterId,
                 VisitID,
                 VisitDate,
    month(VisitDate) As VisitMonth,
    year (VisitDate) As VisitYear,
    BloodPressure,
    Temperature,
    Weight,
    Height,
    BMI,
    STIScreening,
    STISymptoms,
    STITreated,
    Circumcised,
    VMMCReferral,
    LMP,
    MenopausalStatus,
    PregnantAtThisVisit,
    EDD,
    PlanningToGetPregnant,
    PregnancyPlanned,
    PregnancyEnded,
    PregnancyEndDate,
    PregnancyOutcome,
    BirthDefects,
    Breastfeeding,
    FamilyPlanningStatus,
    FPMethods,
    AdherenceDone,
    AdherenceOutcome,
    AdherenceReasons,
    SymptomsAcuteHIV,
    ContraindicationsPrep,
    PrepTreatmentPlan,
    PrepPrescribed,
    RegimenPrescribed,
    MonthsPrescribed,
    CondomsIssued,
    Tobegivennextappointment,
    Reasonfornotgivingnextappointment,
    HepatitisBPositiveResult,
    HepatitisCPositiveResult,
    VaccinationForHepBStarted,
    TreatedForHepB,
    VaccinationForHepCStarted,
    TreatedForHepC,
    NextAppointment,
    ClinicalNotes
from ODS.DBO.PrEP_Visits
where VisitDate is not null
    )
select
    source_data.*,cast( '' as nvarchar(100)) PatientPKHash,
    cast(getdate() as date) as LoadDate
into  [ODS].[dbo].[Intermediate_PrepLastVisit]
from  source_data
where num = 1;

END