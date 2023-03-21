IF OBJECT_ID(N'[ODS].[dbo].[Intermediate_LastestPrepAssessments]', N'U') IS NOT NULL
DROP TABLE [ODS].[dbo].[Intermediate_LastestPrepAssessments];
BEGIN

with source_data as (
    select
        row_number () over (partition by PatientPk, SiteCode order by VisitDate desc) As num,
        PatientPk,
        PatientPKHash,
        SiteCode,
        VisitDate,
        VisitID,
        SexPartnerHIVStatus,
        IsHIVPositivePartnerCurrentonART,
        IsPartnerHighrisk,
        PartnerARTRisk,
        ClientAssessments,
        ClientWillingToTakePrep,
        PrEPDeclineReason,
        RiskReductionEducationOffered,
        ReferralToOtherPrevServices,
        FirstEstablishPartnerStatus,
        PartnerEnrolledtoCCC,
        HIVPartnerCCCnumber,
        HIVPartnerARTStartDate,
        MonthsknownHIVSerodiscordant,
        SexWithoutCondom,
        NumberofchildrenWithPartner,
        ClientRisk,
        case
            when ClientRisk='Risk' then 1 else 0 end as EligiblePrep,
        VisitDate As AssessmentVisitDate,
        case
            when VisitDate is not null then 1 else 0 end as ScreenedPrep
    from ODS.dbo.PrEP_BehaviourRisk
)
select
    source_data.*,convert(nvarchar(64), hashbytes('SHA2_256', cast(HIVPartnerCCCnumber as varchar)), 2) as HIVPartnerCCCnumberHash,
    cast(getdate() as date) as LoadDate
into ODS.dbo.Intermediate_LastestPrepAssessments
from  source_data
where num = 1;

END