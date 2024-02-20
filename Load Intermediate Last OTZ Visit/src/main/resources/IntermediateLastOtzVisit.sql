IF OBJECT_ID(N'[ODS].[dbo].[Intermediate_LastOTZVisit]', N'U') IS NOT NULL
DROP TABLE [ODS].[dbo].[Intermediate_LastOTZVisit];
BEGIN
with source_LastOTZVisit as (
    select
        row_number() over (partition by SiteCode, PatientPK, EMR order by VisitDate desc) as rank,
        PatientID ,
        SiteCode,
        PatientPK,
        PatientPKHash,
        PatientIDHash,
        EMR,
        VisitDate,
        OTZEnrollmentDate,
        TransferInStatus,
        ModulesPreviouslyCovered,
        case when CHARINDEX('Orientation', ModulesCompletedToday) > 0 then 1 else 0 end as ModulesCompletedToday_OTZ_Orientation,
        case when CHARINDEX('Participation', ModulesCompletedToday) > 0 then 1 else 0 end as ModulesCompletedToday_OTZ_Participation,
        case when CHARINDEX('Leadership', ModulesCompletedToday) > 0 then 1 else 0 end as ModulesCompletedToday_OTZ_Leadership,
        case when CHARINDEX('Making Decisions', ModulesCompletedToday) > 0 then 1 else 0 end as ModulesCompletedToday_OTZ_MakingDecisions,
        case when CHARINDEX('Transition', ModulesCompletedToday) > 0 then 1 else 0 end as ModulesCompletedToday_OTZ_Transition,
        case when CHARINDEX('Treatment Literacy', ModulesCompletedToday) > 0 then 1 else 0 end as ModulesCompletedToday_OTZ_TreatmentLiteracy,
        case when CHARINDEX('SRH', ModulesCompletedToday) > 0 then 1 else 0 end as ModulesCompletedToday_OTZ_SRH,
        case when CHARINDEX('Beyon', ModulesCompletedToday) > 0 then 1 else 0 end as ModulesCompletedToday_OTZ_Beyond,
        SupportGroupInvolvement,
        Remarks,
        TransitionAttritionReason,
        OutcomeDate
    from ODS.dbo.CT_Otz
    WHERE  VOIDED=0
)
select

    SiteCode,
    PatientPK,
    PatientID,
    PatientPKHash,
    PatientIDHash,
    EMR,
    VisitDate as LastVisitDate,
    OTZEnrollmentDate,
    TransferInStatus,
    TransitionAttritionReason,
    ModulesPreviouslyCovered,
    ModulesCompletedToday_OTZ_Orientation,
    ModulesCompletedToday_OTZ_Participation,
    ModulesCompletedToday_OTZ_Leadership,
    ModulesCompletedToday_OTZ_MakingDecisions,
    ModulesCompletedToday_OTZ_Transition,
    ModulesCompletedToday_OTZ_TreatmentLiteracy,
    ModulesCompletedToday_OTZ_SRH,
    ModulesCompletedToday_OTZ_Beyond,
    cast(getdate() as date) as LoadDate
INTO [ODS].[dbo].[Intermediate_LastOTZVisit]
from source_LastOTZVisit
where rank = 1
END