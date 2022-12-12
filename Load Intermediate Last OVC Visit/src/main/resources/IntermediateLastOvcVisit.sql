truncate table dbo.Intermediate_LastOVCVisit;
with source_LasttOVCVisit as (
    select
        row_number() over (partition by PatientID, SiteCode, PatientPK, EMR order by VisitDate desc) as rank,
        VisitDate,
        PatientID ,
        SiteCode,
        PatientPK,
        EMR,
        VisitID,
        OVCEnrollmentDate,
        RelationshipToClient,
        EnrolledinCPIMS,
        CPIMSUniqueIdentifier,
        PartnerOfferingOVCServices,
        OVCExitReason,
        ExitDate
    from ODS.dbo.CT_Ovc
)
insert into dbo.Intermediate_LastOVCVisit
select
    VisitDate as LatestVisitDate,
    PatientID ,
    SiteCode,
    PatientPK,
    EMR,
    VisitDate,
    VisitID,
    OVCEnrollmentDate,
    RelationshipToClient,
    EnrolledinCPIMS,
    CPIMSUniqueIdentifier,
    PartnerOfferingOVCServices,
    OVCExitReason,
    ExitDate,
    cast(getdate() as date) as LoadDate
from source_LasttOVCVisit
where rank = 1