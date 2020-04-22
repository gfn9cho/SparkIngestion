package edf

import edf.dataload.{now, timeZone, hiveDB, transformDB}
import org.joda.time.format.DateTimeFormat
import java.sql.Timestamp


package object recon {

    val datetime_format = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS")
    val nowString = datetime_format.withZone(timeZone).
                parseDateTime(now.toString("YYYY-MM-dd HH:mm:ss.sss"))
    val PV_ENDDATE = nowString.withTimeAtStartOfDay().
    minusMillis(1).toString("YYYY-MM-dd HH:mm:ss.SSS")
    val PV_STARTDATE = "1900-01-01 00:00:00"


  val gwplSourceQuery = s"""( SELECT
    CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
    status.TYPECODE as PolStatus,
    'TOTAL_INFORCE_'+ pol.ProductCode as AuditEntity,
    '1900-01-01' as AuditFrom,
    '$PV_ENDDATE'  as AuditThru,
    count(distinct polper.PolicyNumber) as AuditResult
    from
    dbo.pc_policyperiod polper join
    dbo.pc_policy pol on polper.PolicyID = pol.ID
    join dbo.pc_job job on polper.JobID = job.ID
    join dbo.pctl_policyperiodstatus status on polper.status = status.ID join
    dbo.pctl_job jtype on job.Subtype = jtype.ID
    where
    jtype.TYPECODE <> 'Cancellation' and
    (  status.TYPECODE = 'Bound'  AND
    ( '$PV_ENDDATE' BETWEEN Polper.PeriodStart AND Polper.PeriodEnd)) and
    job.CloseDate <=  '$PV_ENDDATE' and
    pol.ProductCode not in('BP7BusinessOwners','CA7CommAuto','CUCommercialUmbrella') and polper.retired=0
    group by status.TYPECODE, pol.ProductCode
    UNION ALL
    SELECT
    CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
    'Expired'  PolStatus,
    'TOTAL_EXPIRED' as AuditEntity,
    '1900-01-01' as AuditFrom,
    '$PV_ENDDATE'  as AuditThru,
    count(distinct polper.PolicyNumber) as AuditResult
    from
    dbo.pc_policyperiod polper join
    dbo.pc_policy pol on polper.PolicyID = pol.ID join
    dbo.pc_job job on polper.JobID = job.ID join
    dbo.pctl_policyperiodstatus status on polper.status = status.ID join
    dbo.pctl_job jtype on job.Subtype = jtype.ID
    where
    ((status.TYPECODE = 'Bound' AND JTYPE.TYPECODE IN ('Submission','Issuance','Renewal','Reinstatement','Rewrite','RewriteNewAccount','Cancellation' )) OR   (jtype.TYPECODE    IN  ('Submission','Issuance','Renewal','Reinstatement','Rewrite','RewriteNewAccount','Cancellation') AND status.TYPECODE IN ( 'Quoted' , 'Bound' , 'Canceling'))) AND
    Polper.PeriodEnd <=  '$PV_ENDDATE' AND
    ISNULL(polper.PolicyNumber,'')  <> '' and
    job.CloseDate <= '$PV_ENDDATE' and
    pol.ProductCode not in('BP7BusinessOwners','CA7CommAuto','CUCommercialUmbrella') and polper.retired=0
    UNION ALL
    SELECT
    CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
    status.TYPECODE as PolStatus,
    'TOTAL_CANCELLED' as AuditEntity,
    '1900-01-01' as AuditFrom,
    '$PV_ENDDATE'  as AuditThru,
    count(distinct polper.PolicyNumber) as AuditResult
    from
    dbo.pc_policyperiod polper join
    dbo.pc_policy pol on polper.PolicyID = pol.ID join
    dbo.pc_job job on polper.JobID = job.ID join
    dbo.pctl_policyperiodstatus status on polper.status = status.ID join
    dbo.pctl_job jtype on job.Subtype = jtype.ID
    where
    status.TYPECODE = 'Bound' and
    jtype.TYPECODE   = 'Cancellation' and
    polper.CancellationDate <= '$PV_ENDDATE' and
    job.CloseDate <= '$PV_ENDDATE' and
    pol.ProductCode not in('BP7BusinessOwners','CA7CommAuto','CUCommercialUmbrella') and polper.retired=0
    group by status.TYPECODE
    UNION ALL
    SELECT
    CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
    'Bound' as PolStatus,
    'YTD_PA_PREM' as AuditEntity,
    cast(cast(YEAR('$PV_ENDDATE') as varchar)+'-01-01' as datetime2) as AuditFrom,
    '$PV_ENDDATE'  as AuditThru,
    coalesce(sum(trans.Amountbilling),0) as AuditResult
    from
    dbo.pc_patransaction trans join
    dbo.pc_pacost cost on trans.PACost = cost.ID join
    dbo.pc_policyperiod polper on trans.BranchID = polper.ID join
    dbo.pc_job job on polper.jobid = job.id join
    dbo.pctl_chargepattern charge on cost.chargepattern = charge.id join
    dbo.pctl_policyperiodstatus status on polper.Status = status.ID
    where
    status.TYPECODE  IN ('Bound','AuditComplete') AND
    job.CloseDate <= '$PV_ENDDATE' and
    charge.TYPECODE IN ('Premium','MCCA') and
    cost.FeeName IS NULL and
    YEAR(trans.WrittenDate) = YEAR('$PV_ENDDATE') and MONTH(trans.WrittenDate) <= MONTH('$PV_ENDDATE')
    UNION ALL
    SELECT
    CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
    'Bound' as PolStatus,
    'YTD_HO_PREM' as AuditEntity,
    cast(cast(YEAR('$PV_ENDDATE') as varchar)+'-01-01' as datetime2) as AuditFrom,
    '$PV_ENDDATE'  as AuditThru,
    coalesce(sum(trans.Amountbilling),0) as AuditResult
    from
    dbo.pcx_hotransaction_hoe trans join
    dbo.pcx_homeownerscost_hoe cost on trans.HomeownersCost = cost.ID join
    dbo.pc_policyperiod polper on trans.BranchID = polper.ID join
    dbo.pc_job job on polper.jobid = job.id  join
    dbo.pctl_chargepattern charge on cost.chargepattern = charge.id join
    dbo.pctl_policyperiodstatus status on polper.Status = status.ID
    where
    status.TYPECODE  IN ('Bound','AuditComplete') AND
    job.CloseDate <= '$PV_ENDDATE' and
    charge.TYPECODE = 'Premium' and
    YEAR(trans.WrittenDate) = YEAR('$PV_ENDDATE') and MONTH(trans.WrittenDate) <= MONTH('$PV_ENDDATE')
    UNION ALL
    SELECT
    CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
   'Bound' as PolStatus,
    'YTD_PUP_PREM' as AuditEntity,
    cast(cast(YEAR('$PV_ENDDATE') as varchar)+'-01-01' as datetime2) as AuditFrom,
    '$PV_ENDDATE'  as AuditThru, coalesce(sum(trans.Amountbilling),0) as AuditResult
    from
    dbo.pcx_puptransaction_pue trans join
    dbo.pcx_pupcost_pue cost on trans.PUPCost = cost.ID join
    dbo.pc_policyperiod polper on trans.BranchID = polper.ID join
    dbo.pc_job job on polper.jobid = job.id  join
    dbo.pctl_chargepattern charge on cost.chargepattern = charge.id join
    dbo.pctl_policyperiodstatus status on polper.Status = status.ID
    where
    status.TYPECODE  IN ('Bound','AuditComplete')  AND
    job.CloseDate <= '$PV_ENDDATE' and
    charge.TYPECODE = 'Premium' and
    YEAR(trans.WrittenDate) = YEAR('$PV_ENDDATE') and MONTH(trans.WrittenDate) <= MONTH('$PV_ENDDATE')
    UNION ALL
    SELECT
    CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
    'Bound' as PolStatus,
    'ALL_PA_PREM' as AuditEntity,
    '1900-01-01' as AuditFrom,
    '$PV_ENDDATE'  as AuditThru,
    coalesce(sum(trans.Amountbilling),0) as AuditResult
    from dbo.pc_patransaction trans join
    dbo.pc_pacost cost on trans.PACost = cost.ID join
    dbo.pc_policyperiod polper on trans.BranchID = polper.ID join
    dbo.pc_job job on polper.jobid = job.id join
    dbo.pctl_chargepattern charge on cost.chargepattern = charge.id join
    dbo.pctl_policyperiodstatus status on polper.Status = status.ID
    where
    status.TYPECODE  IN ('Bound','AuditComplete') AND
    job.CloseDate <= '$PV_ENDDATE' and
    charge.TYPECODE IN ('Premium','MCCA') and
    cost.FeeName IS NULL and
    trans.WrittenDate <= '$PV_ENDDATE'
    UNION ALL
    SELECT
    CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
    'Bound' as PolStatus,
    'ALL_HO_PREM' as AuditEntity,
    '1900-01-01' as AuditFrom,
    '$PV_ENDDATE'  as AuditThru,
    coalesce(sum(trans.Amountbilling),0) as AuditResult
    from
    dbo.pcx_hotransaction_hoe trans join
    dbo.pcx_homeownerscost_hoe cost on trans.HomeownersCost = cost.ID join
    dbo.pc_policyperiod polper on trans.BranchID = polper.ID join
    dbo.pc_job job on polper.jobid = job.id  join
    dbo.pctl_chargepattern charge on cost.chargepattern = charge.id join
    dbo.pctl_policyperiodstatus status on polper.Status = status.ID
    where
    status.TYPECODE  IN ('Bound','AuditComplete') AND
    job.CloseDate <= '$PV_ENDDATE' and
    charge.TYPECODE = 'Premium' and
    trans.WrittenDate <= '$PV_ENDDATE' and
    polper.retired=0
    UNION ALL
    SELECT
    CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
    'Bound' as PolStatus,
    'ALL_PUP_PREM' as AuditEntity, '1900-01-01' as AuditFrom,
    '$PV_ENDDATE'  as AuditThru,
    coalesce(sum(trans.Amountbilling),0) as AuditResult
    from dbo.pcx_puptransaction_pue trans join
    dbo.pcx_pupcost_pue cost on trans.PUPCost = cost.ID join
    dbo.pc_policyperiod polper on trans.BranchID = polper.ID join
    dbo.pc_job job on polper.jobid = job.id  join
    dbo.pctl_chargepattern charge on cost.chargepattern = charge.id join
    dbo.pctl_policyperiodstatus status on polper.Status = status.ID
    where
    status.TYPECODE  IN ('Bound','AuditComplete')  AND
    job.CloseDate <= '$PV_ENDDATE' and
    charge.TYPECODE = 'Premium' and
    trans.WrittenDate <= '$PV_ENDDATE' ) a"""
  
  val gwplLakeQuery = s"""SELECT 
                         |cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                         |polper.pctl_policyperiodstatus_typecode as PolStatus,
                         |'TOTAL_INFORCE_'||pol.ProductCode as AuditEntity, 
                         |CAST('1900-01-01' as date) as AuditFrom, 
                         |cast('$PV_ENDDATE' as timestamp) as AuditThru,
                         |count(distinct polper.PolicyNumber) as AuditResult
                         |from 
                         |$hiveDB.stg_gwpl_pc_policyperiod polper join 
                         |$hiveDB.stg_gwpl_pc_policy pol on polper.PolicyID = pol.ID join 
                         |$hiveDB.stg_gwpl_pc_job job on polper.JobID = job.ID
                         |where 
                         |pctl_job_typecode <> 'Cancellation' and 
                         |(polper.pctl_policyperiodstatus_typecode = 'Bound' and 
                         |( cast('$PV_ENDDATE' as timestamp) BETWEEN Polper.PeriodStart and Polper.PeriodEnd)) and
                         |job.CloseDate <= cast('$PV_ENDDATE' as timestamp) and
                         |pol.ProductCode not in('BP7BusinessOwners','CA7CommAuto','CUCommercialUmbrella') 
                         |group by polper.pctl_policyperiodstatus_typecode, pol.ProductCode 
                         |UNION ALL 
                         |SELECT 
                         |cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                         |'Expired' PolStatus,
                         |'TOTAL_EXPIRED' as AuditEntity, 
                         |CAST('1900-01-01' as date) as AuditFrom, 
                         |cast('$PV_ENDDATE' as timestamp) as AuditThru,
                         |count(distinct polper.PolicyNumber) as AuditResult
                         |from 
                         |$hiveDB.stg_gwpl_pc_policyperiod polper join 
                         |$hiveDB.stg_gwpl_pc_policy pol on polper.PolicyID = pol.ID join 
                         |$hiveDB.stg_gwpl_pc_job job on polper.JobID = job.ID
                         | where 
                         |((polper.pctl_policyperiodstatus_typecode = 'Bound' and 
                         |pctl_job_typecode IN ('Submission','Issuance','Renewal','Reinstatement','Rewrite','RewriteNewAccount','Cancellation' )) OR (pctl_job_typecode IN ('Submission','Issuance','Renewal','Reinstatement','Rewrite','RewriteNewAccount','Cancellation') and 
                         |polper.pctl_policyperiodstatus_typecode IN ( 'Quoted' , 'Bound' , 'Canceling'))) and 
                         |pol.ProductCode not in('BP7BusinessOwners','CA7CommAuto','CUCommercialUmbrella') and 
                         |Polper.PeriodEnd <= cast('$PV_ENDDATE' as timestamp) and
                         |COALESCE(polper.PolicyNumber,'') <> '' and 
                         |job.CloseDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION ALL 
                         |SELECT 
                         |cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                         |polper.pctl_policyperiodstatus_typecode as PolStatus,
                         |'TOTAL_CANCELLED' as AuditEntity, 
                         |CAST('1900-01-01' as date) as AuditFrom, 
                         |cast('$PV_ENDDATE' as timestamp) as AuditThru,
                         |count(distinct polper.PolicyNumber) as AuditResult
                         |from 
                         |$hiveDB.stg_gwpl_pc_policyperiod polper join 
                         |$hiveDB.stg_gwpl_pc_policy pol on polper.PolicyID = pol.ID join 
                         |$hiveDB.stg_gwpl_pc_job job on polper.JobID = job.ID
                         |where 
                         |polper.pctl_policyperiodstatus_typecode = 'Bound' and 
                         |pctl_job_typecode = 'Cancellation' and 
                         |polper.CancellationDate <= cast('$PV_ENDDATE' as timestamp) and
                         |job.CloseDate <= cast('$PV_ENDDATE' as timestamp) and
                         |pol.ProductCode not in('BP7BusinessOwners','CA7CommAuto','CUCommercialUmbrella') 
                         |group by polper.pctl_policyperiodstatus_typecode
                         |UNION ALL 
                         |SELECT 
                         |cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                         |'Bound' as PolStatus,
                         |'YTD_PA_PREM' as AuditEntity, 
                         |CAST('1900-01-01' as date) as AuditFrom,
                         |cast('$PV_ENDDATE' as timestamp) as AuditThru,
                         |coalesce(sum(trans.Amountbilling),0) as AuditResult
                         |from 
                         |$hiveDB.stg_gwpl_pc_patransaction trans join 
                         |$hiveDB.stg_gwpl_pc_pacost cost on trans.PACost = cost.ID join 
                         |$hiveDB.stg_gwpl_pc_policyperiod polper on trans.BranchID = polper.ID join 
                         |$hiveDB.stg_gwpl_pc_job job on polper.jobid = job.id  
                         |where 
                         |polper.pctl_policyperiodstatus_typecode IN ('Bound','AuditComplete') and 
                         |job.CloseDate <= cast('$PV_ENDDATE' as timestamp) and
                         |pctl_chargepattern_typecode IN ('Premium','MCCA') and 
                         |cost.FeeName IS NULL and 
                         |YEAR(trans.WrittenDate) = YEAR(cast('$PV_ENDDATE' as timestamp)) and
                         |MONTH(trans.WrittenDate) <= MONTH(cast('$PV_ENDDATE' as timestamp))
                         |UNION ALL 
                         |SELECT 
                         |cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                         |'Bound' as PolStatus,
                         |'YTD_HO_PREM' as AuditEntity, 
                         |CAST('1900-01-01' as date) as AuditFrom,
                         |cast('$PV_ENDDATE' as timestamp) as AuditThru,
                         |coalesce(sum(trans.Amountbilling),0) as AuditResult
                         |from 
                         |$hiveDB.stg_gwpl_pcx_hotransaction_hoe trans join 
                         |$hiveDB.stg_gwpl_pcx_homeownerscost_hoe cost on trans.HomeownersCost = cost.ID join 
                         |$hiveDB.stg_gwpl_pc_policyperiod polper on trans.BranchID = polper.ID join 
                         |$hiveDB.stg_gwpl_pc_job job on polper.jobid = job.id 
                         |where 
                         |polper.pctl_policyperiodstatus_typecode IN ('Bound','AuditComplete') and 
                         |job.CloseDate <= cast('$PV_ENDDATE' as timestamp) and
                         |pctl_chargepattern_typecode = 'Premium' and 
                         |YEAR(trans.WrittenDate) = YEAR(cast('$PV_ENDDATE' as timestamp)) and
                         |MONTH(trans.WrittenDate) <= MONTH(cast('$PV_ENDDATE' as timestamp))
                         |UNION ALL 
                         |SELECT 
                         |cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                         |'Bound' as PolStatus,
                         |'YTD_PUP_PREM' as AuditEntity, 
                         |CAST('1900-01-01' as date) as AuditFrom,
                         |cast('$PV_ENDDATE' as timestamp) as AuditThru,
                         |coalesce(sum(trans.Amountbilling),0) as AuditResult
                         |from 
                         |$hiveDB.stg_gwpl_pcx_puptransaction_pue trans join 
                         |$hiveDB.stg_gwpl_pcx_pupcost_pue cost on trans.PUPCost = cost.ID join 
                         |$hiveDB.stg_gwpl_pc_policyperiod polper on trans.BranchID = polper.ID join 
                         |$hiveDB.stg_gwpl_pc_job job on polper.jobid = job.id 
                         |WHERE 
                         |polper.pctl_policyperiodstatus_typecode IN ('Bound','AuditComplete') and 
                         |job.CloseDate <= cast('$PV_ENDDATE' as timestamp) and
                         |pctl_chargepattern_typecode = 'Premium' and 
                         |YEAR(trans.WrittenDate) = YEAR(cast('$PV_ENDDATE' as timestamp)) and
                         |MONTH(trans.WrittenDate) <= MONTH(cast('$PV_ENDDATE' as timestamp))
                         |UNION ALL 
                         |SELECT 
                         |cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                         |'Bound' as PolStatus,
                         |'ALL_PA_PREM' as AuditEntity, 
                         |CAST('1900-01-01' as date) as AuditFrom,
                         |cast('$PV_ENDDATE' as timestamp) as AuditThru,
                         |coalesce(sum(trans.Amountbilling),0) as AuditResult
                         |from 
                         |$hiveDB.stg_gwpl_pc_patransaction trans join 
                         |$hiveDB.stg_gwpl_pc_pacost cost on trans.PACost = cost.ID join 
                         |$hiveDB.stg_gwpl_pc_policyperiod polper on trans.BranchID = polper.ID join 
                         |$hiveDB.stg_gwpl_pc_job job on polper.jobid = job.id 
                         |where 
                         |polper.pctl_policyperiodstatus_typecode IN ('Bound','AuditComplete') and 
                         |job.CloseDate <= cast('$PV_ENDDATE' as timestamp) and
                         |pctl_chargepattern_typecode IN ('Premium','MCCA') and 
                         |cost.FeeName IS NULL and 
                         |trans.WrittenDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION ALL 
                         |SELECT 
                         |cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                         |'Bound' as PolStatus,
                         |'ALL_HO_PREM' as AuditEntity, 
                         |CAST('1900-01-01' as date) as AuditFrom,
                         |cast('$PV_ENDDATE' as timestamp) as AuditThru,
                         |coalesce(sum(trans.Amountbilling),0) as AuditResult
                         |from 
                         |$hiveDB.stg_gwpl_pcx_hotransaction_hoe trans join 
                         |$hiveDB.stg_gwpl_pcx_homeownerscost_hoe cost on trans.HomeownersCost = cost.ID join 
                         |$hiveDB.stg_gwpl_pc_policyperiod polper on trans.BranchID = polper.ID join 
                         |$hiveDB.stg_gwpl_pc_job job on polper.jobid = job.id 
                         |where 
                         |polper.pctl_policyperiodstatus_typecode IN ('Bound','AuditComplete') and 
                         |job.CloseDate <= cast('$PV_ENDDATE' as timestamp) and
                         |pctl_chargepattern_typecode = 'Premium' and 
                         |trans.WrittenDate <= cast('$PV_ENDDATE' as timestamp) and
                         |polper.retired=0 
                         |UNION ALL 
                         |SELECT 
                         |cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                         |'Bound' as PolStatus,
                         |'ALL_PUP_PREM' as AuditEntity,
                         |CAST('1900-01-01' as date) as AuditFrom,
                         |cast('$PV_ENDDATE' as timestamp) as AuditThru,
                         |coalesce(sum(trans.Amountbilling),0) as AuditResult
                         |from 
                         |$hiveDB.stg_gwpl_pcx_puptransaction_pue trans join 
                         |$hiveDB.stg_gwpl_pcx_pupcost_pue cost on trans.PUPCost = cost.ID join 
                         |$hiveDB.stg_gwpl_pc_policyperiod polper on trans.BranchID = polper.ID join 
                         |$hiveDB.stg_gwpl_pc_job job on polper.jobid = job.id 
                         |where 
                         |polper.pctl_policyperiodstatus_typecode IN ('Bound','AuditComplete') and 
                         |job.CloseDate <= cast('$PV_ENDDATE' as timestamp) and
                         |pctl_chargepattern_typecode = 'Premium' and 
                         |trans.WrittenDate <= cast('$PV_ENDDATE' as timestamp)""".stripMargin
  val gwclSourceQuery = s"""(SELECT
                           |CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
                           |status.TYPECODE as PolStatus,
                           |'TOTAL_INFORCE_'+pol.ProductCode as AuditEntity,
                           |'1900-01-01' as AuditFrom,
                           |'$PV_ENDDATE'  as AuditThru,
                           |count(distinct polper.PolicyNumber) as AuditResult
                           |from
                           |dbo.pc_policyperiod polper  join
                           |dbo.pc_policy pol on polper.PolicyID = pol.ID  join
                           |dbo.pc_job job on polper.JobID = job.ID  join
                           |dbo.pctl_policyperiodstatus status on polper.status = status.ID  join
                           |dbo.pctl_job jtype on job.Subtype = jtype.ID
                           |where
                           |jtype.TYPECODE <> 'Cancellation' and
                           |  (  status.TYPECODE = 'Bound'  and
                           | ( '$PV_ENDDATE' BETWEEN Polper.PeriodStart and
                           | Polper.PeriodEnd)) and
                           | job.CloseDate <=  '$PV_ENDDATE'
                           |group by status.TYPECODE,pol.ProductCode
                           |UNION ALL
                           |SELECT
                           | CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
                           | 'Expired'  PolStatus,
                           | 'TOTAL_EXPIRED' as AuditEntity,
                           | '1900-01-01' as AuditFrom,
                           | '$PV_ENDDATE'  as AuditThru,
                           | count(distinct polper.PolicyNumber) as AuditResult
                           |from
                           |dbo.pc_policyperiod polper  join
                           |dbo.pc_job job on polper.JobID = job.ID  join
                           |dbo.pctl_policyperiodstatus status on polper.status = status.ID  join
                           |dbo.pctl_job jtype on job.Subtype = jtype.ID
                           |where
                           |((status.TYPECODE = 'Bound' and
                           | JTYPE.TYPECODE IN ('Submission',
                           |'Issuance',
                           |'Renewal',
                           |'Reinstatement',
                           |'Rewrite',
                           |'RewriteNewAccount',
                           |'Cancellation' )) OR   (jtype.TYPECODE    IN  ('Submission',
                           |'Issuance',
                           |'Renewal',
                           |'Reinstatement',
                           |'Rewrite',
                           |'RewriteNewAccount',
                           |'Cancellation') and
                           | status.TYPECODE IN ( 'Quoted' ,
                           | 'Bound' ,
                           | 'Canceling'))) and
                           | Polper.PeriodEnd <=  '$PV_ENDDATE' and
                           | ISNULL(polper.PolicyNumber,
                           |'')  <> '' and
                           | job.CloseDate <= '$PV_ENDDATE'
                           |UNION ALL
                           | SELECT
                           | CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
                           | status.TYPECODE as PolStatus,
                           |'TOTAL_CANCELLED' as AuditEntity,
                           | '1900-01-01' as AuditFrom,
                           | '$PV_ENDDATE'  as AuditThru,
                           | count(distinct polper.PolicyNumber) as AuditResult
                           |from
                           |dbo.pc_policyperiod polper  join
                           |dbo.pc_job job on polper.JobID = job.ID  join
                           |dbo.pctl_policyperiodstatus status on polper.status = status.ID  join
                           |dbo.pctl_job jtype on job.Subtype = jtype.ID
                           |where
                           |status.TYPECODE = 'Bound' and
                           |  jtype.TYPECODE   = 'Cancellation' and
                           | polper.CancellationDate <= '$PV_ENDDATE' and
                           | job.CloseDate <= '$PV_ENDDATE'
                           |group by status.TYPECODE
                           |UNION ALL
                           | SELECT CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
                           | 'Bound' as PolStatus,
                           | 'YTD_CA_PREM' as AuditEntity,
                           |  cast(cast(YEAR('$PV_ENDDATE') as varchar)+'-01-01' as datetime2) as AuditFrom,
                           | '$PV_ENDDATE'  as AuditThru,
                           | coalesce(sum(trans.Amountbilling),
                           |0) as AuditResult
                           |from
                           | dbo.pcx_ca7transaction trans  join
                           | dbo.pcx_ca7cost cost on trans.CA7Cost = cost.ID  join
                           | dbo.pc_policyperiod polper_orig on cost.BranchID = polper_orig.ID  join
                           | dbo.pc_policyperiod polper on trans.BranchID = polper.ID  join
                           | dbo.pc_job job on polper.JobID = job.ID  left  join
                           | dbo.pctl_chargepattern charge on cost.ChargePattern = charge.ID  join
                           | dbo.pctl_policyperiodstatus status on polper.Status = status.ID
                           |where
                           | status.TYPECODE  IN ('Bound',
                           |'AuditComplete') and
                           | job.CloseDate <= '$PV_ENDDATE' and
                           | charge.TYPECODE IN ('Premium',
                           |'MCCA') and
                           | cost.feename IS NULL and
                           | YEAR(trans.WrittenDate) = YEAR('$PV_ENDDATE') and
                           | MONTH(trans.WrittenDate) <= MONTH('$PV_ENDDATE')
                           |UNION ALL
                           | SELECT CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
                           | 'Bound' as PolStatus,
                           | 'YTD_BOP_PREM' as AuditEntity,
                           | cast(cast(YEAR('$PV_ENDDATE') as varchar)+'-01-01' as datetime2)  as AuditFrom,
                           | '$PV_ENDDATE'  as AuditThru,
                           | coalesce(sum(trans.Amountbilling),0) as AuditResult
                           |from
                           |dbo.pcx_bp7transaction trans  join
                           |dbo.pcx_bp7cost cost on trans.BP7Cost = cost.ID   join
                           |dbo.pc_policyperiod polper on trans.BranchID = polper.ID  join
                           |dbo.pc_job job on polper.JobID = job.ID  left  join
                           | dbo.pctl_chargepattern charge on cost.ChargePattern = charge.ID  join
                           | dbo.pctl_policyperiodstatus status on polper.Status = status.ID
                           |where
                           | status.TYPECODE  IN ('Bound',
                           |'AuditComplete') and
                           | job.CloseDate <= '$PV_ENDDATE' and
                           | charge.TYPECODE = 'Premium' and
                           | YEAR(trans.WrittenDate) = YEAR('$PV_ENDDATE') and
                           | MONTH(trans.WrittenDate) <= MONTH('$PV_ENDDATE')
                           |UNION ALL
                           | SELECT CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
                           | 'Bound' as PolStatus,
                           | 'YTD_CUP_PREM' as AuditEntity,
                           | cast(cast(YEAR('$PV_ENDDATE') as varchar)+'-01-01' as datetime2)  as AuditFrom,
                           | '$PV_ENDDATE'  as AuditThru,
                           |coalesce(sum(trans.Amountbilling),0) as AuditResult
                           |from
                           |dbo.pcx_cutransaction trans  join
                           |dbo.pcx_cucost cost on trans.CUCost = cost.ID   join
                           |dbo.pc_policyperiod polper on trans.BranchID = polper.ID  join
                           |dbo.pc_job job on polper.JobID = job.ID  left  join
                           | dbo.pctl_chargepattern charge on cost.ChargePattern = charge.ID  join
                           | dbo.pctl_policyperiodstatus status on polper.Status = status.ID
                           |where
                           | status.TYPECODE  IN ('Bound',
                           |'AuditComplete') and
                           | job.CloseDate <= '$PV_ENDDATE' and
                           | charge.TYPECODE = 'Premium' and
                           | YEAR(trans.WrittenDate) = YEAR('$PV_ENDDATE') and
                           | MONTH(trans.WrittenDate) <= MONTH('$PV_ENDDATE')
                           |UNION ALL
                           | SELECT CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
                           | 'Bound' as PolStatus,
                           | 'ALL_CA_PREM' as AuditEntity,
                           | '1900-01-01' as AuditFrom,
                           | '$PV_ENDDATE'  as AuditThru,
                           | coalesce(sum(trans.Amountbilling),0) as AuditResult
                           |from  dbo.pcx_ca7transaction trans  join
                           | dbo.pcx_ca7cost cost on trans.CA7Cost = cost.ID  join
                           | dbo.pc_policyperiod polper_orig on cost.BranchID = polper_orig.ID  join
                           | dbo.pc_policyperiod polper on trans.BranchID = polper.ID  join
                           | dbo.pc_job job on polper.JobID = job.ID  left  join
                           | dbo.pctl_chargepattern charge on cost.ChargePattern = charge.ID  join
                           | dbo.pctl_policyperiodstatus status on polper.Status = status.ID
                           |where
                           | status.TYPECODE  IN ('Bound','AuditComplete') and
                           | job.CloseDate <= '$PV_ENDDATE' and
                           | charge.TYPECODE IN ('Premium','MCCA') and
                           | cost.feename IS NULL and
                           | trans.WrittenDate <= '$PV_ENDDATE'
                           |UNION ALL
                           | SELECT CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
                           | 'Bound' as PolStatus,
                           | 'ALL_BOP_PREM' as AuditEntity,
                           | '1900-01-01' as AuditFrom,
                           | '$PV_ENDDATE'  as AuditThru,
                           | coalesce(sum(trans.Amountbilling),0) as AuditResult
                           |from
                           |dbo.pcx_bp7transaction trans  join
                           |dbo.pcx_bp7cost cost on trans.BP7Cost = cost.ID   join
                           |dbo.pc_policyperiod polper on trans.BranchID = polper.ID  join
                           |dbo.pc_job job on polper.JobID = job.ID  left  join
                           | dbo.pctl_chargepattern charge on cost.ChargePattern = charge.ID  join
                           | dbo.pctl_policyperiodstatus status on polper.Status = status.ID
                           |where
                           | status.TYPECODE  IN ('Bound','AuditComplete') and
                           | job.CloseDate <= '$PV_ENDDATE' and
                           | charge.TYPECODE = 'Premium' and
                           | trans.WrittenDate <= '$PV_ENDDATE'
                           |UNION ALL
                           | SELECT CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
                           | 'Bound' as PolStatus,
                           | 'ALL_CUP_PREM' as AuditEntity,
                           | '1900-01-01' as AuditFrom,
                           | '$PV_ENDDATE'  as AuditThru,
                           |coalesce(sum(trans.Amountbilling),0) as AuditResult
                           |from
                           |dbo.pcx_cutransaction trans  join
                           |dbo.pcx_cucost cost on trans.CUCost = cost.ID   join
                           |dbo.pc_policyperiod polper on trans.BranchID = polper.ID  join
                           |dbo.pc_job job on polper.JobID = job.ID  left  join
                           | dbo.pctl_chargepattern charge on cost.ChargePattern = charge.ID  join
                           | dbo.pctl_policyperiodstatus status on polper.Status = status.ID
                           |where
                           | status.TYPECODE  IN ('Bound','AuditComplete') and
                           | job.CloseDate <= '$PV_ENDDATE' and
                           | charge.TYPECODE = 'Premium' and
                           | trans.WrittenDate <= '$PV_ENDDATE' ) q""".stripMargin
val gwclLakeQuery = s"""SELECT 
                       |cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                       | polper.pctl_policyperiodstatus_typecode AS PolStatus,
                       | 'TOTAL_INFORCE_'||pol.ProductCode AS AuditEntity,
                       | count(distinct polper.PolicyNumber) AS AuditResult
                       |from 
                       | $hiveDB.stg_gwcl_pc_policyperiod polper join 
                       | $hiveDB.stg_gwcl_pc_policy pol ON polper.PolicyID = pol.ID join 
                       | $hiveDB.stg_gwcl_pc_job job ON polper.JobID = job.ID 
                       |where 
                       | pctl_job_typecode <> 'Cancellation' AND (polper.pctl_policyperiodstatus_typecode = 'Bound' AND ( cast('$PV_ENDDATE' as timestamp) BETWEEN Polper.PeriodStart AND Polper.PeriodEnd)) AND job.CloseDate <= cast('$PV_ENDDATE' as timestamp)
                       | GROUP BY  polper.pctl_policyperiodstatus_typecode, pol.ProductCode
                       |UNION ALL 
                       |SELECT 
                       | cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                       | 'Expired' PolStatus,
                       | 'TOTAL_EXPIRED' AS AuditEntity,
                       | count(distinct polper.PolicyNumber) AS AuditResult
                       |from $hiveDB.stg_gwcl_pc_policyperiod polper join 
                       | $hiveDB.stg_gwcl_pc_job job ON polper.JobID = job.ID
                       |where 
                       | ((polper.pctl_policyperiodstatus_typecode = 'Bound' AND pctl_job_typecode IN ('Submission',
                       |'Issuance','Renewal','Reinstatement','Rewrite','RewriteNewAccount',
                       |'Cancellation' )) OR 
                       |(pctl_job_typecode IN ('Submission','Issuance','Renewal','Reinstatement','Rewrite','RewriteNewAccount','Cancellation') AND polper.pctl_policyperiodstatus_typecode IN ( 'Quoted' ,'Bound' ,'Canceling'))) AND 
                       | Polper.PeriodEnd <= cast('$PV_ENDDATE' as timestamp) AND
                       | COALESCE(polper.PolicyNumber,'') <> '' AND 
                       | job.CloseDate <= cast('$PV_ENDDATE' as timestamp)
                       |UNION ALL 
                       |SELECT 
                       |cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                       | polper.pctl_policyperiodstatus_typecode AS PolStatus,
                       | 'TOTAL_CANCELLED' AS AuditEntity,
                       | count(distinct polper.PolicyNumber) AS AuditResult
                       |from 
                       | $hiveDB.stg_gwcl_pc_policyperiod polper join 
                       | $hiveDB.stg_gwcl_pc_job job ON polper.JobID = job.ID
                       |where 
                       | polper.pctl_policyperiodstatus_typecode = 'Bound' AND 
                       | pctl_job_typecode = 'Cancellation' AND 
                       | polper.CancellationDate <= cast('$PV_ENDDATE' as timestamp) AND
                       | job.CloseDate <= cast('$PV_ENDDATE' as timestamp)
                       |GROUP BY  polper.pctl_policyperiodstatus_typecode
                       |UNION ALL 
                       |SELECT 
                       | cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                       | 'Bound' AS PolStatus,
                       | 'YTD_CA_PREM' AS AuditEntity,
                       | coalesce(sum(trans.Amountbilling),0) AS AuditResult
                       |from 
                       | $hiveDB.stg_gwcl_pcx_ca7transaction trans join 
                       | $hiveDB.stg_gwcl_pcx_ca7cost cost ON trans.CA7Cost = cost.ID join 
                       | $hiveDB.stg_gwcl_pc_policyperiod polper ON trans.BranchID = polper.ID join 
                       | $hiveDB.stg_gwcl_pc_job job ON polper.JobID = job.ID 
                       |where 
                       | polper.pctl_policyperiodstatus_typecode IN ('Bound','AuditComplete') AND 
                       | job.CloseDate <= cast('$PV_ENDDATE' as timestamp) AND
                       | pctl_chargepattern_typecode IN ('Premium','MCCA') AND 
                       | cost.feename IS NULL AND 
                       | YEAR(trans.WrittenDate) = YEAR(cast('$PV_ENDDATE' as timestamp)) AND
                       | MONTH(trans.WrittenDate) <= MONTH(cast('$PV_ENDDATE' as timestamp))
                       |UNION ALL 
                       |SELECT 
                       | cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                       | 'Bound' AS PolStatus,
                       | 'YTD_BOP_PREM' AS AuditEntity,
                       | coalesce(sum(trans.Amountbilling),0) AS AuditResult
                       |from 
                       | $hiveDB.stg_gwcl_pcx_bp7transaction trans join 
                       | $hiveDB.stg_gwcl_pcx_bp7cost cost ON trans.BP7Cost = cost.ID join 
                       | $hiveDB.stg_gwcl_pc_policyperiod polper ON trans.BranchID = polper.ID join 
                       | $hiveDB.stg_gwcl_pc_job job ON polper.JobID = job.ID 
                       |where 
                       | polper.pctl_policyperiodstatus_typecode IN ('Bound','AuditComplete') AND 
                       | job.CloseDate <= cast('$PV_ENDDATE' as timestamp) AND
                       | pctl_chargepattern_typecode = 'Premium' AND 
                       | YEAR(trans.WrittenDate) = YEAR(cast('$PV_ENDDATE' as timestamp)) AND
                       | MONTH(trans.WrittenDate) <= MONTH(cast('$PV_ENDDATE' as timestamp))
                       |UNION ALL 
                       |SELECT 
                       |cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                       | 'Bound' AS PolStatus,
                       | 'ALL_CA_PREM' AS AuditEntity,
                       | coalesce(sum(trans.Amountbilling),0) AS AuditResult
                       |from 
                       | $hiveDB.stg_gwcl_pcx_ca7transaction trans join 
                       | $hiveDB.stg_gwcl_pcx_ca7cost cost ON trans.CA7Cost = cost.ID join 
                       | $hiveDB.stg_gwcl_pc_policyperiod polper ON trans.BranchID = polper.ID join 
                       | $hiveDB.stg_gwcl_pc_job job ON polper.JobID = job.ID 
                       |where 
                       | polper.pctl_policyperiodstatus_typecode IN ('Bound','AuditComplete') AND 
                       | job.CloseDate <= cast('$PV_ENDDATE' as timestamp) AND
                       | pctl_chargepattern_typecode IN ('Premium','MCCA') AND 
                       | cost.feename IS NULL AND 
                       | trans.WrittenDate <= cast('$PV_ENDDATE' as timestamp)
                       |UNION ALL 
                       |SELECT 
                       |cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                       | 'Bound' AS PolStatus,
                       | 'ALL_BOP_PREM' AS AuditEntity,
                       | coalesce(sum(trans.Amountbilling),0) AS AuditResult
                       |from 
                       | $hiveDB.stg_gwcl_pcx_bp7transaction trans join 
                       | $hiveDB.stg_gwcl_pcx_bp7cost cost ON trans.BP7Cost = cost.ID join 
                       | $hiveDB.stg_gwcl_pc_policyperiod polper ON trans.BranchID = polper.ID join 
                       | $hiveDB.stg_gwcl_pc_job job ON polper.JobID = job.ID 
                       |where 
                       | polper.pctl_policyperiodstatus_typecode IN ('Bound','AuditComplete') AND 
                       | job.CloseDate <= cast('$PV_ENDDATE' as timestamp) AND
                       | pctl_chargepattern_typecode = 'Premium' AND 
                       | trans.WrittenDate <= cast('$PV_ENDDATE' as timestamp)
                       |UNION ALL 
                       |SELECT 
                       | cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                       | 'Bound' AS PolStatus,
                       | 'ALL_CUP_PREM' AS AuditEntity,
                       |coalesce(sum(trans.Amountbilling),0) AS AuditResult
                       |from 
                       | $hiveDB.stg_gwcl_pcx_cutransaction trans join 
                       | $hiveDB.stg_gwcl_pcx_cucost cost ON trans.CUCost = cost.ID join 
                       | $hiveDB.stg_gwcl_pc_policyperiod polper ON trans.BranchID = polper.ID join 
                       | $hiveDB.stg_gwcl_pc_job job ON polper.JobID = job.ID
                       |where 
                       | polper.pctl_policyperiodstatus_typecode IN ('Bound','AuditComplete') AND 
                       | job.CloseDate <= cast('$PV_ENDDATE' as timestamp) AND
                       | pctl_chargepattern_typecode = 'Premium' AND 
                       | trans.WrittenDate <= cast('$PV_ENDDATE' as timestamp)
                       |UNION ALL 
                       |SELECT 
                       |cast(cast('$PV_ENDDATE' as timestamp) AS Date) AS ExtractDate,
                       | 'Bound' AS PolStatus,
                       | 'YTD_CUP_PREM' AS AuditEntity,
                       | coalesce(sum(trans.Amountbilling),0) AS AuditResult
                       |from 
                       | $hiveDB.stg_gwcl_pcx_cutransaction trans join 
                       | $hiveDB.stg_gwcl_pcx_cucost cost ON trans.CUCost = cost.ID join 
                       | $hiveDB.stg_gwcl_pc_policyperiod polper ON trans.BranchID = polper.ID join 
                       | $hiveDB.stg_gwcl_pc_job job ON polper.JobID = job.ID
                       |where 
                       | polper.pctl_policyperiodstatus_typecode IN ('Bound','AuditComplete') AND 
                       | job.CloseDate <= cast('$PV_ENDDATE' as timestamp) AND
                       | pctl_chargepattern_typecode = 'Premium' AND 
                       | YEAR(trans.WrittenDate) = YEAR(cast('$PV_ENDDATE' as timestamp)) AND
                       | MONTH(trans.WrittenDate) <= MONTH(cast('$PV_ENDDATE' as timestamp))""".stripMargin


  val gwccSourceQuery = s"""(SELECT
                           |CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
                           |'Bound' as polstatus,
                           |'CLAIM_ID_CNT' as auditentity,
                           |'1900-01-01' as AuditFrom,
                           |'$PV_ENDDATE'  as AuditThru,
                           |count(distinct claim.id) as AuditResult
                           |FROM dbo.cc_claim claim  join
                           |dbo.cc_transaction trans on trans.claimid = claim.id
                           |Where
                           |cast (claim.CreateTime as Datetime) <= '$PV_ENDDATE' and
                           |cast (trans.CreateTime as Datetime) <= '$PV_ENDDATE' and
                           |cast (trans.UpdateTime as Datetime) <= '$PV_ENDDATE' and
                           |trans.LifeCycleState=8
                           |union
                           |SELECT
                           |CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
                           |'Bound' as polstatus,
                           |'CLAIM_NBR_CNT' as auditentity,
                           |'1900-01-01' as AuditFrom,
                           |'$PV_ENDDATE'  as AuditThru,
                           |count(distinct claim.claimnumber) as AuditResult
                           |FROM dbo.cc_claim claim  join
                           |dbo.cc_transaction trans on trans.claimid = claim.id
                           |Where
                           |cast (claim.CreateTime as Datetime) <= '$PV_ENDDATE' and
                           |cast (trans.CreateTime as Datetime) <= '$PV_ENDDATE' and
                           |cast (trans.UpdateTime as Datetime) <= '$PV_ENDDATE' and
                           |trans.LifeCycleState=8
                           |UNION
                           |SELECT
                           |CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
                           |'Bound' as polstatus,
                           |'MTD_LOSSAMT' as auditentity,
                           |DATEADD(mm, DATEDIFF(mm, 0, CAST('$PV_ENDDATE' as DATE) ), 0) as AuditFrom,
                           |'$PV_ENDDATE'  as AuditThru,
                           |SUM(isnull(lineitem.transactionAmount,0)) as AuditResult
                           |FROM
                           |dbo.cc_transactionlineitem lineitem  join
                           |dbo.cc_transaction trans on lineitem.TransactionID = trans.ID
                           |where
                           |COALESCE (cast(trans.OriginalCreateDate_Ext as DATETIME),
                           |cast(trans.CreateTime as DATETIME)) >= DATEADD(mm, DATEDIFF(mm, 0, CAST('$PV_ENDDATE' as DATE) ), 0) and
                           |COALESCE (cast(trans.OriginalCreateDate_Ext as DATETIME),
                           |cast(trans.CreateTime as DATETIME)) < '$PV_ENDDATE' and
                           |cast(lineitem.CreateTime as datetime) < '$PV_ENDDATE'
                           |UNION
                           |SELECT
                           |CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
                           |'Bound' as polstatus,
                           |'YTD_LOSSAMT' as auditentity,
                           |DATEADD(yyyy, DATEDIFF(yyyy, 0, CAST('$PV_ENDDATE' as DATE) ), 0) as AuditFrom,
                           |'$PV_ENDDATE'  as AuditThru,
                           |SUM(isnull(lineitem.transactionAmount,0)) as AuditResult
                           |FROM
                           |dbo.cc_transactionlineitem lineitem  join
                           |dbo.cc_transaction trans on lineitem.TransactionID = trans.ID
                           |where
                           |COALESCE (cast(trans.OriginalCreateDate_Ext as DATETIME), cast(trans.CreateTime as DATETIME)) >= DATEADD(yyyy, DATEDIFF(yyyy, 0, CAST('$PV_ENDDATE' as DATE) ), 0) and
                           |COALESCE (cast(trans.OriginalCreateDate_Ext as DATETIME), cast(trans.CreateTime as DATETIME)) < '$PV_ENDDATE' and
                           |cast(lineitem.CreateTime as datetime) < '$PV_ENDDATE'
                           |UNION
                           |SELECT
                           |CAST('$PV_ENDDATE' AS DATE) as ExtractDate,
                           |'Bound' as polstatus,
                           |'ALL_LOSSAMT' as auditentity,
                           |DATEADD(yyyy, DATEDIFF(yyyy, 0, CAST(CONVERT(date, CAST(CAST(YEAR(GETDATE()) AS CHAR(4))+'0101' AS DATE), 112) as DATE) ), 0) as AuditFrom,
                           |'$PV_ENDDATE'  as AuditThru,
                           |SUM(isnull(lineitem.transactionAmount,0)) as AuditResult
                           |FROM
                           |dbo.cc_transactionlineitem lineitem  join
                           |dbo.cc_transaction trans on lineitem.TransactionID = trans.ID
                           |where
                           |COALESCE (cast(trans.OriginalCreateDate_Ext as DATETIME),
                           |cast(trans.CreateTime as DATETIME)) >= '1900-01-01' and
                           |COALESCE (cast(trans.OriginalCreateDate_Ext as DATETIME),
                           |cast(trans.CreateTime as DATETIME)) < '$PV_ENDDATE' and
                           |cast(lineitem.CreateTime as datetime) < '$PV_ENDDATE'
                           |) q""".stripMargin
  val gwccLakeQuery = s"""SELECT
                         |'CLAIM_ID_CNT' as auditentity,
                         |count(distinct claim.id) as AuditResult
                         |FROM
                         |$hiveDB.stg_gwcc_cc_claim claim join
                         |$hiveDB.stg_gwcc_cc_transaction trans on trans.claimid = claim.id
                         |WHERE
                         |claim.CreateTime <= cast('$PV_ENDDATE' as timestamp) and
                         |trans.CreateTime <= cast('$PV_ENDDATE' as timestamp) and
                         |trans.updatetime <= cast('$PV_ENDDATE' as timestamp) and
                         |trans.LifeCycleState=8
                         |union
                         |SELECT
                         |'CLAIM_NBR_CNT' as auditentity,
                         |count(distinct claim.claimnumber) as AuditResult
                         |FROM
                         |$hiveDB.stg_gwcc_cc_claim claim join
                         |$hiveDB.stg_gwcc_cc_transaction trans on trans.claimid = claim.id
                         |WHERE
                         |claim.CreateTime <= cast('$PV_ENDDATE' as timestamp) and
                         |trans.CreateTime <= cast('$PV_ENDDATE' as timestamp) and
                         |trans.updatetime <= cast('$PV_ENDDATE' as timestamp) and
                         |trans.LifeCycleState=8
                         |union
                         |SELECT
                         |'MTD_LOSSAMT' as auditentity,
                         |SUM(coalesce(lineitem.transactionAmount,0)) as AuditResult
                         |FROM $hiveDB.stg_gwcc_cc_transactionlineitem lineitem JOIN
                         |$hiveDB.stg_gwcc_cc_transaction trans ON lineitem.TransactionID = trans.ID JOIN
                         |(select cast(add_months(CAST('1900-01-01' AS timestamp),cast(months_between(cast('$PV_ENDDATE' as date), CAST('1900-01-01' AS date)) as int)) as timestamp) as PV_STARTTIME ) lc ON 1=1
                         |WHERE
                         |COALESCE (cast(trans.OriginalCreateDate_Ext AS timestamp), cast(trans.CreateTime AS timestamp)) >= PV_STARTTIME AND
                         |COALESCE (cast(trans.OriginalCreateDate_Ext AS timestamp), cast(trans.CreateTime AS timestamp)) < cast('$PV_ENDDATE' as timestamp) and
                         |cast(lineitem.CreateTime as timestamp) < cast('$PV_ENDDATE' as timestamp)
                         |union
                         |SELECT
                         |'YTD_LOSSAMT' as auditentity,
                         |SUM(coalesce(lineitem.transactionAmount,0)) as AuditResult
                         |FROM
                         |$hiveDB.stg_gwcc_cc_transactionlineitem lineitem JOIN
                         |$hiveDB.stg_gwcc_cc_transaction trans ON lineitem.TransactionID = trans.ID JOIN
                         |(select cast(concat_ws('-',(year(CAST('1900-01-01' AS timestamp)) + cast(months_between(cast(cast('$PV_ENDDATE' AS timestamp) as date), CAST('1900-01-01' AS date))/12 as int)),'01','01') as timestamp) as PV_STARTTIME ) lc ON 1=1
                         |WHERE
                         |COALESCE (cast(trans.OriginalCreateDate_Ext AS timestamp), cast(trans.CreateTime AS timestamp)) >= PV_STARTTIME AND
                         |COALESCE (cast(trans.OriginalCreateDate_Ext AS timestamp), cast(trans.CreateTime AS timestamp)) < cast('$PV_ENDDATE' as timestamp) and
                         |cast(lineitem.CreateTime as timestamp) < cast('$PV_ENDDATE' as timestamp)
                         |union
                         |SELECT
                         |'ALL_LOSSAMT' as auditentity,
                         |SUM(coalesce(lineitem.transactionAmount,0)) as AuditResult
                         |FROM
                         |$hiveDB.stg_gwcc_cc_transactionlineitem lineitem JOIN
                         |$hiveDB.stg_gwcc_cc_transaction trans ON lineitem.TransactionID = trans.ID  JOIN
                         |(select CAST('1900-01-01' AS date) as PV_STARTTIME ) lc ON 1=1
                         |WHERE
                         |COALESCE (cast(trans.OriginalCreateDate_Ext AS timestamp), cast(trans.CreateTime AS timestamp)) >= PV_STARTTIME AND
                         |COALESCE (cast(trans.OriginalCreateDate_Ext AS timestamp), cast(trans.CreateTime AS timestamp)) < cast('$PV_ENDDATE' as timestamp) and
                         |cast(lineitem.CreateTime as timestamp) < cast('$PV_ENDDATE' as timestamp)
                         |""".stripMargin

  val gwbcSourceQuery = s"""(SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_BILL_INV_ITM_CNT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, count(distinct invitem.PublicID) AS auditresult
                           |FROM dbo.bc_invoiceitem invitem
                           |WHERE (invitem.UpdateTime <= '$PV_ENDDATE'
                           |        OR invitem.CreateTime <= '$PV_ENDDATE' )
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_BILL_INV_ITM_AMT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, sum(invitem.Amount) AS auditresult
                           |FROM dbo.bc_invoiceitem invitem
                           |WHERE (invitem.UpdateTime <= '$PV_ENDDATE'
                           |        OR invitem.CreateTime <= '$PV_ENDDATE' )
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_BILL_CHRG_CNT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, count(distinct chrg.PublicID) AS auditresult
                           |FROM dbo.bc_charge chrg
                           |WHERE chrg.CreateTime <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_BILL_CHRG_AMT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(chrg.Amount) AS auditresult
                           |FROM dbo.bc_charge chrg
                           |WHERE chrg.CreateTime <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_BILL_DIST_ITM_CNT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, count(distinct distitem.PublicID) AS auditresult
                           |FROM dbo.bc_basedistitem distitem
                           |WHERE (distitem.UpdateTime <= '$PV_ENDDATE'
                           |        OR distitem.CreateTime <= '$PV_ENDDATE' )
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_GROSS_APPLY_AMT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(distitem.GrossAmountToApply) AS auditresult
                           |FROM dbo.bc_basedistitem distitem
                           |WHERE (distitem.UpdateTime <= '$PV_ENDDATE'
                           |        OR distitem.CreateTime <= '$PV_ENDDATE' )
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_COMM_APPLY_AMT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(distitem.CommissionAmountToApply) AS auditresult
                           |FROM dbo.bc_basedistitem distitem
                           |WHERE (distitem.UpdateTime <= '$PV_ENDDATE'
                           |        OR distitem.CreateTime <= '$PV_ENDDATE' )
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_BILL_DIST_CNT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, count(distinct basedist.PublicID) AS auditresult
                           |FROM dbo.bc_basedist basedist
                           |LEFT JOIN
                           |    (SELECT DISTINCT ISNULL(ActiveDistID ,
                           |         ReversedDistID ) AS DistID,
                           |        MAX(CAST(CASE
                           |        WHEN distitem.CreateTime > '$PV_STARTDATE'
                           |            AND distitem.CreateTime <= '$PV_ENDDATE' THEN
                           |        distitem.CreateTime
                           |        ELSE distitem.UpdateTime
                           |        END AS datetime2)) AS distitem_ROW_PROC_DTS
                           |    FROM dbo.bc_basedistitem distitem
                           |        WHERE (distitem.UpdateTime > '$PV_STARTDATE'
                           |                AND distitem.UpdateTime <= '$PV_ENDDATE')
                           |                OR (distitem.CreateTime > '$PV_STARTDATE'
                           |                AND distitem.CreateTime <= '$PV_ENDDATE')
                           |        GROUP BY  ISNULL(ActiveDistID , ReversedDistID )) disitem
                           |        ON disitem.DistID = basedist.ID
                           |WHERE ((basedist.UpdateTime <= '$PV_ENDDATE'
                           |        OR basedist.CreateTime <= '$PV_ENDDATE' )
                           |        OR distitem_ROW_PROC_DTS <= '$PV_ENDDATE')
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_BILL_DIST_WO_AMT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(basedist.WriteOffAmount) AS auditresult
                           |FROM dbo.bc_basedist basedist
                           |LEFT JOIN
                           |    (SELECT DISTINCT ISNULL(ActiveDistID ,
                           |         ReversedDistID ) AS DistID,
                           |        MAX(CAST(CASE
                           |        WHEN distitem.CreateTime > '$PV_STARTDATE'
                           |            AND distitem.CreateTime <= '$PV_ENDDATE' THEN
                           |        distitem.CreateTime
                           |        ELSE distitem.UpdateTime
                           |        END AS datetime2)) AS distitem_ROW_PROC_DTS
                           |    FROM dbo.bc_basedistitem distitem
                           |        WHERE (distitem.UpdateTime > '$PV_STARTDATE'
                           |                AND distitem.UpdateTime <= '$PV_ENDDATE')
                           |                OR (distitem.CreateTime > '$PV_STARTDATE'
                           |                AND distitem.CreateTime <= '$PV_ENDDATE')
                           |        GROUP BY  ISNULL(ActiveDistID , ReversedDistID )) disitem
                           |        ON disitem.DistID = basedist.ID
                           |WHERE ((basedist.UpdateTime <= '$PV_ENDDATE'
                           |        OR basedist.CreateTime <= '$PV_ENDDATE' )
                           |        OR distitem_ROW_PROC_DTS <= '$PV_ENDDATE')
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_BILL_WO_CNT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, count(distinct wo.PublicID) AS auditresult
                           |FROM dbo.bc_writeoff wo
                           |WHERE wo.CreateTime <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_REVERSED_AMT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(wo.ReversedAmount) AS auditresult
                           |FROM dbo.bc_writeoff wo
                           |WHERE wo.CreateTime <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_WO_AMT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(wo.Amount) AS auditresult
                           |FROM dbo.bc_writeoff wo
                           |WHERE wo.CreateTime <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'MTD_BILL_CHRG_TRANS_AMT' AS auditentity, DATEADD(mm, DATEDIFF(mm, 0, CAST('$PV_ENDDATE' AS DATE) ), 0) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_chargeinstancecontext cix
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = cix.TransactionID
                           |JOIN dbo.bc_charge chrg
                           |    ON chrg.ID = cix.ChargeID
                           |JOIN dbo.bc_TAccountContainer TAC
                           |    ON TAC.ID = chrg.TAccountContainerID
                           |JOIN dbo.bctl_TAccountContainer TC
                           |    ON TC.ID = TAC.Subtype
                           |LEFT JOIN dbo.bc_policyperiod polper
                           |    ON (polper.HiddenTAccountContainerID = chrg.TAccountContainerID
                           |        AND TC.TYPECODE = 'PolTAcctContainer')
                           |LEFT JOIN dbo.bc_policy pol
                           |    ON pol.ID = polper.PolicyID
                           |        AND pol.PCPublicID IS NOT NULL
                           |LEFT JOIN dbo.bc_account a
                           |    ON a.ID = pol.AccountID
                           |LEFT JOIN dbo.bc_account ta
                           |    ON (ta.HiddenTAccountContainerID = chrg.TAccountContainerID
                           |        AND TC.TYPECODE = 'AcctTAcctContainer')
                           |WHERE CONVERT(varchar(6), trans.TransactionDate, 112) = CONVERT(varchar(6), '$PV_ENDDATE', 112)
                           |        AND trans.TransactionDate <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'YTD_BILL_CHRG_TRANS_AMT' AS auditentity, cast(cast(YEAR('$PV_ENDDATE') AS varchar)+'-01-01' AS datetime2) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_chargeinstancecontext cix
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = cix.TransactionID
                           |JOIN dbo.bc_charge chrg
                           |    ON chrg.ID = cix.ChargeID
                           |JOIN dbo.bc_TAccountContainer TAC
                           |    ON TAC.ID = chrg.TAccountContainerID
                           |JOIN dbo.bctl_TAccountContainer TC
                           |    ON TC.ID = TAC.Subtype
                           |LEFT JOIN dbo.bc_policyperiod polper
                           |    ON (polper.HiddenTAccountContainerID = chrg.TAccountContainerID
                           |        AND TC.TYPECODE = 'PolTAcctContainer')
                           |LEFT JOIN dbo.bc_policy pol
                           |    ON pol.ID = polper.PolicyID
                           |        AND pol.PCPublicID IS NOT NULL
                           |LEFT JOIN dbo.bc_account a
                           |    ON a.ID = pol.AccountID
                           |LEFT JOIN dbo.bc_account ta
                           |    ON (ta.HiddenTAccountContainerID = chrg.TAccountContainerID
                           |        AND TC.TYPECODE = 'AcctTAcctContainer')
                           |WHERE YEAR(trans.TransactionDate) = YEAR('$PV_ENDDATE')
                           |        AND trans.TransactionDate <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_BILL_NWO_CNT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, COUNT(DISTINCT nwo.PublicID) AS auditresult
                           |FROM dbo.bc_negativewriteoff nwo
                           |WHERE nwo.CreateTime <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_NWO_AMT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(nwo.Amount) AS auditresult
                           |FROM dbo.bc_negativewriteoff nwo
                           |WHERE nwo.CreateTime <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_DISB_AMT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(disb.Amount) AS auditresult
                           |FROM dbo.bc_disbursement disb
                           |WHERE disb.CreateTime <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_FUND_TRANFR_AMT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(ft.Amount) AS auditresult
                           |FROM dbo.bc_fundsTransfer ft
                           |WHERE ft.CreateTime <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_BILL_NONRECV_DIST_ITM_CNT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, COUNT(distinct distitem.PublicID) AS auditresult
                           |FROM dbo.bc_basenonrecdistitem distitem
                           |WHERE (distitem.UpdateTime <= '$PV_ENDDATE'
                           |        OR distitem.CreateTime <= '$PV_ENDDATE' )
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_GROSS_NONRECV_APPLY_AMT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(distitem.GrossAmountToApply) AS auditresult
                           |FROM dbo.bc_basenonrecdistitem distitem
                           |WHERE (distitem.UpdateTime <= '$PV_ENDDATE'
                           |        OR distitem.CreateTime <= '$PV_ENDDATE' )
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'TOTAL_COMM_NONRECV_APPLY_AMT' AS auditentity, '1900-01-01 00:00:00' AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(distitem.CommissionAmountToApply) AS auditresult
                           |FROM dbo.bc_basenonrecdistitem distitem
                           |WHERE (distitem.UpdateTime <= '$PV_ENDDATE'
                           |        OR distitem.CreateTime <= '$PV_ENDDATE' )
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'MTD_BILL_PROD_TRANS_AMT' AS auditentity, DATEADD(mm, DATEDIFF(mm, 0, CAST('$PV_ENDDATE' AS DATE) ), 0) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_producercontext px
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = px.TransactionID
                           |LEFT JOIN dbo.bc_chargecommission comm
                           |    ON comm.ID = px.ChargeCommissionID
                           |LEFT JOIN dbo.bc_charge chrg
                           |    ON chrg.ID = comm.ChargeID
                           |LEFT JOIN dbo.bc_policyperiod polper
                           |    ON polper.HiddenTAccountContainerID = chrg.TAccountContainerID
                           |LEFT JOIN dbo.bc_policy pol
                           |    ON pol.ID = polper.PolicyID
                           |        AND pol.PCPublicID IS NOT NULL
                           |LEFT JOIN dbo.bc_account a
                           |    ON a.ID = pol.AccountID
                           |WHERE CONVERT(varchar(6), trans.TransactionDate, 112) = CONVERT(varchar(6), '$PV_ENDDATE', 112)
                           |        AND trans.TransactionDate <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'YTD_BILL_PROD_TRANS_AMT' AS auditentity, cast(cast(YEAR('$PV_ENDDATE') AS varchar)+'-01-01' AS datetime2) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_producercontext px
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = px.TransactionID
                           |LEFT JOIN dbo.bc_chargecommission comm
                           |    ON comm.ID = px.ChargeCommissionID
                           |LEFT JOIN dbo.bc_charge chrg
                           |    ON chrg.ID = comm.ChargeID
                           |LEFT JOIN dbo.bc_policyperiod polper
                           |    ON polper.HiddenTAccountContainerID = chrg.TAccountContainerID
                           |LEFT JOIN dbo.bc_policy pol
                           |    ON pol.ID = polper.PolicyID
                           |        AND pol.PCPublicID IS NOT NULL
                           |LEFT JOIN dbo.bc_account a
                           |    ON a.ID = pol.AccountID
                           |WHERE YEAR(trans.TransactionDate) = YEAR('$PV_ENDDATE')
                           |        AND trans.TransactionDate <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'MTD_BILL_ACCT_TRANS_AMT' AS auditentity, DATEADD(mm, DATEDIFF(mm, 0, CAST('$PV_ENDDATE' AS DATE) ), 0) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_accountcontext cix
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = cix.TransactionID
                           |JOIN dbo.bc_account acc
                           |    ON acc.ID = cix.AccountID
                           |WHERE CONVERT(varchar(6), trans.TransactionDate, 112) = CONVERT(varchar(6), '$PV_ENDDATE', 112)
                           |        AND trans.TransactionDate <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'YTD_BILL_ACCT_TRANS_AMT' AS auditentity, cast(cast(YEAR('$PV_ENDDATE') AS varchar)+'-01-01' AS datetime2) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_accountcontext cix
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = cix.TransactionID
                           |JOIN dbo.bc_account acc
                           |    ON acc.ID = cix.AccountID
                           |WHERE YEAR(trans.TransactionDate) = YEAR('$PV_ENDDATE')
                           |        AND trans.TransactionDate <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'MTD_BILL_DBMR_TRANS_AMT' AS auditentity, DATEADD(mm, DATEDIFF(mm, 0, CAST('$PV_ENDDATE' AS DATE) ), 0) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_dbmoneyrcvdcontext cix
                           |JOIN dbo.bc_account acc
                           |    ON acc.ID = cix.AccountID
                           |LEFT JOIN dbo.bc_basemoneyreceived bsmr
                           |    ON bsmr.ID = cix.DirectBillMoneyRcvdID
                           |LEFT JOIN dbo.bc_policyperiod polper
                           |    ON polper.ID = bsmr.PolicyPeriodID
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = cix.TransactionID
                           |WHERE CONVERT(varchar(6), trans.TransactionDate, 112) = CONVERT(varchar(6), '$PV_ENDDATE', 112)
                           |        AND trans.TransactionDate <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'YTD_BILL_DBMR_TRANS_AMT' AS auditentity, cast(cast(YEAR('$PV_ENDDATE') AS varchar)+'-01-01' AS datetime2) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_dbmoneyrcvdcontext cix
                           |JOIN dbo.bc_account acc
                           |    ON acc.ID = cix.AccountID
                           |LEFT JOIN dbo.bc_basemoneyreceived bsmr
                           |    ON bsmr.ID = cix.DirectBillMoneyRcvdID
                           |LEFT JOIN dbo.bc_policyperiod polper
                           |    ON polper.ID = bsmr.PolicyPeriodID
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = cix.TransactionID
                           |WHERE YEAR(trans.TransactionDate) = YEAR('$PV_ENDDATE')
                           |        AND trans.TransactionDate <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'MTD_BILL_SUSPYMT_TRANS_AMT' AS auditentity, DATEADD(mm, DATEDIFF(mm, 0, CAST('$PV_ENDDATE' AS DATE) ), 0) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_susppymtcontext cix
                           |JOIN dbo.bc_suspensepayment spmt
                           |    ON spmt.ID = cix.SuspensePaymentID
                           |LEFT JOIN dbo.bc_policyperiod polper
                           |    ON polper.ID = spmt.PolicyPeriodAppliedToID
                           |LEFT JOIN dbo.bc_policy pol
                           |    ON pol.ID = polper.PolicyID
                           |        AND pol.PCPublicID IS NOT NULL
                           |LEFT JOIN dbo.bc_account a
                           |    ON a.ID = pol.AccountID
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = cix.TransactionID
                           |WHERE CONVERT(varchar(6), trans.TransactionDate, 112) = CONVERT(varchar(6), '$PV_ENDDATE', 112)
                           |        AND trans.TransactionDate <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'YTD_BILL_SUSPYMT_TRANS_AMT' AS auditentity, cast(cast(YEAR('$PV_ENDDATE') AS varchar)+'-01-01' AS datetime2) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_susppymtcontext cix
                           |JOIN dbo.bc_suspensepayment spmt
                           |    ON spmt.ID = cix.SuspensePaymentID
                           |LEFT JOIN dbo.bc_policyperiod polper
                           |    ON polper.ID = spmt.PolicyPeriodAppliedToID
                           |LEFT JOIN dbo.bc_policy pol
                           |    ON pol.ID = polper.PolicyID
                           |        AND pol.PCPublicID IS NOT NULL
                           |LEFT JOIN dbo.bc_account a
                           |    ON a.ID = pol.AccountID
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = cix.TransactionID
                           |WHERE YEAR(trans.TransactionDate) = YEAR('$PV_ENDDATE')
                           |        AND trans.TransactionDate <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'MTD_BILL_TRANSFR_TRANS_AMT' AS auditentity, DATEADD(mm, DATEDIFF(mm, 0, CAST('$PV_ENDDATE' AS DATE) ), 0) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_transfertxcontext cix
                           |LEFT JOIN dbo.bc_account sa
                           |    ON sa.ID = cix.SourceAccountID
                           |LEFT JOIN dbo.bc_account ta
                           |    ON ta.ID = cix.TargetAccountID
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = cix.TransactionID
                           |WHERE CONVERT(varchar(6), trans.TransactionDate, 112) = CONVERT(varchar(6), '$PV_ENDDATE', 112)
                           |        AND trans.TransactionDate <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'YTD_BILL_TRANSFR_TRANS_AMT' AS auditentity, cast(cast(YEAR('$PV_ENDDATE') AS varchar)+'-01-01' AS datetime2) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_transfertxcontext cix
                           |LEFT JOIN dbo.bc_account sa
                           |    ON sa.ID = cix.SourceAccountID
                           |LEFT JOIN dbo.bc_account ta
                           |    ON ta.ID = cix.TargetAccountID
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = cix.TransactionID
                           |WHERE YEAR(trans.TransactionDate) = YEAR('$PV_ENDDATE')
                           |        AND trans.TransactionDate <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'MTD_BILL_NONRECV_TRANS_AMT' AS auditentity, DATEADD(mm, DATEDIFF(mm, 0, CAST('$PV_ENDDATE' AS DATE) ), 0) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_nonreceivableitemctx cix
                           |JOIN dbo.bc_basenonrecdistitem bndi
                           |    ON bndi.ID = cix.BaseNonReceivableDistItemID
                           |LEFT JOIN dbo.bc_account acc
                           |    ON acc.ID = cix.AccountWithSuspenseID
                           |LEFT JOIN dbo.bc_policyperiod polper
                           |    ON polper.ID = bndi.MatchingPolicyID
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = cix.TransactionID
                           |WHERE CONVERT(varchar(6), trans.TransactionDate, 112) = CONVERT(varchar(6), '$PV_ENDDATE', 112)
                           |        AND trans.TransactionDate <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'YTD_BILL_NONRECV_TRANS_AMT' AS auditentity, cast(cast(YEAR('$PV_ENDDATE') AS varchar)+'-01-01' AS datetime2) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_nonreceivableitemctx cix
                           |JOIN dbo.bc_basenonrecdistitem bndi
                           |    ON bndi.ID = cix.BaseNonReceivableDistItemID
                           |LEFT JOIN dbo.bc_account acc
                           |    ON acc.ID = cix.AccountWithSuspenseID
                           |LEFT JOIN dbo.bc_policyperiod polper
                           |    ON polper.ID = bndi.MatchingPolicyID
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = cix.TransactionID
                           |WHERE YEAR(trans.TransactionDate) = YEAR('$PV_ENDDATE')
                           |        AND trans.TransactionDate <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'MTD_BILL_CREDIT_TRANS_AMT' AS auditentity, DATEADD(mm, DATEDIFF(mm, 0, CAST('$PV_ENDDATE' AS DATE) ), 0) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_creditcontext cix
                           |JOIN dbo.bc_account acc
                           |    ON acc.ID = cix.AccountID
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = cix.TransactionID
                           |WHERE CONVERT(varchar(6), trans.TransactionDate, 112) = CONVERT(varchar(6), '$PV_ENDDATE', 112)
                           |        AND trans.TransactionDate <= '$PV_ENDDATE'
                           |UNION
                           |SELECT '$PV_ENDDATE' AS extractdate,
                           |         'GWBC' AS source_system, 'YTD_BILL_CREDIT_TRANS_AMT' AS auditentity, cast(cast(YEAR('$PV_ENDDATE') AS varchar)+'-01-01' AS datetime2) AS auditfrom, '$PV_ENDDATE' AS auditthru, SUM(trans.Amount) AS auditresult
                           |FROM dbo.bc_creditcontext cix
                           |JOIN dbo.bc_account acc
                           |    ON acc.ID = cix.AccountID
                           |JOIN dbo.bc_transaction trans
                           |    ON trans.ID = cix.TransactionID
                           |WHERE YEAR(trans.TransactionDate) = YEAR('$PV_ENDDATE')
                           |        AND trans.TransactionDate <= '$PV_ENDDATE' ) a""".stripMargin

  val gwbcLakeQuery = s"""SELECT 'TOTAL_BILL_INV_ITM_CNT' AS auditentity, count(distinct invitem.PublicID) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_invoiceitem invitem
                         |WHERE (invitem.UpdateTime <= cast('$PV_ENDDATE' as timestamp)
                         |        OR invitem.CreateTime <= cast('$PV_ENDDATE' as timestamp) )
                         |UNION
                         |SELECT 'TOTAL_BILL_INV_ITM_AMT' AS auditentity, sum(invitem.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_invoiceitem invitem
                         |WHERE (invitem.UpdateTime <= cast('$PV_ENDDATE' as timestamp)
                         |        OR invitem.CreateTime <= cast('$PV_ENDDATE' as timestamp) )
                         |UNION
                         |SELECT  'TOTAL_BILL_CHRG_CNT' AS auditentity,  count(distinct chrg.PublicID) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_charge chrg
                         |WHERE chrg.CreateTime <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'TOTAL_BILL_CHRG_AMT' AS auditentity,  SUM(chrg.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_charge chrg
                         |WHERE chrg.CreateTime <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'TOTAL_BILL_DIST_ITM_CNT' AS auditentity,  count(distinct distitem.PublicID) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_basedistitem distitem
                         |WHERE (distitem.UpdateTime <= cast('$PV_ENDDATE' as timestamp)
                         |        OR distitem.CreateTime <= cast('$PV_ENDDATE' as timestamp) )
                         |UNION
                         |SELECT  'TOTAL_GROSS_APPLY_AMT' AS auditentity,  SUM(distitem.GrossAmountToApply) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_basedistitem distitem
                         |WHERE (distitem.UpdateTime <= cast('$PV_ENDDATE' as timestamp)
                         |        OR distitem.CreateTime <= cast('$PV_ENDDATE' as timestamp) )
                         |UNION
                         |SELECT  'TOTAL_COMM_APPLY_AMT' AS auditentity,  SUM(distitem.CommissionAmountToApply) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_basedistitem distitem
                         |WHERE (distitem.UpdateTime <= cast('$PV_ENDDATE' as timestamp)
                         |        OR distitem.CreateTime <= cast('$PV_ENDDATE' as timestamp) )
                         |UNION
                         |SELECT  'TOTAL_BILL_DIST_CNT' AS auditentity,  count(distinct basedist.PublicID) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_basedist basedist
                         |LEFT JOIN
                         |    (SELECT COALESCE(ActiveDistID ,
                         |         ReversedDistID ) AS DistID ,
                         |        CASE
                         |        WHEN distitem.CreateTime > cast('$PV_STARTDATE' as timestamp)
                         |            AND distitem.CreateTime <= cast('$PV_ENDDATE' as timestamp) THEN
                         |        MAX(distitem.CreateTime)
                         |        ELSE MAX(distitem.UpdateTime)
                         |        END AS distitem_ROW_PROC_DTS
                         |    FROM $hiveDB.stg_gwbc_bc_basedistitem distitem
                         |        WHERE (distitem.UpdateTime > cast('$PV_STARTDATE' as timestamp)
                         |                AND distitem.UpdateTime <= cast('$PV_ENDDATE' as timestamp))
                         |                OR (distitem.CreateTime > cast('$PV_STARTDATE' as timestamp)
                         |                AND distitem.CreateTime <= cast('$PV_ENDDATE' as timestamp))
                         |        GROUP BY  COALESCE(ActiveDistID , ReversedDistID ) ,distitem.CreateTime,distitem.UpdateTime ) disitem
                         |        ON disitem.DistID = basedist.ID
                         |WHERE ((basedist.UpdateTime <= cast('$PV_ENDDATE' as timestamp)
                         |        OR basedist.CreateTime <= cast('$PV_ENDDATE' as timestamp) )
                         |        OR distitem_ROW_PROC_DTS <= cast('$PV_ENDDATE' as timestamp))
                         |UNION
                         |SELECT   'TOTAL_BILL_DIST_WO_AMT' AS auditentity,  SUM(basedist.WriteOffAmount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_basedist basedist
                         |LEFT JOIN
                         |    (SELECT COALESCE(ActiveDistID ,
                         |         ReversedDistID ) AS DistID,
                         |        CASE
                         |        WHEN distitem.CreateTime > cast('$PV_STARTDATE' as timestamp)
                         |            AND distitem.CreateTime <= cast('$PV_ENDDATE' as timestamp) THEN
                         |        MAX(distitem.CreateTime)
                         |        ELSE MAX(distitem.UpdateTime)
                         |        END AS distitem_ROW_PROC_DTS
                         |    FROM $hiveDB.stg_gwbc_bc_basedistitem distitem
                         |        WHERE (distitem.UpdateTime > cast('$PV_STARTDATE' as timestamp)
                         |                AND distitem.UpdateTime <= cast('$PV_ENDDATE' as timestamp))
                         |                OR (distitem.CreateTime > cast('$PV_STARTDATE' as timestamp)
                         |                AND distitem.CreateTime <= cast('$PV_ENDDATE' as timestamp))
                         |        GROUP BY  COALESCE(ActiveDistID , ReversedDistID ),distitem.CreateTime ) disitem
                         |        ON disitem.DistID = basedist.ID
                         |WHERE (basedist.UpdateTime <= cast('$PV_ENDDATE' as timestamp)
                         |        OR basedist.CreateTime <= cast('$PV_ENDDATE' as timestamp) )
                         |        OR distitem_ROW_PROC_DTS <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'TOTAL_BILL_WO_CNT' AS auditentity,  count(distinct wo.PublicID) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_writeoff wo
                         |WHERE wo.CreateTime <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'TOTAL_REVERSED_AMT' AS auditentity,  SUM(wo.ReversedAmount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_writeoff wo
                         |WHERE wo.CreateTime <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'TOTAL_WO_AMT' AS auditentity,  SUM(wo.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_writeoff wo
                         |WHERE wo.CreateTime <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'MTD_BILL_CHRG_TRANS_AMT' AS auditentity, SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_chargeinstancecontext cix
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = cix.TransactionID
                         |JOIN $hiveDB.stg_gwbc_bc_charge chrg
                         |    ON chrg.ID = cix.ChargeID
                         |JOIN $hiveDB.stg_gwbc_bc_TAccountContainer TAC
                         |    ON TAC.ID = chrg.TAccountContainerID
                         |JOIN $hiveDB.stg_gwbc_bc_taccountcontainer TC
                         |    ON TC.ID = TAC.Subtype
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policyperiod polper
                         |    ON (polper.HiddenTAccountContainerID = chrg.TAccountContainerID
                         |        AND TC.bctl_taccountcontainer_typecode_subtype = 'PolTAcctContainer' )
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policy pol
                         |    ON pol.ID = polper.PolicyID
                         |        AND pol.PCPublicID IS NOT NULL
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_account a
                         |    ON a.ID = pol.AccountID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_account ta
                         |    ON (ta.HiddenTAccountContainerID = chrg.TAccountContainerID
                         |        AND TC.bctl_taccountcontainer_typecode_subtype = 'AcctTAcctContainer')
                         |WHERE replace(substring(trans.TransactionDate, 1, 7),'-','') = replace(substring(cast('$PV_ENDDATE' as timestamp), 1, 7),'-','')
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'YTD_BILL_CHRG_TRANS_AMT' AS auditentity, SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_chargeinstancecontext cix
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = cix.TransactionID
                         |JOIN $hiveDB.stg_gwbc_bc_charge chrg
                         |    ON chrg.ID = cix.ChargeID
                         |JOIN $hiveDB.stg_gwbc_bc_TAccountContainer TAC
                         |    ON TAC.ID = chrg.TAccountContainerID
                         |JOIN $hiveDB.stg_gwbc_bc_taccountcontainer TC
                         |    ON TC.ID = TAC.Subtype
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policyperiod polper
                         |    ON (polper.HiddenTAccountContainerID = chrg.TAccountContainerID
                         |        AND TC.bctl_taccountcontainer_typecode_subtype = 'PolTAcctContainer')
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policy pol
                         |    ON pol.ID = polper.PolicyID
                         |        AND pol.PCPublicID IS NOT NULL
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_account a
                         |    ON a.ID = pol.AccountID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_account ta
                         |    ON (ta.HiddenTAccountContainerID = chrg.TAccountContainerID
                         |        AND TC.bctl_taccountcontainer_typecode_subtype = 'AcctTAcctContainer')
                         |WHERE YEAR(trans.TransactionDate) = YEAR(cast('$PV_ENDDATE' as timestamp))
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'TOTAL_BILL_NWO_CNT' AS auditentity,  COUNT(DISTINCT nwo.PublicID) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_negativewriteoff nwo
                         |WHERE nwo.CreateTime <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'TOTAL_NWO_AMT' AS auditentity,  SUM(nwo.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_negativewriteoff nwo
                         |WHERE nwo.CreateTime <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'TOTAL_DISB_AMT' AS auditentity,  SUM(disb.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_disbursement disb
                         |WHERE disb.CreateTime <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'TOTAL_FUND_TRANFR_AMT' AS auditentity,  SUM(ft.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_fundsTransfer ft
                         |WHERE ft.CreateTime <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'TOTAL_BILL_NONRECV_DIST_ITM_CNT' AS auditentity,  COUNT(distinct distitem.PublicID) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_basenonrecdistitem distitem
                         |WHERE (distitem.UpdateTime <= cast('$PV_ENDDATE' as timestamp)
                         |        OR distitem.CreateTime <= cast('$PV_ENDDATE' as timestamp) )
                         |UNION
                         |SELECT  'TOTAL_GROSS_NONRECV_APPLY_AMT' AS auditentity,  SUM(distitem.GrossAmountToApply) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_basenonrecdistitem distitem
                         |WHERE (distitem.UpdateTime <= cast('$PV_ENDDATE' as timestamp)
                         |        OR distitem.CreateTime <= cast('$PV_ENDDATE' as timestamp) )
                         |UNION
                         |SELECT  'TOTAL_COMM_NONRECV_APPLY_AMT' AS auditentity,  SUM(distitem.CommissionAmountToApply) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_basenonrecdistitem distitem
                         |WHERE (distitem.UpdateTime <= cast('$PV_ENDDATE' as timestamp)
                         |        OR distitem.CreateTime <= cast('$PV_ENDDATE' as timestamp) )
                         |UNION
                         |SELECT  'MTD_BILL_PROD_TRANS_AMT' AS auditentity, SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_producercontext px
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = px.TransactionID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_chargecommission comm
                         |    ON comm.ID = px.ChargeCommissionID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_charge chrg
                         |    ON chrg.ID = comm.ChargeID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policyperiod polper
                         |    ON polper.HiddenTAccountContainerID = chrg.TAccountContainerID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policy pol
                         |    ON pol.ID = polper.PolicyID
                         |        AND pol.PCPublicID IS NOT NULL
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_account a
                         |    ON a.ID = pol.AccountID
                         |WHERE replace(substring(trans.TransactionDate, 1, 7),'-','') = replace(substring(cast('$PV_ENDDATE' as timestamp), 1, 7),'-','')
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'YTD_BILL_PROD_TRANS_AMT' AS auditentity, SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_producercontext px
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = px.TransactionID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_chargecommission comm
                         |    ON comm.ID = px.ChargeCommissionID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_charge chrg
                         |    ON chrg.ID = comm.ChargeID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policyperiod polper
                         |    ON polper.HiddenTAccountContainerID = chrg.TAccountContainerID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policy pol
                         |    ON pol.ID = polper.PolicyID
                         |        AND pol.PCPublicID IS NOT NULL
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_account a
                         |    ON a.ID = pol.AccountID
                         |WHERE YEAR(trans.TransactionDate) = YEAR(cast('$PV_ENDDATE' as timestamp))
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'MTD_BILL_ACCT_TRANS_AMT' AS auditentity, SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_accountcontext cix
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = cix.TransactionID
                         |JOIN $hiveDB.stg_gwbc_bc_account acc
                         |    ON acc.ID = cix.AccountID
                         |WHERE replace(substring(trans.TransactionDate, 1, 7),'-','') = replace(substring(cast('$PV_ENDDATE' as timestamp), 1, 7),'-','')
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'YTD_BILL_ACCT_TRANS_AMT' AS auditentity, SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_accountcontext cix
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = cix.TransactionID
                         |JOIN $hiveDB.stg_gwbc_bc_account acc
                         |    ON acc.ID = cix.AccountID
                         |WHERE YEAR(trans.TransactionDate) = YEAR(cast('$PV_ENDDATE' as timestamp))
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'MTD_BILL_DBMR_TRANS_AMT' AS auditentity, SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_dbmoneyrcvdcontext cix
                         |JOIN $hiveDB.stg_gwbc_bc_account acc
                         |    ON acc.ID = cix.AccountID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_basemoneyreceived bsmr
                         |    ON bsmr.ID = cix.DirectBillMoneyRcvdID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policyperiod polper
                         |    ON polper.ID = bsmr.PolicyPeriodID
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = cix.TransactionID
                         |WHERE replace(substring(trans.TransactionDate, 1, 7),'-','') = replace(substring(cast('$PV_ENDDATE' as timestamp), 1, 7),'-','')
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'YTD_BILL_DBMR_TRANS_AMT' AS auditentity, SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_dbmoneyrcvdcontext cix
                         |JOIN $hiveDB.stg_gwbc_bc_account acc
                         |    ON acc.ID = cix.AccountID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_basemoneyreceived bsmr
                         |    ON bsmr.ID = cix.DirectBillMoneyRcvdID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policyperiod polper
                         |    ON polper.ID = bsmr.PolicyPeriodID
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = cix.TransactionID
                         |WHERE YEAR(trans.TransactionDate) = YEAR(cast('$PV_ENDDATE' as timestamp))
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'MTD_BILL_SUSPYMT_TRANS_AMT' AS auditentity,  SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_susppymtcontext cix
                         |JOIN $hiveDB.stg_gwbc_bc_suspensepayment spmt
                         |    ON spmt.ID = cix.SuspensePaymentID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policyperiod polper
                         |    ON polper.ID = spmt.PolicyPeriodAppliedToID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policy pol
                         |    ON pol.ID = polper.PolicyID
                         |        AND pol.PCPublicID IS NOT NULL
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_account a
                         |    ON a.ID = pol.AccountID
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = cix.TransactionID
                         |WHERE replace(substring(trans.TransactionDate, 1, 7),'-','') = replace(substring(cast('$PV_ENDDATE' as timestamp), 1, 7),'-','')
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'YTD_BILL_SUSPYMT_TRANS_AMT' AS auditentity, SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_susppymtcontext cix
                         |JOIN $hiveDB.stg_gwbc_bc_suspensepayment spmt
                         |    ON spmt.ID = cix.SuspensePaymentID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policyperiod polper
                         |    ON polper.ID = spmt.PolicyPeriodAppliedToID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policy pol
                         |    ON pol.ID = polper.PolicyID
                         |        AND pol.PCPublicID IS NOT NULL
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_account a
                         |    ON a.ID = pol.AccountID
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = cix.TransactionID
                         |WHERE YEAR(trans.TransactionDate) = YEAR(cast('$PV_ENDDATE' as timestamp))
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'MTD_BILL_TRANSFR_TRANS_AMT' AS auditentity,  SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_transfertxcontext cix
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_account sa
                         |    ON sa.ID = cix.SourceAccountID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_account ta
                         |    ON ta.ID = cix.TargetAccountID
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = cix.TransactionID
                         |WHERE replace(substring(trans.TransactionDate, 1, 7),'-','') = replace(substring(cast('$PV_ENDDATE' as timestamp), 1, 7),'-','')
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'YTD_BILL_TRANSFR_TRANS_AMT' AS auditentity, SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_transfertxcontext cix
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_account sa
                         |    ON sa.ID = cix.SourceAccountID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_account ta
                         |    ON ta.ID = cix.TargetAccountID
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = cix.TransactionID
                         |WHERE YEAR(trans.TransactionDate) = YEAR(cast('$PV_ENDDATE' as timestamp))
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'MTD_BILL_NONRECV_TRANS_AMT' AS auditentity, SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_nonreceivableitemctx cix
                         |JOIN $hiveDB.stg_gwbc_bc_basenonrecdistitem bndi
                         |    ON bndi.ID = cix.BaseNonReceivableDistItemID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_account acc
                         |    ON acc.ID = cix.AccountWithSuspenseID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policyperiod polper
                         |    ON polper.ID = bndi.MatchingPolicyID
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = cix.TransactionID
                         |WHERE replace(substring(trans.TransactionDate, 1, 7),'-','') = replace(substring(cast('$PV_ENDDATE' as timestamp), 1, 7),'-','')
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'YTD_BILL_NONRECV_TRANS_AMT' AS auditentity, SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_nonreceivableitemctx cix
                         |JOIN $hiveDB.stg_gwbc_bc_basenonrecdistitem bndi
                         |    ON bndi.ID = cix.BaseNonReceivableDistItemID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_account acc
                         |    ON acc.ID = cix.AccountWithSuspenseID
                         |LEFT JOIN $hiveDB.stg_gwbc_bc_policyperiod polper
                         |    ON polper.ID = bndi.MatchingPolicyID
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = cix.TransactionID
                         |WHERE YEAR(trans.TransactionDate) = YEAR(cast('$PV_ENDDATE' as timestamp))
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'MTD_BILL_CREDIT_TRANS_AMT' AS auditentity, SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_creditcontext cix
                         |JOIN $hiveDB.stg_gwbc_bc_account acc
                         |    ON acc.ID = cix.AccountID
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = cix.TransactionID
                         |WHERE replace(substring(trans.TransactionDate, 1, 7),'-','') = replace(substring(cast('$PV_ENDDATE' as timestamp), 1, 7),'-','')
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)
                         |UNION
                         |SELECT  'YTD_BILL_CREDIT_TRANS_AMT' AS auditentity, SUM(trans.Amount) AS AuditResult
                         |FROM $hiveDB.stg_gwbc_bc_creditcontext cix
                         |JOIN $hiveDB.stg_gwbc_bc_account acc
                         |    ON acc.ID = cix.AccountID
                         |JOIN $hiveDB.stg_gwbc_bc_transaction trans
                         |    ON trans.ID = cix.TransactionID
                         |WHERE YEAR(trans.TransactionDate) = YEAR(cast('$PV_ENDDATE' as timestamp))
                         |        AND trans.TransactionDate <= cast('$PV_ENDDATE' as timestamp)""".stripMargin
}
