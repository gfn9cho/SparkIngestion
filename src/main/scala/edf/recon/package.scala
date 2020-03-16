package edf

import edf.dataingestion.{now, timeZone, hiveDB}
import org.joda.time.format.DateTimeFormat
import java.sql.Timestamp


package object recon {

    val datetime_format = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS")
    val nowString = datetime_format.withZone(timeZone).
                parseDateTime(now.toString("YYYY-MM-dd HH:mm:ss.sss"))
    val PV_ENDDATE = nowString.withTimeAtStartOfDay().
    minusMillis(1).toString("YYYY-MM-dd HH:mm:ss.SSS")


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
}
