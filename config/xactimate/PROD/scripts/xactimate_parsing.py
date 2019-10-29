

def create_aws_objects(user, aws_credentials):
    import boto3

    # create an Athena client
    athena_client = boto3.client('athena'
                                 , region_name=aws_credentials[user]['AWS_REGION']
                                 , aws_access_key_id=aws_credentials[user]['ACCESS_KEY']
                                 , aws_secret_access_key=aws_credentials[user]['SECRET_KEY']
                                 )

    # create an S3 client
    s3_client = boto3.client('s3'
                             , region_name=aws_credentials[user]['AWS_REGION']
                             , aws_access_key_id=aws_credentials[user]['ACCESS_KEY']
                             , aws_secret_access_key=aws_credentials[user]['SECRET_KEY']
                             )

    # create an s3 resource
    s3_resource = boto3.resource('s3'
                                 , region_name=aws_credentials[user]['AWS_REGION']
                                 , aws_access_key_id=aws_credentials[user]['ACCESS_KEY']
                                 , aws_secret_access_key=aws_credentials[user]['SECRET_KEY']
                                 )

    return athena_client, s3_client, s3_resource

def main():

    import pandas as pd
    import xmlschema
    import s3fs
    import pyathena
    import json
    from datetime import datetime

    def retreive_athena_parsed_files():
        anthena_connect = pyathena.connect(aws_access_key_id=aws_credentials['SVC_L4_XACTIMATEPROD']['ACCESS_KEY'],
                                           aws_secret_access_key=aws_credentials['SVC_L4_XACTIMATEPROD']['SECRET_KEY'],
                                           region_name=aws_credentials['SVC_L4_XACTIMATEPROD']['AWS_REGION'],
                                           s3_staging_dir='s3://sa-l4f-emr-edl-claims-secure/processed_claims_vendor/athena_query_results/'
                                           )
        df_athena_parsed_rough_draft_files = pd.read_sql(
            "select distinct file from processed_claims_vendor_secure.xactimate_rough_draft",
            anthena_connect)

        df_athena_parsed_notification_files = pd.read_sql(
            "select distinct file from processed_claims_vendor_secure.xactimate_notification",
            anthena_connect)

        df_athena_parsed_estimate_files = pd.read_sql("select distinct file from processed_claims_vendor_secure.xactimate_estimate",
                                                      anthena_connect)

        df_athena_parsed_failure_files = pd.read_sql("select distinct file from processed_claims_vendor_secure.xactimate_parsing_failures",
                                                      anthena_connect)

        return list(df_athena_parsed_rough_draft_files['file']), list(df_athena_parsed_notification_files['file']), list(
            df_athena_parsed_estimate_files['file']), list(df_athena_parsed_failure_files['file'])

    def export_rough_draft_files():
        def send_to_s3(folder_name, file):
            with fs.open(
                    's3://sa-l4f-emr-edl-claims-secure/claims_vendor/xactimate/{}/{}_{}.csv'.format(folder_name,
                                                                                                        folder_name,
                                                                                                        datetime.now()),
                    'wb') as f:
                f.write(file)

        df = pd.DataFrame(roughDraft_final_list, columns=[
            'fileXMLNS',
            'fileMajorVersion',
            'fileMinorVersion',
            'headerCompName',
            'headerDateCreated',
            'estimateInfoCarrierID',
            'estimateInfoClaimNumber',
            'estimateInfoDeprMat',
            'estimateInfoDeprNonMat',
            'estimateInfoDeprTaxes',
            'estimateInfoEstimateName',
            'estimateInfoEstimateType',
            'estimateInfoInspectionNotPerformed',
            'estimateInfoInsuredName',
            'estimateInfoLaborEff',
            'estimateInfoPolicyNumber',
            'estimateInfoOpeningStatement',
            'dateLoss',
            'dateCompleted',
            'dateReceived',
            'dateEntered',
            'dateContacted',
            'lineItemDetailContractorTotal',
            'lineItemDetailHomeownerTotal',
            'lineItemDetailTotal',
            'recapByRoomTotal',
            'recapCategorySubtotalACV',
            'recapCategorySubtotalDeprec',
            'recapCategorySubtotalRCV',
            'recapCategoryNonOPSubtotalACV',
            'recapCategoryNonOPSubtotalPercentage',
            'recapCategoryNonOPSubtotalDeprec',
            'recapCategoryNonOPSubtotalRCV',
            'recapCategoryOPSubtotalACV',
            'recapCategoryOPSubtotalDeprec',
            'recapCategoryOPSubtotalPercentage',
            'recapCategoryOPSubtotalRCV',
            'recapCategoryOverheadPercentage',
            'recapCategoryOverheadRCV',
            'recapCategoryProfitPercentage',
            'recapCategoryProfitRCV',
            'recapCategoryTaxACV',
            'recapCategoryTaxDeprec',
            'recapCategoryTaxDesc',
            'recapCategoryTaxPercentage',
            'recapCategoryTaxRCV',
            'recapTaxLineItemsOverhead',
            'recapTaxLineItemsProfit',
            'recapTaxOverheadAmt',
            'recapTaxOverheadRate',
            'recapTaxProfitAmt',
            'recapTaxProfitRate',
            'closingStatement',
            'ptTotals',
            'fileTransactionID',
            'file'

        ])
        df_contacts = pd.DataFrame(roughDraft_contacts_list, columns=[
            'contactID',
            'contactType',
            'contactName'
        ])
        df_contacts_address = pd.DataFrame(roughDraft_contacts_address_list, columns=[
            'contactID',
            'contactAddressType',
            'contactAddressStreet',
            'contactAddressCity',
            'contactAddressState',
            'contactAddressPostal',
            'contactAddressPrimary',
            'fileTransactionID',
            'file'
        ])
        df_contacts_phone = pd.DataFrame(roughDraft_contacts_phone_list, columns=[
            'contactID',
            'contactPhonePrimary',
            'contactPhoneType',
            'contactPhoneNumber',
            'fileTransactionID',
            'file'
        ])
        df_coverage = pd.DataFrame(roughDraft_coverage_list, columns=[
            'coverageID',
            'coverageName',
            'coverageType',
            'coverageDeductible',
            'coverageLossDataBD',
            'coverageLossDataNonRecDeprec',
            'coverageLossDataDedutApplied',
            'coverageLossDataInsCarried',
            'coverageLossDataRCVInsCarried',
            'coverageLossDataACVInsCarried',
            'coverageLossDataPotentialSuppClaim',
            'coverageLossDataTotal',
            'coverageLossDataValACV',
            'coverageLossDataValRCV',
            'coverageLossDataSalvage',
            'coverageLossDataDirectReplacement',
            'coverageLossDataPriorPmts',
            'coverageLossDataCoins',
            'coverageLossDataCoinsFormula',
            'coverageLossDataOverLimits',
            'coverageLossDataAcvClaim',
            'coverageLossDataAcvLoss',
            'coverageLossDataAdjLossAmt',
            'coverageLossDataRCL',
            'coverageLossDataRcvClaim',
            'coverageSummaryActualRecDepr',
            'coverageSummaryACV',
            'coverageSummaryDeductible',
            'coverageSummaryDepr',
            'coverageSummaryFullDeduct',
            'coverageSummaryLineItemTotal',
            'coverageSummaryNetClaim',
            'coverageSummaryNetClaimIfRec',
            'coverageSummaryPriorPmts',
            'coverageSummaryPriorPmtsAdj',
            'coverageSummaryRCV',
            'coverageSummaryRecDepr',
            'coverageSummarySubtotal',
            'coverageSummaryPaymentTrackerPTestRemaining',
            'coverageSummaryPaymentTrackerPTestValue',
            'coverageSummaryPaymentTrackerPTItemsWithRemaining',
            'coverageSummaryPaymentTrackerPTnumItemActs',
            'coverageSummaryPaymentTrackerPTRecoverableDep',
            'coverageSummaryPaymentTrackerPTTotalPaid',
            'coverageSummaryOverhead',
            'coverageSummaryProfit',
            'coverageSummarySignature',
            'fileTransactionID',
            'file'

        ])
        df_coversheet_address = pd.DataFrame(roughDraft_coversheet_address_list, columns=[
            'coversheetAddressType',
            'coversheetAddressStreet',
            'coversheetAddressCity',
            'coversheetAddressState',
            'coversheetAddressPostal',
            'coversheetAddressPrimary',
            'fileTransactionID',
            'file'
        ])
        df_coversheet_phone = pd.DataFrame(roughDraft_coversheet_phone_list, columns=[
            'coversheetPhoneType',
            'coversheetPhonePrimary',
            'coversheetPhoneNumber',
            'fileTransactionID',
            'file'
        ])
        df_lineItemDetailTotals = pd.DataFrame(roughDraft_lineItemDetailTotals_list, columns=[
            'lineItemDetailDesc',
            'lineItemDetailacvTotal',
            'lineItemDetailAddons',
            'lineItemDetailrcvTotal',
            'lineItemDetailTax',
            'lineItemDetailTotal',
            'fileTransactionID',
            'file'
        ])
        df_lineItemDetailSF = pd.DataFrame(roughDraft_lineItemDetailSF_list, columns=[
            'lineItemDetailDesc',
            'sfWalls',
            'sfCeiling',
            'sfWallsCeiling',
            'sfFloor',
            'syFloor',
            'lfFloorPerim',
            'lfCeilingPerim',
            'sfSkFloor',
            'sfSkTotalFloor',
            'sfSkIntWall',
            'sfSkExtWall',
            'lfSkExtWallPerim',
            'sfSkRoof',
            'skRoofSquares',
            'lfSkRoofPerim',
            'lfSkRoofRidge',
            'fileTransactionID',
            'file'
        ])
        df_lineItemDetailSFTotals = pd.DataFrame(roughDraft_lineItemDetailSFTotals_list, columns=[
            'lineItemDetailDesc',
            'sfWalls',
            'sfCeiling',
            'sfWallsCeiling',
            'sfFloor',
            'syFloor',
            'lfFloorPerim',
            'lfCeilingPerim',
            'sfSkFloor',
            'sfSkTotalFloor',
            'sfSkIntWall',
            'sfSkExtWall',
            'lfSkExtWallPerim',
            'sfSkRoof',
            'skRoofSquares',
            'lfSkRoofPerim',
            'lfSkRoofRidge',
            'fileTransactionID',
            'file'
        ])
        df_lineItemDetailItems = pd.DataFrame(roughDraft_lineItemDetailItems_list, columns=[
            'descLineage',
            'act',
            'acv',
            'acvTotal',
            'addons',
            'calc',
            'cat',
            'containsBSCDontApply',
            'coverageName',
            'desc',
            'isPartOfInitSettle',
            'laborBase',
            'laborBurden',
            'laborHours',
            'laborMarkup',
            'laborTotal',
            'lineNum',
            'qty',
            'rcvTotal',
            'recoverable',
            'replace',
            'sel',
            'total',
            'type',
            'unit',
            'fileTransactionID',
            'file'
        ])
        df_recapByRoomDetail = pd.DataFrame(roughDraft_recapByRoomDetail_list, columns=[
            'desc',
            'items',
            'itemsPercentage',
            'subtotal',
            'subtotalPercentage',
            'covName',
            'covAmt',
            'covRate',
            'descLineage',
            'fileTransactionID',
            'file'
        ])

        # Tables not currently needed but the logic is ready to go when needed
        #
        # df_ptTotals = pd.DataFrame(roughDraft_payments_list, columns=[
        #     'paymentDate',
        #     'paymentUserID',
        #     'fileTransactionID',
        #     'file'
        # ])
        # df_recapByCategory = pd.DataFrame(roughDraft_recapByCategory_list, columns=[
        #     'categoryID',
        #     'recapCategoryCategoryACV',
        #     'recapCategoryCategoryDesc',
        #     'recapCategoryCategoryRCV',
        #     'fileTransactionID',
        #     'file'
        #
        # ])
        # df_recapByCategoryCov = pd.DataFrame(roughDraft_recapByCategoryCov_list, columns=[
        #     'categoryID',
        #     'categoryType',
        #     'CovName',
        #     'CovRate',
        #     'CovAmt',
        #     'fileTransactionID',
        #     'file'
        # ])
        # df_recapByCategorySalesTaxes = pd.DataFrame(roughDraft_recapByCategorySalesTaxes_list, columns=[
        #     'recapCategoryTaxACV',
        #     'recapCategoryTaxDeprec',
        #     'recapCategoryTaxDesc',
        #     'recapCategoryTaxPercentage',
        #     'recapCategoryTaxRCV',
        #     'covAmt',
        #     'covName',
        #     'covRate',
        #     'fileTransactionID',
        #     'file'
        # ])
        # df_recapTaxLineItemsTaxNum = pd.DataFrame(roughDraft_recapTaxLineItemsTaxNum_list, columns=[
        #     'recapTaxLineItemsAmt',
        #     'recapTaxLineItemsTaxNum',
        #     'fileTransactionID',
        #     'file'
        #
        # ])
        # df_recapTaxOP = pd.DataFrame(roughDraft_recapTaxOP_list, columns=[
        #     'taxDetailNumber',
        #     'taxDetailRate',
        #     'taxDetailDesc',
        #     'fileTransactionID',
        #     'file'
        # ])
        # df_summaryTaxes = pd.DataFrame(roughDraft_summaryTaxes_list, columns=[
        #     'coverageID',
        #     'coverageSummarySalesTaxAmount',
        #     'coverageSummarySalesTaxDesc',
        #     'coverageSummarySalesTaxOP',
        #     'fileTransactionID',
        #     'file'
        # ])

        bytes_df = df.to_csv(None, index=False).encode()
        bytes_df_contacts = df_contacts.to_csv(None, index=False).encode()
        bytes_df_contacts_address = df_contacts_address.to_csv(None, index=False).encode()
        bytes_df_contacts_phone = df_contacts_phone.to_csv(None, index=False).encode()
        bytes_df_coverage = df_coverage.to_csv(None, index=False).encode()
        bytes_df_coversheet_address = df_coversheet_address.to_csv(None, index=False).encode()
        bytes_df_coversheet_phone = df_coversheet_phone.to_csv(None, index=False).encode()
        bytes_df_lineItemDetail_Totals = df_lineItemDetailTotals.to_csv(None, index=False).encode()
        bytes_df_lineItemDetail_SF = df_lineItemDetailSF.to_csv(None, index=False).encode()
        bytes_df_lineItemDetail_SFTotals = df_lineItemDetailSFTotals.to_csv(None, index=False).encode()
        bytes_df_lineItemDetail_Items = df_lineItemDetailItems.to_csv(None, index=False).encode()
        bytes_df_recapByRoomDetail = df_recapByRoomDetail.to_csv(None, index=False).encode()

        # Tables not currently needed but the logic is ready to go when needed
        #
        # bytes_df_item = df_item.to_csv(None, index=False).encode()
        # bytes_df_item_act = df_item_act.to_csv(None, index=False).encode()
        # bytes_df_ptTotals = df_ptTotals.to_csv(None, index=False).encode()
        # bytes_df_recapByCategory = df_recapByCategory.to_csv(None, index=False).encode()
        # bytes_df_recapByCategoryCov = df_recapByCategoryCov.to_csv(None, index=False).encode()
        # bytes_df_recapByCategorySalesTaxes = df_recapByCategorySalesTaxes.to_csv(None, index=False).encode()
        # bytes_df_recapTaxLineItemsTaxNum = df_recapTaxLineItemsTaxNum.to_csv(None, index=False).encode()
        # bytes_df_recapTaxOP = df_recapTaxOP.to_csv(None, index=False).encode()
        # bytes_df_summaryTaxes = df_summaryTaxes.to_csv(None, index=False).encode()

        send_to_s3(folder_name='rough_draft', file=bytes_df)
        send_to_s3(folder_name='rough_draft_contacts', file=bytes_df_contacts)
        send_to_s3(folder_name='rough_draft_contacts_address', file=bytes_df_contacts_address)
        send_to_s3(folder_name='rough_draft_contacts_phone', file=bytes_df_contacts_phone)
        send_to_s3(folder_name='rough_draft_coverage', file=bytes_df_coverage)
        send_to_s3(folder_name='rough_draft_coversheet_address', file=bytes_df_coversheet_address)
        send_to_s3(folder_name='rough_draft_coversheet_phone', file=bytes_df_coversheet_phone)
        send_to_s3(folder_name='rough_draft_lineItemDetail_Totals', file=bytes_df_lineItemDetail_Totals)
        send_to_s3(folder_name='rough_draft_lineItemDetail_SF', file=bytes_df_lineItemDetail_SF)
        send_to_s3(folder_name='rough_draft_lineItemDetail_SF_Totals', file=bytes_df_lineItemDetail_SFTotals)
        send_to_s3(folder_name='rough_draft_lineItemDetail_Items', file=bytes_df_lineItemDetail_Items)
        send_to_s3(folder_name='rough_draft_recapByRoom', file=bytes_df_recapByRoomDetail)

    def export_estimate_files():
        def send_to_s3(folder_name, file):
            with fs.open(
                    's3://sa-l4f-emr-edl-claims-secure/processed_claims_vendor/xactimate/{}/{}_{}.csv'.format(folder_name,
                                                                                                        folder_name,
                                                                                                        datetime.now()),
                    'wb') as f:
                f.write(file)

        df = pd.DataFrame(estimate_final_list, columns=[
            'XactnetInfoAssignmentType',
            'XactnetInfoBusinessUnit',
            'XactnetCarrierID',
            'XactnetCarrierName',
            'XactnetCarrierOffice1',
            'XactnetCarrierOffice2',
            'XactnetCreatorEmailAddress',
            'XactnetCreatorFirstName',
            'XactnetCreatorLastName',
            'XactnetCreatorUserNumber',
            'XactnetEmergency',
            'XactnetEstimateCount',
            'XactnetJobSizeCode',
            'XactnetMitigation',
            'XactnetProfileCode',
            'XactnetRecipientsXM8UserID',
            'XactnetrotationTrade',
            'XactnetSenderID',
            'XactnetSendersOfficeDescription1',
            'XactnetSendersOfficeDescription2',
            'XactnetSendersXNAddress',
            'XactnetTransactionId',
            'XactnetTransactionType',
            'summaryContractorItems',
            'summaryDeductible',
            'summaryEstimateLineItemTotal',
            'summaryGrossEstimate',
            'summaryHomeOwnerItems',
            'summaryMinimumChargeAdjustments',
            'summaryNetEstimate',
            'summaryNonRecoverableDepreciation',
            'summaryOverhead',
            'summaryPriceListLineItemTotal',
            'summaryProfit',
            'summaryRecoverableDepreciation',
            'summarySalesTax',
            'projectInfoAssignmentCode',
            'projectInfoCreated',
            'projectInfoName',
            'projectInfoProfile',
            'projectInfoShowDeskAdjuster',
            'projectInfoShowIADeskAdjuster',
            'projectInfoStatus',
            'projectInfoUserID',
            'projectInfoVersion',
            'projectInfoXactnetAddress',
            'projectInfoNotes',
            'paramsCheckpointPL',
            'paramsCumulativeOP',
            'paramsDefaultRepairedBy',
            'paramsDepMat',
            'paramsDepNonMat',
            'paramsDepTaxes',
            'paramsMaxDepr',
            'paramsOverhead',
            'paramsPlModifiedDateTime',
            'paramsPriceList',
            'paramsProfit',
            'paramsTaxJurisdiction',
            'admAgentCode',
            'admDateContacted',
            'admDateEntered',
            'admDateInspected',
            'admDateOfLoss',
            'admDateProjCompleted',
            'admDateReceived',
            'admDenial',
            'admFileNumber',
            'admCoverageLossType',
            'admCoverageLossDesc',
            'admLossOfUseReserve',
            'admLossOfUse',
            'admDoNotApplyLimits',
            'adminBranch',
            'adminAccessName',
            'adminAccessPhone',
            'file'
        ])
        df_contacts = pd.DataFrame(estimate_contacts_list, columns=[
            'contactID',
            'contactBirthdate',
            'contactLanguage',
            'contactName',
            'contactQCode',
            'contactReference',
            'contactTitle',
            'contactType'
        ])
        df_minimums = pd.DataFrame(estimate_EMBEDDED_PL_minimums_list, columns=[
            'minimumAmt',
            'minimumCAT',
            'minimumDesc',
            'minimumID',
            'minimumSEL',
            'XactnetTransactionId',
            'file'
        ])
        df_address = pd.DataFrame(estimate_address_list, columns=[
            'contactID',
            'contactAddressCity',
            'contactAddressCounty',
            'contactAddressPostal',
            'contactAddressState',
            'contactAddressStreet',
            'contactAddressType',
            'admClaimNumber',
            'admPolicyNumber',
            'addressCount',
            'XactnetTransactionId',
            'file'
        ])
        df_coverage = pd.DataFrame(estimate_coverage_list, columns=[
            'covApplyTo',
            'covCoins',
            'covName',
            'covType',
            'covDeductible',
            'covID',
            'covPolicyLimit',
            'covReserveAmt',
            'covDeductApplied',
            'covOverLimits',
            'admClaimNumber',
            'admPolicyNumber',
            'admCat',
            'XactnetTransactionId',
            'file'
        ])
        df_phone = pd.DataFrame(estimate_phone_list, columns=[
            'contactID',
            'contactPhoneType',
            'contactPhoneNumber',
            'admClaimNumber',
            'admPolicyNumber',
            'contactPhoneCount',
            'XactnetTransactionId',
            'file'
        ])
        df_email = pd.DataFrame(estimate_email_list, columns=[
            'contactID',
            'contactEmail',
            'admClaimNumber',
            'admPolicyNumber',
            'contactEmailCount',
            'XactnetTransactionId', 'file'
        ])
        df_attachments = pd.DataFrame(estimate_attachments_list, columns=[
            'attFileDate',
            'attFileDesc',
            'attFileName',
            'attFileType',
            'XactnetTransactionId',
            'file'
        ])
        df_control_points = pd.DataFrame(estimate_control_points_list, columns=[
            'controlPointStamp',
            'controlPointType',
            'controlPointsReferral',
            'controlPointsTestAssignment',
            'XactnetTransactionId',
            'file'
        ])

        bytes_df = df.to_csv(None, index=False).encode()
        bytes_df_contacts = df_contacts.to_csv(None, index=False).encode()
        bytes_df_minimums = df_minimums.to_csv(None, index=False).encode()
        bytes_df_address = df_address.to_csv(None, index=False).encode()
        bytes_df_coverage = df_coverage.to_csv(None, index=False).encode()
        bytes_df_phone = df_phone.to_csv(None, index=False).encode()
        bytes_df_email = df_email.to_csv(None, index=False).encode()
        bytes_df_attachments = df_attachments.to_csv(None, index=False).encode()
        bytes_df_control_points = df_control_points.to_csv(None, index=False).encode()

        send_to_s3(folder_name='estimate', file=bytes_df)
        send_to_s3(folder_name='estimate_contacts', file=bytes_df_contacts)
        send_to_s3(folder_name='estimate_minimums', file=bytes_df_minimums)
        send_to_s3(folder_name='estimate_address', file=bytes_df_address)
        send_to_s3(folder_name='estimate_coverage', file=bytes_df_coverage)
        send_to_s3(folder_name='estimate_phone', file=bytes_df_phone)
        send_to_s3(folder_name='estimate_email', file=bytes_df_email)
        send_to_s3(folder_name='estimate_attachments', file=bytes_df_attachments)
        send_to_s3(folder_name='estimate_control_points', file=bytes_df_control_points)

    def export_notification_files():
        def send_to_s3(folder_name, file):
            with fs.open(
                    's3://sa-l4f-emr-edl-claims-secure/processed_claims_vendor/xactimate/{}/{}_{}.csv'.format(folder_name,
                                                                                                        folder_name,
                                                                                                        datetime.now()),
                    'wb') as f:
                f.write(file)

        df_notification = pd.DataFrame(notification_final_list, columns=[
            'claimNumber',
            'xactTransactionId',
            'recipients',
            'recipientUserID',
            'timestamp',
            'status',
            'contactType',
            'contactName',
            'file',
        ])
        df_notification_phone = pd.DataFrame(notification_phone_list, columns=[
            'contactName',
            'contactType',
            'contactPhoneType',
            'contactPhoneNumber',
            'contactPhoneExtension',
            'xactTransactionId',
            'file'
        ])
        df_notification_email = pd.DataFrame(notification_email_list, columns=[
            'contactName',
            'contactType',
            'contactEmail',
            'xactTransactionId',
            'file'
        ])

        bytes_df_notification = df_notification.to_csv(None, index=False).encode()
        bytes_df_notification_phone = df_notification_phone.to_csv(None, index=False).encode()
        bytes_df_notification_email = df_notification_email.to_csv(None, index=False).encode()

        send_to_s3(folder_name='notification', file=bytes_df_notification)
        send_to_s3(folder_name='notification_email', file=bytes_df_notification_email)
        send_to_s3(folder_name='notification_phone', file=bytes_df_notification_phone)

    def export_parsing_support_files():
        def send_to_s3(folder_name, file):
            print("exporting summary and failure files")
            with fs.open(
                    's3://sa-l4f-emr-edl-claims-secure/processed_claims_vendor/xactimate_summary/{}/{}_{}.csv'.format(folder_name,
                                                                                                        folder_name,
                                                                                                        datetime.now()),
                    'wb') as f:
                f.write(file)

        df_parsing_summary = pd.DataFrame(parsing_summary_list, columns=[
            'startTime',
            'endTime',
            'duration',
            'estimateSuccesses',
            'roughDraftSuccesses',
            'notificationSuccesses',
            'estimateFailures',
            'roughDraftFailures',
            'notificationFailures',
            'totalFilesScanned'
        ])
        df_parsing_failures = pd.DataFrame(fileFailures_list, columns=[
            'errorTime',
            'fileType',
            'file'
        ])

        bytes_df_parsing_summary = df_parsing_summary.to_csv(None, index=False).encode()
        bytes_df_parsing_failures = df_parsing_failures.to_csv(None, index=False).encode()

        send_to_s3(folder_name='parsing_summary', file=bytes_df_parsing_summary)
        send_to_s3(folder_name='parsing_failures', file=bytes_df_parsing_failures)

    def notification_run(file, notification_text):

        note_dict = notification_schema.to_dict(notification_text)

        file = file.key

        claimNumber = note_dict['ADM']['TYPESOFLOSS']['TYPEOFLOSS']['@claimNumber']

        xact_ID = note_dict['XACTNET_INFO']['@transactionId']
        recipients = note_dict['XACTNET_INFO']['@recipientsXNAddress']
        recipient_user_ID = note_dict['XACTNET_INFO']['@recipientsXM8UserId']

        timestamp = note_dict['XACTNET_INFO']['CONTROL_POINTS']['CONTROL_POINT']['@stamp']
        status = note_dict['XACTNET_INFO']['CONTROL_POINTS']['CONTROL_POINT']['@type']

        contact_type = note_dict['CONTACTS']['CONTACT']['@type']
        contact_name = note_dict['CONTACTS']['CONTACT']['@name']

        if 'CONTACTMETHODS' in note_dict['CONTACTS']['CONTACT']:
            if note_dict['CONTACTS']['CONTACT']['CONTACTMETHODS'] != None:
                if 'PHONE' in note_dict['CONTACTS']['CONTACT']['CONTACTMETHODS']:
                    if note_dict['CONTACTS']['CONTACT']['CONTACTMETHODS']['PHONE'] != None:
                        contactPhoneCount = len(note_dict['CONTACTS']['CONTACT']['CONTACTMETHODS']['PHONE'])
                        if contactPhoneCount > 0:
                            for phone in note_dict['CONTACTS']['CONTACT']['CONTACTMETHODS']['PHONE']:
                                contactPhoneType = phone['@type']
                                contactPhoneNumber = phone['@number']
                                contactPhoneExtension = phone['@extension']

                                notification_phone_list.append([
                                    contact_type,
                                    contact_name,
                                    contactPhoneType,
                                    contactPhoneNumber,
                                    contactPhoneExtension,
                                    xact_ID,
                                    file

                                ])
                        else:

                            notification_phone_list.append([
                                contact_type,
                                contact_name,
                                '',
                                '',
                                '',
                                xact_ID,
                                file
                            ])

                if 'EMAIL' in note_dict['CONTACTS']['CONTACT']['CONTACTMETHODS']:
                    if note_dict['CONTACTS']['CONTACT']['CONTACTMETHODS'] != None:
                        contactEmailCount = len(note_dict['CONTACTS']['CONTACT']['CONTACTMETHODS']['EMAIL'])
                        if contactEmailCount > 0:
                            for email in note_dict['CONTACTS']['CONTACT']['CONTACTMETHODS']['EMAIL']:
                                contactEmail = email['@address']

                                notification_email_list.append([
                                    contact_type,
                                    contact_name,
                                    contactEmail,
                                    xact_ID,
                                    file
                                ])
                        else:

                            notification_email_list.append([
                                contact_type,
                                contact_name,
                                '',
                                xact_ID,
                                file
                            ])
            else:

                notification_phone_list.append([
                    contact_type,
                    contact_name,
                    '',
                    '',
                    '',
                    xact_ID,
                    file
                ])

                notification_email_list.append([
                    contact_type,
                    contact_name,
                    '',
                    xact_ID,
                    file
                ])

        notification_final_list.append([
            claimNumber,
            xact_ID,
            recipients,
            recipient_user_ID,
            timestamp,
            status,
            contact_type,
            contact_name,
            file
        ])

    def roughDraft_run(file,estimate_test):

        def recapByRoom(dict):

            def recapByRoomRecursiveGroup(dictPartition, descLineage):

                if 'RECAP_GROUP' in dictPartition:
                    for group in dictPartition['RECAP_GROUP']:

                        if '@desc' in group:
                            recapByRoomGroupDesc = group['@desc']
                        else:
                            recapByRoomGroupDesc = ''

                        if '@items' in group:
                            recapByRoomGroupItems = group['@items']
                        else:
                            recapByRoomGroupItems = ''

                        if '@itemsPercentage' in group:
                            recapByRoomGroupItemsPercentage = group['@itemsPercentage']
                        else:
                            recapByRoomGroupItemsPercentage = ''

                        if '@subtotal' in group:
                            recapByRoomGroupSubtotal = group['@subtotal']
                        else:
                            recapByRoomGroupSubtotal = ''

                        if '@subtotalPercentages' in group:
                            recapByRoomGroupSubtotalPercentages = group['@subtotalPercentages']
                        else:
                            recapByRoomGroupSubtotalPercentages = ''

                        if 'ITEMS_COV_INFO' in group:
                            if 'COV' in group['ITEMS_COV_INFO']:
                                for cov in group['ITEMS_COV_INFO']['COV']:
                                    if '@name' in cov:
                                        covName = cov['@name']
                                    else:
                                        covName = ''

                                    if '@amount' in cov:
                                        covAMT = cov['@amount']
                                    else:
                                        covAMT = ''

                                    if '@rate' in cov:
                                        covRate = cov['@rate']
                                    else:
                                        covRate = ''

                                    roughDraft_recapByRoomDetail_list.append([
                                        recapByRoomGroupDesc,
                                        recapByRoomGroupItems,
                                        recapByRoomGroupItemsPercentage,
                                        recapByRoomGroupSubtotal,
                                        recapByRoomGroupSubtotalPercentages,
                                        covName,
                                        covAMT,
                                        covRate,
                                        descLineage + ' || ' + recapByRoomGroupDesc,
                                        fileTransactionID,
                                        file
                                    ])
                        else:
                            covName = ''
                            covAMT = ''
                            covRate = ''

                            roughDraft_recapByRoomDetail_list.append([
                                recapByRoomGroupDesc,
                                recapByRoomGroupItems,
                                recapByRoomGroupItemsPercentage,
                                recapByRoomGroupSubtotal,
                                recapByRoomGroupSubtotalPercentages,
                                covName,
                                covAMT,
                                covRate,
                                descLineage + ' || ' + recapByRoomGroupDesc,
                                fileTransactionID,
                                file
                            ])

                        if 'RECAP_GROUPS' in group:
                            if descLineage != '':
                                subDescLineage = '{} || {}'.format(descLineage, recapByRoomGroupDesc)
                            else:
                                subDescLineage = recapByRoomGroupDesc
                            recapByRoomRecursiveGroup(group['RECAP_GROUPS'], subDescLineage)

            if '@total' in dict:
                recapByRoomTotal = dict['@total']
            else:
                recapByRoomTotal = ''

            descLineage = ''

            if 'RECAP_GROUP' in dict:
                if '@desc' in dict['RECAP_GROUP']:
                    recapByRoomGroupDesc = dict['RECAP_GROUP']['@desc']
                else:
                    recapByRoomGroupDesc = ''

                if '@items' in dict['RECAP_GROUP']:
                    recapByRoomGroupItems = dict['RECAP_GROUP']['@items']
                else:
                    recapByRoomGroupItems = ''

                if '@itemsPercentage' in dict['RECAP_GROUP']:
                    recapByRoomGroupItemsPercentage = dict['RECAP_GROUP']['@itemsPercentage']
                else:
                    recapByRoomGroupItemsPercentage = ''

                if '@subtotal' in dict['RECAP_GROUP']:
                    recapByRoomGroupSubtotal = dict['RECAP_GROUP']['@subtotal']
                else:
                    recapByRoomGroupSubtotal = ''

                if '@subtotalPercentages' in dict['RECAP_GROUP']:
                    recapByRoomGroupSubtotalPercentages = dict['RECAP_GROUP']['@subtotalPercentages']
                else:
                    recapByRoomGroupSubtotalPercentages = ''

                if recapByRoomGroupDesc != '':
                    descLineage = recapByRoomGroupDesc

                if 'ITEMS_COV_INFO' in dict:
                    if 'COV' in dict['ITEMS_COV_INFO']:
                        for cov in dict['ITEMS_COV_INFO']['COV']:
                            if '@name' in cov:
                                covName = cov['@name']
                            else:
                                covName = ''

                            if '@amount' in cov:
                                covAMT = cov['@amount']
                            else:
                                covAMT = ''

                            if '@rate' in cov:
                                covRate = cov['@rate']
                            else:
                                covRate = ''

                            roughDraft_recapByRoomDetail_list.append([
                                recapByRoomGroupDesc,
                                recapByRoomGroupItems,
                                recapByRoomGroupItemsPercentage,
                                recapByRoomGroupSubtotal,
                                recapByRoomGroupSubtotalPercentages,
                                covName,
                                covAMT,
                                covRate,
                                descLineage,
                                fileTransactionID,
                                file
                            ])
                else:
                    covName = ''
                    covAMT = ''
                    covRate = ''

                    roughDraft_recapByRoomDetail_list.append([
                        recapByRoomGroupDesc,
                        recapByRoomGroupItems,
                        recapByRoomGroupItemsPercentage,
                        recapByRoomGroupSubtotal,
                        recapByRoomGroupSubtotalPercentages,
                        covName,
                        covAMT,
                        covRate,
                        descLineage,
                        fileTransactionID,
                        file
                    ])

                if 'RECAP_GROUPS' in dict['RECAP_GROUP']:
                    recapByRoomRecursiveGroup(dict['RECAP_GROUP']['RECAP_GROUPS'], descLineage)

                return recapByRoomTotal

        def lineItemDetail(dict):
            def lineItemDetailRecursiveGroup(dictPartition, descLineage):
                if 'GROUP' in dictPartition:
                    for group in dictPartition['GROUP']:

                        if '@acvTotal' in group:
                            groupacvTotal = group['@acvTotal']
                        else:
                            groupacvTotal = ''

                        if '@addons' in group:
                            groupAddons = group['@addons']
                        else:
                            groupAddons = ''

                        if '@desc' in group:
                            groupDesc = group['@desc']
                        else:
                            groupDesc = ''

                        if '@rcvTotal' in group:
                            grouprcvTotal = group['@rcvTotal']
                        else:
                            grouprcvTotal = ''

                        if '@tax' in group:
                            groupTax = group['@tax']
                        else:
                            groupTax = ''

                        if '@total' in group:
                            groupTotal = group['@total']
                        else:
                            groupTotal = ''

                        if descLineage != '':
                            tempDescLineage = descLineage + ' || ' + groupDesc
                        else:
                            tempDescLineage = groupDesc

                        roughDraft_lineItemDetailTotals_list.append([
                            tempDescLineage,
                            groupacvTotal,
                            groupAddons,
                            grouprcvTotal,
                            groupTax,
                            groupTotal,
                            fileTransactionID,
                            file
                        ])

                        if 'ROOM_DIM_VARS' in group:

                            if '@sfWalls' in group['ROOM_DIM_VARS']:
                                sfWalls = group['ROOM_DIM_VARS']['@sfWalls']
                            else:
                                sfWalls = ''

                            if '@sfCeiling' in group['ROOM_DIM_VARS']:
                                sfCeiling = group['ROOM_DIM_VARS']['@sfCeiling']
                            else:
                                sfCeiling = ''

                            if '@sfWallsCeiling' in group['ROOM_DIM_VARS']:
                                sfWallsCeiling = group['ROOM_DIM_VARS']['@sfWallsCeiling']
                            else:
                                sfWallsCeiling = ''

                            if '@sfFloor' in group['ROOM_DIM_VARS']:
                                sfFloor = group['ROOM_DIM_VARS']['@sfFloor']
                            else:
                                sfFloor = ''

                            if '@syFloor' in group['ROOM_DIM_VARS']:
                                syFloor = group['ROOM_DIM_VARS']['@syFloor']
                            else:
                                syFloor = ''

                            if '@lfFloorPerim' in group['ROOM_DIM_VARS']:
                                lfFloorPerim = group['ROOM_DIM_VARS']['@lfFloorPerim']
                            else:
                                lfFloorPerim = ''

                            if '@lfCeilingPerim' in group['ROOM_DIM_VARS']:
                                lfCeilingPerim = group['ROOM_DIM_VARS']['@lfCeilingPerim']
                            else:
                                lfCeilingPerim = ''

                            if '@sfSkFloor' in group['ROOM_DIM_VARS']:
                                sfSkFloor = group['ROOM_DIM_VARS']['@sfSkFloor']
                            else:
                                sfSkFloor = ''

                            if '@sfSkTotalFloor' in group['ROOM_DIM_VARS']:
                                sfSkTotalFloor = group['ROOM_DIM_VARS']['@sfSkTotalFloor']
                            else:
                                sfSkTotalFloor = ''

                            if '@sfSkIntWall' in group['ROOM_DIM_VARS']:
                                sfSkIntWall = group['ROOM_DIM_VARS']['@sfSkIntWall']
                            else:
                                sfSkIntWall = ''

                            if '@sfSkExtWall' in group['ROOM_DIM_VARS']:
                                sfSkExtWall = group['ROOM_DIM_VARS']['@sfSkExtWall']
                            else:
                                sfSkExtWall = ''

                            if '@lfSkExtWallPerim' in group['ROOM_DIM_VARS']:
                                lfSkExtWallPerim = group['ROOM_DIM_VARS']['@lfSkExtWallPerim']
                            else:
                                lfSkExtWallPerim = ''

                            if '@sfSkRoof' in group['ROOM_DIM_VARS']:
                                sfSkRoof = group['ROOM_DIM_VARS']['@sfSkRoof']
                            else:
                                sfSkRoof = ''

                            if '@skRoofSquares' in group['ROOM_DIM_VARS']:
                                skRoofSquares = group['ROOM_DIM_VARS']['@skRoofSquares']
                            else:
                                skRoofSquares = ''

                            if '@lfSkRoofPerim' in group['ROOM_DIM_VARS']:
                                lfSkRoofPerim = group['ROOM_DIM_VARS']['@lfSkRoofPerim']
                            else:
                                lfSkRoofPerim = ''

                            if '@lfSkRoofRidge' in group['ROOM_DIM_VARS']:
                                lfSkRoofRidge = group['ROOM_DIM_VARS']['@lfSkRoofRidge']
                            else:
                                lfSkRoofRidge = ''

                            roughDraft_lineItemDetailSF_list.append([
                                tempDescLineage,
                                sfWalls,
                                sfCeiling,
                                sfWallsCeiling,
                                sfFloor,
                                syFloor,
                                lfFloorPerim,
                                lfCeilingPerim,
                                sfSkFloor,
                                sfSkTotalFloor,
                                sfSkIntWall,
                                sfSkExtWall,
                                lfSkExtWallPerim,
                                sfSkRoof,
                                skRoofSquares,
                                lfSkRoofPerim,
                                lfSkRoofRidge,
                                fileTransactionID,
                                file
                            ])

                        if 'ITEMS' in group:
                            if 'ITEM' in group['ITEMS']:
                                for item in group['ITEMS']['ITEM']:
                                    if '@act' in item:
                                        act = item['@act']
                                    else:
                                        act = ''

                                    if '@acv' in item:
                                        acv = item['@acv']
                                    else:
                                        acv = ''

                                    if '@acvTotal' in item:
                                        acvTotal = item['@acvTotal']
                                    else:
                                        acvTotal = ''

                                    if '@addons' in item:
                                        addons = item['@addons']
                                    else:
                                        addons = ''

                                    if '@calc' in item:
                                        calc = item['@calc']
                                    else:
                                        calc = ''

                                    if '@cat' in item:
                                        cat = item['@cat']
                                    else:
                                        cat = ''

                                    if 'containsBSCDontApply' in item:
                                        containsBSCDontApply = item['@containsBSCDontApply']
                                    else:
                                        containsBSCDontApply = ''

                                    if '@coverageName' in item:
                                        coverageName = item['@coverageName']
                                    else:
                                        coverageName = ''

                                    if '@desc' in item:
                                        desc = item['@desc']
                                    else:
                                        desc = ''

                                    if '@isPartOfInitSettle' in item:
                                        isPartOfInitSettle = item['@isPartOfInitSettle']
                                    else:
                                        isPartOfInitSettle = ''

                                    if '@laborBase' in item:
                                        laborBase = item['@laborBase']
                                    else:
                                        laborBase = ''

                                    if '@laborBurden' in item:
                                        laborBurden = item['@laborBurden']
                                    else:
                                        laborBurden = ''

                                    if '@laborHours' in item:
                                        laborHours = item['@laborHours']
                                    else:
                                        laborHours = ''

                                    if '@laborMarkup' in item:
                                        laborMarkup = item['@laborMarkup']
                                    else:
                                        laborMarkup = ''

                                    if '@laborTotal' in item:
                                        laborTotal = item['@laborTotal']
                                    else:
                                        laborTotal = ''

                                    if '@lineNum' in item:
                                        lineNum = item['@lineNum']
                                    else:
                                        lineNum = ''

                                    if '@qty' in item:
                                        qty = item['@qty']
                                    else:
                                        qty = ''

                                    if '@rcvTotal' in item:
                                        rcvTotal = item['@rcvTotal']
                                    else:
                                        rcvTotal = ''

                                    if '@recoverable' in item:
                                        recoverable = item['@recoverable']
                                    else:
                                        recoverable = ''

                                    if '@replace' in item:
                                        replace = item['@replace']
                                    else:
                                        replace = ''

                                    if '@sel' in item:
                                        sel = item['@sel']
                                    else:
                                        sel = ''

                                    if '@total' in item:
                                        total = item['@total']
                                    else:
                                        total = ''

                                    if '@type' in item:
                                        type = item['@type']
                                    else:
                                        type = ''

                                    if '@unit' in item:
                                        unit = item['@unit']
                                    else:
                                        unit = ''

                                    roughDraft_lineItemDetailItems_list.append([
                                        tempDescLineage,
                                        act,
                                        acv,
                                        acvTotal,
                                        addons,
                                        calc,
                                        cat,
                                        containsBSCDontApply,
                                        coverageName,
                                        desc,
                                        isPartOfInitSettle,
                                        laborBase,
                                        laborBurden,
                                        laborHours,
                                        laborMarkup,
                                        laborTotal,
                                        lineNum,
                                        qty,
                                        rcvTotal,
                                        recoverable,
                                        replace,
                                        sel,
                                        total,
                                        type,
                                        unit,
                                        fileTransactionID,
                                        file
                                    ])

                        if 'SUBROOMS' in group:
                            if 'SUBROOM' in group['SUBROOMS']:

                                for room in group['SUBROOMS']['SUBROOM']:

                                    if '@desc' in room:
                                        desc = room['@desc']
                                    else:
                                        desc = ''

                                    if descLineage != '':
                                        subroomDescLineage = tempDescLineage + ' || ' + desc
                                    else:
                                        subroomDescLineage = desc

                                    if 'ROOM_DIM_VARS' in room:
                                        if '@sfWalls' in room['ROOM_DIM_VARS']:
                                            sfWalls = room['ROOM_DIM_VARS']['@sfWalls']
                                        else:
                                            sfWalls = ''

                                        if '@sfCeiling' in room['ROOM_DIM_VARS']:
                                            sfCeiling = room['ROOM_DIM_VARS']['@sfCeiling']
                                        else:
                                            sfCeiling = ''

                                        if '@sfWallsCeiling' in room['ROOM_DIM_VARS']:
                                            sfWallsCeiling = room['ROOM_DIM_VARS']['@sfWallsCeiling']
                                        else:
                                            sfWallsCeiling = ''

                                        if '@sfFloor' in room['ROOM_DIM_VARS']:
                                            sfFloor = room['ROOM_DIM_VARS']['@sfFloor']
                                        else:
                                            sfFloor = ''

                                        if '@syFloor' in room['ROOM_DIM_VARS']:
                                            syFloor = room['ROOM_DIM_VARS']['@syFloor']
                                        else:
                                            syFloor = ''

                                        if '@lfFloorPerim' in room['ROOM_DIM_VARS']:
                                            lfFloorPerim = room['ROOM_DIM_VARS']['@lfFloorPerim']
                                        else:
                                            lfFloorPerim = ''

                                        if '@lfCeilingPerim' in room['ROOM_DIM_VARS']:
                                            lfCeilingPerim = room['ROOM_DIM_VARS']['@lfCeilingPerim']
                                        else:
                                            lfCeilingPerim = ''

                                        if '@sfSkFloor' in room['ROOM_DIM_VARS']:
                                            sfSkFloor = room['ROOM_DIM_VARS']['@sfSkFloor']
                                        else:
                                            sfSkFloor = ''

                                        if '@sfSkTotalFloor' in room['ROOM_DIM_VARS']:
                                            sfSkTotalFloor = room['ROOM_DIM_VARS']['@sfSkTotalFloor']
                                        else:
                                            sfSkTotalFloor = ''

                                        if '@sfSkIntWall' in room['ROOM_DIM_VARS']:
                                            sfSkIntWall = room['ROOM_DIM_VARS']['@sfSkIntWall']
                                        else:
                                            sfSkIntWall = ''

                                        if '@sfSkExtWall' in room['ROOM_DIM_VARS']:
                                            sfSkExtWall = room['ROOM_DIM_VARS']['@sfSkExtWall']
                                        else:
                                            sfSkExtWall = ''

                                        if '@lfSkExtWallPerim' in room['ROOM_DIM_VARS']:
                                            lfSkExtWallPerim = room['ROOM_DIM_VARS']['@lfSkExtWallPerim']
                                        else:
                                            lfSkExtWallPerim = ''

                                        if '@sfSkRoof' in room['ROOM_DIM_VARS']:
                                            sfSkRoof = room['ROOM_DIM_VARS']['@sfSkRoof']
                                        else:
                                            sfSkRoof = ''

                                        if '@skRoofSquares' in room['ROOM_DIM_VARS']:
                                            skRoofSquares = room['ROOM_DIM_VARS']['@skRoofSquares']
                                        else:
                                            skRoofSquares = ''

                                        if '@lfSkRoofPerim' in room['ROOM_DIM_VARS']:
                                            lfSkRoofPerim = room['ROOM_DIM_VARS']['@lfSkRoofPerim']
                                        else:
                                            lfSkRoofPerim = ''

                                        if '@lfSkRoofRidge' in room['ROOM_DIM_VARS']:
                                            lfSkRoofRidge = room['ROOM_DIM_VARS']['@lfSkRoofRidge']
                                        else:
                                            lfSkRoofRidge = ''

                                        roughDraft_lineItemDetailSF_list.append([
                                            subroomDescLineage,
                                            sfWalls,
                                            sfCeiling,
                                            sfWallsCeiling,
                                            sfFloor,
                                            syFloor,
                                            lfFloorPerim,
                                            lfCeilingPerim,
                                            sfSkFloor,
                                            sfSkTotalFloor,
                                            sfSkIntWall,
                                            sfSkExtWall,
                                            lfSkExtWallPerim,
                                            sfSkRoof,
                                            skRoofSquares,
                                            lfSkRoofPerim,
                                            lfSkRoofRidge,
                                            fileTransactionID,
                                            file
                                        ])

                        if 'GROUPS' in group:
                            if descLineage != '':
                                subDescLineage = '{} || {}'.format(descLineage, groupDesc)
                            else:
                                subDescLineage = groupDesc

                            lineItemDetailRecursiveGroup(group['GROUPS'], subDescLineage)

            if '@acvTotal' in dict:
                lineItemDetailacvTotal = dict['@acvTotal']
            else:
                lineItemDetailacvTotal = ''

            if '@addons' in dict:
                lineItemDetailAddons = dict['@addons']
            else:
                lineItemDetailAddons = ''

            if '@desc' in dict:
                lineItemDetailDesc = dict['@desc']
            else:
                lineItemDetailDesc = ''

            if '@rcvTotal' in dict:
                lineItemDetailrcvTotal = dict['@rcvTotal']
            else:
                lineItemDetailrcvTotal = ''

            if '@tax' in dict:
                lineItemDetailTax = dict['@tax']
            else:
                lineItemDetailTax = ''

            if '@total' in dict:
                lineItemDetailTotal = dict['@total']
            else:
                lineItemDetailTotal = ''

            descLineage = lineItemDetailDesc

            roughDraft_lineItemDetailTotals_list.append([
                lineItemDetailDesc,
                lineItemDetailacvTotal,
                lineItemDetailAddons,
                lineItemDetailrcvTotal,
                lineItemDetailTax,
                lineItemDetailTotal,
                fileTransactionID,
                file
            ])

            if 'AREA_DIM_VARS' in dict:
                if '@sfWalls' in dict['AREA_DIM_VARS']:
                    sfWalls = dict['AREA_DIM_VARS']['@sfWalls']
                else:
                    sfWalls = ''

                if '@sfCeiling' in dict['AREA_DIM_VARS']:
                    sfCeiling = dict['AREA_DIM_VARS']['@sfCeiling']
                else:
                    sfCeiling = ''

                if '@sfWallsCeiling' in dict['AREA_DIM_VARS']:
                    sfWallsCeiling = dict['AREA_DIM_VARS']['@sfWallsCeiling']
                else:
                    sfWallsCeiling = ''

                if '@sfFloor' in dict['AREA_DIM_VARS']:
                    sfFloor = dict['AREA_DIM_VARS']['@sfFloor']
                else:
                    sfFloor = ''

                if '@syFloor' in dict['AREA_DIM_VARS']:
                    syFloor = dict['AREA_DIM_VARS']['@syFloor']
                else:
                    syFloor = ''

                if '@lfFloorPerim' in dict['AREA_DIM_VARS']:
                    lfFloorPerim = dict['AREA_DIM_VARS']['@lfFloorPerim']
                else:
                    lfFloorPerim = ''

                if '@lfCeilingPerim' in dict['AREA_DIM_VARS']:
                    lfCeilingPerim = dict['AREA_DIM_VARS']['@lfCeilingPerim']
                else:
                    lfCeilingPerim = ''

                if '@sfSkFloor' in dict['AREA_DIM_VARS']:
                    sfSkFloor = dict['AREA_DIM_VARS']['@sfSkFloor']
                else:
                    sfSkFloor = ''

                if '@sfSkTotalFloor' in dict['AREA_DIM_VARS']:
                    sfSkTotalFloor = dict['AREA_DIM_VARS']['@sfSkTotalFloor']
                else:
                    sfSkTotalFloor = ''

                if '@sfSkIntWall' in dict['AREA_DIM_VARS']:
                    sfSkIntWall = dict['AREA_DIM_VARS']['@sfSkIntWall']
                else:
                    sfSkIntWall = ''

                if '@sfSkExtWall' in dict['AREA_DIM_VARS']:
                    sfSkExtWall = dict['AREA_DIM_VARS']['@sfSkExtWall']
                else:
                    sfSkExtWall = ''

                if '@lfSkExtWallPerim' in dict['AREA_DIM_VARS']:
                    lfSkExtWallPerim = dict['AREA_DIM_VARS']['@lfSkExtWallPerim']
                else:
                    lfSkExtWallPerim = ''

                if '@sfSkRoof' in dict['AREA_DIM_VARS']:
                    sfSkRoof = dict['AREA_DIM_VARS']['@sfSkRoof']
                else:
                    sfSkRoof = ''

                if '@skRoofSquares' in dict['AREA_DIM_VARS']:
                    skRoofSquares = dict['AREA_DIM_VARS']['@skRoofSquares']
                else:
                    skRoofSquares = ''

                if '@lfSkRoofPerim' in dict['AREA_DIM_VARS']:
                    lfSkRoofPerim = dict['AREA_DIM_VARS']['@lfSkRoofPerim']
                else:
                    lfSkRoofPerim = ''

                if '@lfSkRoofRidge' in dict['AREA_DIM_VARS']:
                    lfSkRoofRidge = dict['AREA_DIM_VARS']['@lfSkRoofRidge']
                else:
                    lfSkRoofRidge = ''

                roughDraft_lineItemDetailSFTotals_list.append([
                    lineItemDetailDesc,
                    sfWalls,
                    sfCeiling,
                    sfWallsCeiling,
                    sfFloor,
                    syFloor,
                    lfFloorPerim,
                    lfCeilingPerim,
                    sfSkFloor,
                    sfSkTotalFloor,
                    sfSkIntWall,
                    sfSkExtWall,
                    lfSkExtWallPerim,
                    sfSkRoof,
                    skRoofSquares,
                    lfSkRoofPerim,
                    lfSkRoofRidge,
                    fileTransactionID,
                    file
                ])

            if 'ITEMS' in dict:
                if 'ITEM' in dict['ITEMS']:
                    for item in dict['ITEMS']['ITEM']:
                        if '@act' in item:
                            act = item['@act']
                        else:
                            act = ''

                        if '@acv' in item:
                            acv = item['@acv']
                        else:
                            acv = ''

                        if '@acvTotal' in item:
                            acvTotal = item['@acvTotal']
                        else:
                            acvTotal = ''

                        if '@addons' in item:
                            addons = item['@addons']
                        else:
                            addons = ''

                        if '@calc' in item:
                            calc = item['@calc']
                        else:
                            calc = ''

                        if '@cat' in item:
                            cat = item['@cat']
                        else:
                            cat = ''

                        if 'containsBSCDontApply' in item:
                            containsBSCDontApply = item['@containsBSCDontApply']
                        else:
                            containsBSCDontApply = ''

                        if '@coverageName' in item:
                            coverageName = item['@coverageName']
                        else:
                            coverageName = ''

                        if '@desc' in item:
                            desc = item['@desc']
                        else:
                            desc = ''

                        if '@isPartOfInitSettle' in item:
                            isPartOfInitSettle = item['@isPartOfInitSettle']
                        else:
                            isPartOfInitSettle = ''

                        if '@laborBase' in item:
                            laborBase = item['@laborBase']
                        else:
                            laborBase = ''

                        if '@laborBurden' in item:
                            laborBurden = item['@laborBurden']
                        else:
                            laborBurden = ''

                        if '@laborHours' in item:
                            laborHours = item['@laborHours']
                        else:
                            laborHours = ''

                        if '@laborMarkup' in item:
                            laborMarkup = item['@laborMarkup']
                        else:
                            laborMarkup = ''

                        if '@laborTotal' in item:
                            laborTotal = item['@laborTotal']
                        else:
                            laborTotal = ''

                        if '@lineNum' in item:
                            lineNum = item['@lineNum']
                        else:
                            lineNum = ''

                        if '@qty' in item:
                            qty = item['@qty']
                        else:
                            qty = ''

                        if '@rcvTotal' in item:
                            rcvTotal = item['@rcvTotal']
                        else:
                            rcvTotal = ''

                        if '@recoverable' in item:
                            recoverable = item['@recoverable']
                        else:
                            recoverable = ''

                        if '@replace' in item:
                            replace = item['@replace']
                        else:
                            replace = ''

                        if '@sel' in item:
                            sel = item['@sel']
                        else:
                            sel = ''

                        if '@total' in item:
                            total = item['@total']
                        else:
                            total = ''

                        if '@type' in item:
                            type = item['@type']
                        else:
                            type = ''

                        if '@unit' in item:
                            unit = item['@unit']
                        else:
                            unit = ''

                        roughDraft_lineItemDetailItems_list.append([
                            descLineage,
                            act,
                            acv,
                            acvTotal,
                            addons,
                            calc,
                            cat,
                            containsBSCDontApply,
                            coverageName,
                            desc,
                            isPartOfInitSettle,
                            laborBase,
                            laborBurden,
                            laborHours,
                            laborMarkup,
                            laborTotal,
                            lineNum,
                            qty,
                            rcvTotal,
                            recoverable,
                            replace,
                            sel,
                            total,
                            type,
                            unit,
                            fileTransactionID,
                            file
                        ])

            if 'GROUPS' in dict:
                lineItemDetailRecursiveGroup(dict['GROUPS'], descLineage)

        draft_dict = rough_draft_schema.to_dict(estimate_text)

        file = file.key

        fileXMLNS = draft_dict['@xmlns']
        fileMajorVersion = draft_dict['@majorVersion']
        fileMinorVersion = draft_dict['@minorVersion']
        fileTransactionID = draft_dict['@transactionId']

        if 'HEADER' in draft_dict:
            if '@compName' in draft_dict['HEADER']:
                headerCompName = draft_dict['HEADER']['@compName']
            else:
                headerCompName = ''

            if '@dateCreated' in draft_dict['HEADER']:
                headerDateCreated = draft_dict['HEADER']['@dateCreated']
            else:
                headerDateCreated = ''
        else:
            headerCompName = ''
            headerDateCreated = ''

        if 'COVERSHEET' in draft_dict:
            if 'ADDRESSES' in draft_dict['COVERSHEET']:
                if draft_dict['COVERSHEET']['ADDRESSES'] != None:
                    coversheetAddressCount = len(draft_dict['COVERSHEET']['ADDRESSES']['ADDRESS'])
                    if coversheetAddressCount > 0:
                        for address in draft_dict['COVERSHEET']['ADDRESSES']['ADDRESS']:

                            if '@type' in address:
                                coversheetAddressType = address['@type']
                            else:
                                coversheetAddressType = ''

                            if '@street' in address:
                                coversheetAddressStreet = address['@street']
                            else:
                                coversheetAddressStreet = ''

                            if '@city' in address:
                                coversheetAddressCity = address['@city']
                            else:
                                coversheetAddressCity = ''

                            if '@state' in address:
                                coversheetAddressState = address['@state']
                            else:
                                coversheetAddressState = ''

                            if '@zip' in address:
                                coversheetAddressPostal = address['@zip']
                            else:
                                coversheetAddressPostal = ''

                            if '@primary' in address:
                                coversheetAddressPrimary = address['@primary']
                            else:
                                coversheetAddressPrimary = ''

                            roughDraft_coversheet_address_list.append([
                                coversheetAddressType,
                                coversheetAddressStreet,
                                coversheetAddressCity,
                                coversheetAddressState,
                                coversheetAddressPostal,
                                coversheetAddressPrimary,
                                fileTransactionID,
                                file
                            ])

            if 'PHONES' in draft_dict['COVERSHEET']:
                if draft_dict['COVERSHEET']['PHONES'] != None:
                    coversheetPhoneCount = len(draft_dict['COVERSHEET']['PHONES']['PHONE'])
                    if coversheetPhoneCount > 0:
                        for phone in draft_dict['COVERSHEET']['PHONES']['PHONE']:

                            if '@type' in phone:
                                coversheetPhoneType = phone['@type']
                            else:
                                coversheetPhoneType = ''

                            if '@primary' in phone:
                                coversheetPhonePrimary = phone['@primary']
                            else:
                                coversheetPhonePrimary = ''

                            if '@phone' in phone:
                                coversheetPhoneNumber = phone['@phone']
                            else:
                                coversheetPhoneNumber = ''

                            roughDraft_coversheet_phone_list.append([
                                coversheetPhoneType,
                                coversheetPhonePrimary,
                                coversheetPhoneNumber,
                                fileTransactionID,
                                file
                            ])

            if 'CONTACTS' in draft_dict['COVERSHEET']:
                contactCount = len(draft_dict['COVERSHEET']['CONTACTS']['CONTACT'])

                if contactCount > 0:
                    for contact in draft_dict['COVERSHEET']['CONTACTS']['CONTACT']:

                        contactID = fileTransactionID + '||' + str(
                            draft_dict['COVERSHEET']['CONTACTS']['CONTACT'].index(contact))

                        if '@type' in contact:
                            contactType = contact['@type']
                        else:
                            contactType = ''

                        if '@name' in contact:
                            contactName = contact['@name']
                        else:
                            contactName = ''

                        roughDraft_contacts_list.append([
                            contactID,
                            contactType,
                            contactName
                        ])

                        if 'ADDRESSES' in contact:
                            if contact['ADDRESSES'] != None:
                                contactAddressCount = len(contact['ADDRESSES']['ADDRESS'])
                                if contactAddressCount > 0:
                                    for address in contact['ADDRESSES']['ADDRESS']:

                                        if '@type' in address:
                                            contactAddressType = address['@type']
                                        else:
                                            contactAddressType = ''

                                        if '@street' in address:
                                            contactAddressStreet = address['@street']
                                        else:
                                            contactAddressStreet = ''

                                        if '@city' in address:
                                            contactAddressCity = address['@city']
                                        else:
                                            contactAddressCity = ''

                                        if '@state' in address:
                                            contactAddressState = address['@state']
                                        else:
                                            contactAddressState = ''

                                        if '@zip' in address:
                                            contactAddressPostal = address['@zip']
                                        else:
                                            contactAddressPostal = ''

                                        if '@primary' in address:
                                            contactAddressPrimary = address['@primary']
                                        else:
                                            contactAddressPrimary = ''

                                        roughDraft_contacts_address_list.append([
                                            contactID,
                                            contactAddressType,
                                            contactAddressStreet,
                                            contactAddressCity,
                                            contactAddressState,
                                            contactAddressPostal,
                                            contactAddressPrimary,
                                            fileTransactionID,
                                            file
                                        ])

                        if 'PHONES' in contact:
                            if contact['PHONES'] != None:
                                contactPhoneCount = len(contact['PHONES']['PHONE'])
                                if contactPhoneCount > 0:
                                    for phone in contact['PHONES']['PHONE']:

                                        if '@primary' in phone:
                                            contactPhonePrimary = phone['@primary']
                                        else:
                                            contactPhonePrimary = ''

                                        if '@type' in phone:
                                            contactPhoneType = phone['@type']
                                        else:
                                            contactPhoneType = ''

                                        if '@phone' in phone:
                                            contactPhoneNumber = phone['@phone']
                                        else:
                                            contactPhoneNumber = ''

                                        roughDraft_contacts_phone_list.append([
                                            contactID,
                                            contactPhonePrimary,
                                            contactPhoneType,
                                            contactPhoneNumber,
                                            fileTransactionID,
                                            file
                                        ])

            if 'ESTIMATE_INFO' in draft_dict['COVERSHEET']:
                if '@carrierId' in draft_dict['COVERSHEET']['ESTIMATE_INFO']:
                    estimateInfoCarrierID = draft_dict['COVERSHEET']['ESTIMATE_INFO']['@carrierId']
                else:
                    estimateInfoCarrierID = ''

                if '@claimNumber' in draft_dict['COVERSHEET']['ESTIMATE_INFO']:
                    estimateInfoClaimNumber = draft_dict['COVERSHEET']['ESTIMATE_INFO']['@claimNumber']
                else:
                    estimateInfoClaimNumber = ''

                if '@deprMat' in draft_dict['COVERSHEET']['ESTIMATE_INFO']:
                    estimateInfoDeprMat = draft_dict['COVERSHEET']['ESTIMATE_INFO']['@deprMat']
                else:
                    estimateInfoDeprMat = ''

                if '@deprNonMat' in draft_dict['COVERSHEET']['ESTIMATE_INFO']:
                    estimateInfoDeprNonMat = draft_dict['COVERSHEET']['ESTIMATE_INFO']['@deprNonMat']
                else:
                    estimateInfoDeprNonMat = ''

                if '@deprTaxes' in draft_dict['COVERSHEET']['ESTIMATE_INFO']:
                    estimateInfoDeprTaxes = draft_dict['COVERSHEET']['ESTIMATE_INFO']['@deprTaxes']
                else:
                    estimateInfoDeprTaxes = ''

                if '@estimateName' in draft_dict['COVERSHEET']['ESTIMATE_INFO']:
                    estimateInfoEstimateName = draft_dict['COVERSHEET']['ESTIMATE_INFO']['@estimateName']
                else:
                    estimateInfoEstimateName = ''

                if '@estimateType' in draft_dict['COVERSHEET']['ESTIMATE_INFO']:
                    estimateInfoEstimateType = draft_dict['COVERSHEET']['ESTIMATE_INFO']['@estimateType']
                else:
                    estimateInfoEstimateType = ''

                if '@inspNotPerformed' in draft_dict['COVERSHEET']['ESTIMATE_INFO']:
                    estimateInfoInspectionNotPerformed = draft_dict['COVERSHEET']['ESTIMATE_INFO'][
                        '@inspNotPerformed']
                else:
                    estimateInfoInspectionNotPerformed = ''

                if '@insuredName' in draft_dict['COVERSHEET']['ESTIMATE_INFO']:
                    estimateInfoInsuredName = draft_dict['COVERSHEET']['ESTIMATE_INFO']['@insuredName']
                else:
                    estimateInfoInsuredName = ''

                if '@laborEff' in draft_dict['COVERSHEET']['ESTIMATE_INFO']:
                    estimateInfoLaborEff = draft_dict['COVERSHEET']['ESTIMATE_INFO']['@laborEff']
                else:
                    estimateInfoLaborEff = ''

                if '@policyNumber' in draft_dict['COVERSHEET']['ESTIMATE_INFO']:
                    estimateInfoPolicyNumber = draft_dict['COVERSHEET']['ESTIMATE_INFO']['@policyNumber']
                else:
                    estimateInfoPolicyNumber = ''
            else:
                estimateInfoCarrierID = ''
                estimateInfoClaimNumber = ''
                estimateInfoDeprMat = ''
                estimateInfoDeprNonMat = ''
                estimateInfoDeprTaxes = ''
                estimateInfoEstimateName = ''
                estimateInfoEstimateType = ''
                estimateInfoInspectionNotPerformed = ''
                estimateInfoInsuredName = ''
                estimateInfoLaborEff = ''
                estimateInfoPolicyNumber = ''

            if 'OPENING_STATEMENT' in draft_dict['COVERSHEET']:
                estimateInfoOpeningStatement = ' '.join(draft_dict['COVERSHEET']['OPENING_STATEMENT'].split())
            else:
                estimateInfoOpeningStatement = ''

            if 'DATES' in draft_dict['COVERSHEET']:
                if '@loss' in draft_dict['COVERSHEET']['DATES']:
                    dateLoss = draft_dict['COVERSHEET']['DATES']['@loss']
                else:
                    dateLoss = ''

                if '@completed' in draft_dict['COVERSHEET']['DATES']:
                    dateCompleted = draft_dict['COVERSHEET']['DATES']['@completed']
                else:
                    dateCompleted = ''

                if '@received' in draft_dict['COVERSHEET']['DATES']:
                    dateReceived = draft_dict['COVERSHEET']['DATES']['@received']
                else:
                    dateReceived = ''

                if '@entered' in draft_dict['COVERSHEET']['DATES']:
                    dateEntered = draft_dict['COVERSHEET']['DATES']['@entered']
                else:
                    dateEntered = ''

                if '@contacted' in draft_dict['COVERSHEET']['DATES']:
                    dateContacted = draft_dict['COVERSHEET']['DATES']['@contacted']
                else:
                    dateContacted = ''
            else:
                dateLoss = ''
                dateCompleted = ''
                dateReceived = ''
                dateEntered = ''
                dateContacted = ''

        if 'COVERAGES' in draft_dict:
            coverageCount = len(draft_dict['COVERAGES']['COVERAGE'])
            if coverageCount > 0:
                for coverage in draft_dict['COVERAGES']['COVERAGE']:
                    if '@deductible' in coverage:
                        coverageDeductible = coverage['@deductible']
                    else:
                        coverageDeductible = ''

                    if '@coverageName' in coverage:
                        coverageName = coverage['@coverageName']
                    else:
                        coverageName = ''

                    if '@coverageType' in coverage:
                        coverageType = coverage['@coverageType']
                    else:
                        coverageType = ''

                    if '@id' in coverage:
                        coverageID = coverage['@id']
                    else:
                        coverageID = ''

                    if 'LOSS_DATA' in coverage:
                        if '@rcl' in coverage['LOSS_DATA']:
                            coverageLossDataRCL = coverage['LOSS_DATA']['@rcl']
                        else:
                            coverageLossDataRCL = ''

                        if '@bd' in coverage['LOSS_DATA']:
                            coverageLossDataBD = coverage['LOSS_DATA']['@bd']
                        else:
                            coverageLossDataBD = ''

                        if '@acvLoss' in coverage['LOSS_DATA']:
                            coverageLossDataAcvLoss = coverage['LOSS_DATA']['@acvLoss']
                        else:
                            coverageLossDataAcvLoss = ''

                        if '@nonRecDeprec' in coverage['LOSS_DATA']:
                            coverageLossDataNonRecDeprec = coverage['LOSS_DATA']['@nonRecDeprec']
                        else:
                            coverageLossDataNonRecDeprec = ''

                        if '@deductApplied' in coverage['LOSS_DATA']:
                            coverageLossDataDedutApplied = coverage['LOSS_DATA']['@deductApplied']
                        else:
                            coverageLossDataDedutApplied = ''

                        if '@insCarried' in coverage['LOSS_DATA']:
                            coverageLossDataInsCarried = coverage['LOSS_DATA']['@insCarried']
                        else:
                            coverageLossDataInsCarried = ''

                        if '@rcvInsCarried' in coverage['LOSS_DATA']:
                            coverageLossDataRCVInsCarried = coverage['LOSS_DATA']['@rcvInsCarried']
                        else:
                            coverageLossDataRCVInsCarried = ''

                        if '@acvInsCarried' in coverage['LOSS_DATA']:
                            coverageLossDataACVInsCarried = coverage['LOSS_DATA']['@acvInsCarried']
                        else:
                            coverageLossDataACVInsCarried = ''

                        if '@potentialSuppClaim' in coverage['LOSS_DATA']:
                            coverageLossDataPotentialSuppClaim = coverage['LOSS_DATA']['@potentialSuppClaim']
                        else:
                            coverageLossDataPotentialSuppClaim = ''

                        if '@total' in coverage['LOSS_DATA']:
                            coverageLossDataTotal = coverage['LOSS_DATA']['@total']
                        else:
                            coverageLossDataTotal = ''

                        if '@valACV' in coverage['LOSS_DATA']:
                            coverageLossDataValACV = coverage['LOSS_DATA']['@valACV']
                        else:
                            coverageLossDataValACV = ''

                        if '@valRCV' in coverage['LOSS_DATA']:
                            coverageLossDataValRCV = coverage['LOSS_DATA']['@valRCV']
                        else:
                            coverageLossDataValRCV = ''

                        if '@salvage' in coverage['LOSS_DATA']:
                            coverageLossDataSalvage = coverage['LOSS_DATA']['@salvage']
                        else:
                            coverageLossDataSalvage = ''

                        if '@directReplacement' in coverage['LOSS_DATA']:
                            coverageLossDataDirectReplacement = coverage['LOSS_DATA']['@directReplacement']
                        else:
                            coverageLossDataDirectReplacement = ''

                        if '@priorPmts' in coverage['LOSS_DATA']:
                            coverageLossDataPriorPmts = coverage['LOSS_DATA']['@priorPmts']
                        else:
                            coverageLossDataPriorPmts = ''

                        if '@coins' in coverage['LOSS_DATA']:
                            coverageLossDataCoins = coverage['LOSS_DATA']['@coins']
                        else:
                            coverageLossDataCoins = ''

                        if '@coinsFormula' in coverage['LOSS_DATA']:
                            coverageLossDataCoinsFormula = coverage['LOSS_DATA']['@coinsFormula']
                        else:
                            coverageLossDataCoinsFormula = ''

                        if '@overLimits' in coverage['LOSS_DATA']:
                            coverageLossDataOverLimits = coverage['LOSS_DATA']['@overLimits']
                        else:
                            coverageLossDataOverLimits = ''

                        if '@acvClaim' in coverage['LOSS_DATA']:
                            coverageLossDataAcvClaim = coverage['LOSS_DATA']['@acvClaim']
                        else:
                            coverageLossDataAcvClaim = ''

                        if '@adjLossAmt' in coverage['LOSS_DATA']:
                            coverageLossDataAdjLossAmt = coverage['LOSS_DATA']['@adjLossAmt']
                        else:
                            coverageLossDataAdjLossAmt = ''

                        if '@rcvClaim' in coverage['LOSS_DATA']:
                            coverageLossDataRcvClaim = coverage['LOSS_DATA']['@rcvClaim']
                        else:
                            coverageLossDataRcvClaim = ''
                    else:
                        coverageLossDataBD = ''
                        coverageLossDataNonRecDeprec = ''
                        coverageLossDataDedutApplied = ''
                        coverageLossDataInsCarried = ''
                        coverageLossDataRCVInsCarried = ''
                        coverageLossDataACVInsCarried = ''
                        coverageLossDataPotentialSuppClaim = ''
                        coverageLossDataTotal = ''
                        coverageLossDataValACV = ''
                        coverageLossDataValRCV = ''
                        coverageLossDataSalvage = ''
                        coverageLossDataDirectReplacement = ''
                        coverageLossDataPriorPmts = ''
                        coverageLossDataCoins = ''
                        coverageLossDataCoinsFormula = ''
                        coverageLossDataOverLimits = ''
                        coverageLossDataAcvClaim = ''
                        coverageLossDataAcvLoss = ''
                        coverageLossDataAdjLossAmt = ''
                        coverageLossDataRCL = ''
                        coverageLossDataRcvClaim = ''

                    if 'SUMMARY' in coverage:
                        if 'TOTALS' in coverage['SUMMARY']:

                            if '@actualRecDepr' in coverage['SUMMARY']['TOTALS']:
                                coverageSummaryActualRecDepr = coverage['SUMMARY']['TOTALS']['@actualRecDepr']
                            else:
                                coverageSummaryActualRecDepr = ''

                            if '@acv' in coverage['SUMMARY']['TOTALS']:
                                coverageSummaryACV = coverage['SUMMARY']['TOTALS']['@acv']
                            else:
                                coverageSummaryACV = ''

                            if '@deductible' in coverage['SUMMARY']['TOTALS']:
                                coverageSummaryDeductible = coverage['SUMMARY']['TOTALS']['@deductible']
                            else:
                                coverageSummaryDeductible = ''

                            if '@depr' in coverage['SUMMARY']['TOTALS']:
                                coverageSummaryDepr = coverage['SUMMARY']['TOTALS']['@depr']
                            else:
                                coverageSummaryDepr = ''

                            if '@fullDeduct' in coverage['SUMMARY']['TOTALS']:
                                coverageSummaryFullDeduct = coverage['SUMMARY']['TOTALS']['@fullDeduct']
                            else:
                                coverageSummaryFullDeduct = ''

                            if '@lineItemTotal' in coverage['SUMMARY']['TOTALS']:
                                coverageSummaryLineItemTotal = coverage['SUMMARY']['TOTALS']['@lineItemTotal']
                            else:
                                coverageSummaryLineItemTotal = ''

                            if '@netClaim' in coverage['SUMMARY']['TOTALS']:
                                coverageSummaryNetClaim = coverage['SUMMARY']['TOTALS']['@netClaim']
                            else:
                                coverageSummaryNetClaim = ''

                            if '@netClaimIfRec' in coverage['SUMMARY']['TOTALS']:
                                coverageSummaryNetClaimIfRec = coverage['SUMMARY']['TOTALS']['@netClaimIfRec']
                            else:
                                coverageSummaryNetClaimIfRec = ''

                            if '@priorPmts' in coverage['SUMMARY']['TOTALS']:
                                coverageSummaryPriorPmts = coverage['SUMMARY']['TOTALS']['@priorPmts']
                            else:
                                coverageSummaryPriorPmts = ''

                            if '@priorPmtsAdj' in coverage['SUMMARY']['TOTALS']:
                                coverageSummaryPriorPmtsAdj = coverage['SUMMARY']['TOTALS']['@priorPmtsAdj']
                            else:
                                coverageSummaryPriorPmtsAdj = ''

                            if '@rcv' in coverage['SUMMARY']['TOTALS']:
                                coverageSummaryRCV = coverage['SUMMARY']['TOTALS']['@rcv']
                            else:
                                coverageSummaryRCV = ''

                            if '@recDepr' in coverage['SUMMARY']['TOTALS']:
                                coverageSummaryRecDepr = coverage['SUMMARY']['TOTALS']['@recDepr']
                            else:
                                coverageSummaryRecDepr = ''

                            if '@subtotal' in coverage['SUMMARY']['TOTALS']:
                                coverageSummarySubtotal = coverage['SUMMARY']['TOTALS']['@subtotal']
                            else:
                                coverageSummarySubtotal = ''

                            if 'PAYMENT_TRACKER' in coverage['SUMMARY']['TOTALS']:
                                if '@ptEstRemaining' in coverage['SUMMARY']['TOTALS']['PAYMENT_TRACKER']:
                                    coverageSummaryPaymentTrackerPTestRemaining = \
                                        coverage['SUMMARY']['TOTALS']['PAYMENT_TRACKER']['@ptEstRemaining']
                                else:
                                    coverageSummaryPaymentTrackerPTestRemaining = ''

                                if '@ptEstValue' in coverage['SUMMARY']['TOTALS']['PAYMENT_TRACKER']:
                                    coverageSummaryPaymentTrackerPTestValue = \
                                        coverage['SUMMARY']['TOTALS']['PAYMENT_TRACKER']['@ptEstValue']
                                else:
                                    coverageSummaryPaymentTrackerPTestValue = ''

                                if '@ptItemsWithRemaining' in coverage['SUMMARY']['TOTALS']['PAYMENT_TRACKER']:
                                    coverageSummaryPaymentTrackerPTItemsWithRemaining = \
                                        coverage['SUMMARY']['TOTALS']['PAYMENT_TRACKER'][
                                            '@ptItemsWithRemaining']
                                else:
                                    coverageSummaryPaymentTrackerPTItemsWithRemaining = ''

                                if '@ptNumItemActs' in coverage['SUMMARY']['TOTALS']['PAYMENT_TRACKER']:
                                    coverageSummaryPaymentTrackerPTnumItemActs = \
                                        coverage['SUMMARY']['TOTALS']['PAYMENT_TRACKER']['@ptNumItemActs']
                                else:
                                    coverageSummaryPaymentTrackerPTnumItemActs = ''

                                if '@ptRecoverableDep' in coverage['SUMMARY']['TOTALS']['PAYMENT_TRACKER']:
                                    coverageSummaryPaymentTrackerPTRecoverableDep = \
                                        coverage['SUMMARY']['TOTALS']['PAYMENT_TRACKER']['@ptRecoverableDep']
                                else:
                                    coverageSummaryPaymentTrackerPTRecoverableDep = ''

                                if '@ptTotalPaid' in coverage['SUMMARY']['TOTALS']['PAYMENT_TRACKER']:
                                    coverageSummaryPaymentTrackerPTTotalPaid = \
                                        coverage['SUMMARY']['TOTALS']['PAYMENT_TRACKER']['@ptTotalPaid']
                                else:
                                    coverageSummaryPaymentTrackerPTTotalPaid = ''

                            else:
                                coverageSummaryPaymentTrackerPTestRemaining = ''
                                coverageSummaryPaymentTrackerPTestValue = ''
                                coverageSummaryPaymentTrackerPTItemsWithRemaining = ''
                                coverageSummaryPaymentTrackerPTnumItemActs = ''
                                coverageSummaryPaymentTrackerPTRecoverableDep = ''
                                coverageSummaryPaymentTrackerPTTotalPaid = ''

                            if 'OVERHEAD' in coverage['SUMMARY']['TOTALS']:
                                coverageSummaryOverhead = coverage['SUMMARY']['TOTALS']['OVERHEAD']['@amount']
                            else:
                                coverageSummaryOverhead = ''

                            if 'PROFIT' in coverage['SUMMARY']['TOTALS']:
                                coverageSummaryProfit = coverage['SUMMARY']['TOTALS']['PROFIT']['@amount']
                            else:
                                coverageSummaryProfit = ''

                            if 'SIGNATURE' in coverage['SUMMARY']['TOTALS']:
                                coverageSummarySignature = coverage['SUMMARY']['TOTALS']['SIGNATURE'][
                                    '@estimator']
                            else:
                                coverageSummarySignature = ''

                            if 'SALES_TAXES' in coverage['SUMMARY']['TOTALS']:
                                for tax in coverage['SUMMARY']['TOTALS']['SALES_TAXES']['SALES_TAX']:
                                    coverageSummarySalesTaxAmount = tax['@amount']
                                    coverageSummarySalesTaxDesc = tax['@desc']
                                    coverageSummarySalesTaxOP = tax['@taxOP']

                                    roughDraft_summaryTaxes_list.append([
                                        coverageID,
                                        coverageSummarySalesTaxAmount,
                                        coverageSummarySalesTaxDesc,
                                        coverageSummarySalesTaxOP,
                                        fileTransactionID,
                                        file
                                    ])
                        else:
                            coverageSummaryActualRecDepr = ''
                            coverageSummaryACV = ''
                            coverageSummaryDeductible = ''
                            coverageSummaryDepr = ''
                            coverageSummaryFullDeduct = ''
                            coverageSummaryLineItemTotal = ''
                            coverageSummaryNetClaim = ''
                            coverageSummaryNetClaimIfRec = ''
                            coverageSummaryPriorPmts = ''
                            coverageSummaryPriorPmtsAdj = ''
                            coverageSummaryRCV = ''
                            coverageSummaryRecDepr = ''
                            coverageSummarySubtotal = ''
                            coverageSummaryPaymentTrackerPTestRemaining = ''
                            coverageSummaryPaymentTrackerPTestValue = ''
                            coverageSummaryPaymentTrackerPTItemsWithRemaining = ''
                            coverageSummaryPaymentTrackerPTnumItemActs = ''
                            coverageSummaryPaymentTrackerPTRecoverableDep = ''
                            coverageSummaryPaymentTrackerPTTotalPaid = ''
                            coverageSummaryOverhead = ''
                            coverageSummaryProfit = ''
                            coverageSummarySignature = ''
                    else:
                        coverageSummaryActualRecDepr = ''
                        coverageSummaryACV = ''
                        coverageSummaryDeductible = ''
                        coverageSummaryDepr = ''
                        coverageSummaryFullDeduct = ''
                        coverageSummaryLineItemTotal = ''
                        coverageSummaryNetClaim = ''
                        coverageSummaryNetClaimIfRec = ''
                        coverageSummaryPriorPmts = ''
                        coverageSummaryPriorPmtsAdj = ''
                        coverageSummaryRCV = ''
                        coverageSummaryRecDepr = ''
                        coverageSummarySubtotal = ''
                        coverageSummaryPaymentTrackerPTestRemaining = ''
                        coverageSummaryPaymentTrackerPTestValue = ''
                        coverageSummaryPaymentTrackerPTItemsWithRemaining = ''
                        coverageSummaryPaymentTrackerPTnumItemActs = ''
                        coverageSummaryPaymentTrackerPTRecoverableDep = ''
                        coverageSummaryPaymentTrackerPTTotalPaid = ''
                        coverageSummaryOverhead = ''
                        coverageSummaryProfit = ''
                        coverageSummarySignature = ''

                    roughDraft_coverage_list.append([
                        coverageID,
                        coverageName,
                        coverageType,
                        coverageDeductible,
                        coverageLossDataBD,
                        coverageLossDataNonRecDeprec,
                        coverageLossDataDedutApplied,
                        coverageLossDataInsCarried,
                        coverageLossDataRCVInsCarried,
                        coverageLossDataACVInsCarried,
                        coverageLossDataPotentialSuppClaim,
                        coverageLossDataTotal,
                        coverageLossDataValACV,
                        coverageLossDataValRCV,
                        coverageLossDataSalvage,
                        coverageLossDataDirectReplacement,
                        coverageLossDataPriorPmts,
                        coverageLossDataCoins,
                        coverageLossDataCoinsFormula,
                        coverageLossDataOverLimits,
                        coverageLossDataAcvClaim,
                        coverageLossDataAcvLoss,
                        coverageLossDataAdjLossAmt,
                        coverageLossDataRCL,
                        coverageLossDataRcvClaim,
                        coverageSummaryActualRecDepr,
                        coverageSummaryACV,
                        coverageSummaryDeductible,
                        coverageSummaryDepr,
                        coverageSummaryFullDeduct,
                        coverageSummaryLineItemTotal,
                        coverageSummaryNetClaim,
                        coverageSummaryNetClaimIfRec,
                        coverageSummaryPriorPmts,
                        coverageSummaryPriorPmtsAdj,
                        coverageSummaryRCV,
                        coverageSummaryRecDepr,
                        coverageSummarySubtotal,
                        coverageSummaryPaymentTrackerPTestRemaining,
                        coverageSummaryPaymentTrackerPTestValue,
                        coverageSummaryPaymentTrackerPTItemsWithRemaining,
                        coverageSummaryPaymentTrackerPTnumItemActs,
                        coverageSummaryPaymentTrackerPTRecoverableDep,
                        coverageSummaryPaymentTrackerPTTotalPaid,
                        coverageSummaryOverhead,
                        coverageSummaryProfit,
                        coverageSummarySignature,
                        fileTransactionID,
                        file
                    ])

        if 'LINE_ITEM_DETAIL' in draft_dict:
            if '@contractorTotal' in draft_dict['LINE_ITEM_DETAIL']:
                lineItemDetailContractorTotal = draft_dict['LINE_ITEM_DETAIL']['@contractorTotal']
            else:
                lineItemDetailContractorTotal = ''

            if '@homeownerTotal' in draft_dict['LINE_ITEM_DETAIL']:
                lineItemDetailHomeownerTotal = draft_dict['LINE_ITEM_DETAIL']['@homeownerTotal']
            else:
                lineItemDetailHomeownerTotal = ''

            if '@total' in draft_dict['LINE_ITEM_DETAIL']:
                lineItemDetailTotal = draft_dict['LINE_ITEM_DETAIL']['@total']
            else:
                lineItemDetailTotal = ''

            if 'GROUP' in draft_dict['LINE_ITEM_DETAIL']:
                lineItemDetail(draft_dict['LINE_ITEM_DETAIL']['GROUP'])
        else:
            lineItemDetailContractorTotal = ''
            lineItemDetailHomeownerTotal = ''
            lineItemDetailTotal = ''

        if 'RECAP_BY_ROOM' in draft_dict:
            recapByRoomTotal = recapByRoom(draft_dict['RECAP_BY_ROOM'])
        else:
            recapByRoomTotal = ''

        if 'RECAP_BY_CATEGORY' in draft_dict:
            if '@subtotalACV' in draft_dict['RECAP_BY_CATEGORY']:
                recapCategorySubtotalACV = draft_dict['RECAP_BY_CATEGORY']['@subtotalACV']
            else:
                recapCategorySubtotalACV = ''

            if '@subtotalDeprec' in draft_dict['RECAP_BY_CATEGORY']:
                recapCategorySubtotalDeprec = draft_dict['RECAP_BY_CATEGORY']['@subtotalDeprec']
            else:
                recapCategorySubtotalDeprec = ''

            if '@subtotalRCV' in draft_dict['RECAP_BY_CATEGORY']:
                recapCategorySubtotalRCV = draft_dict['RECAP_BY_CATEGORY']['@subtotalRCV']
            else:
                recapCategorySubtotalRCV = ''

            if 'NON_OP_ITEMS' in draft_dict['RECAP_BY_CATEGORY']:
                if '@subtotalACV' in draft_dict['RECAP_BY_CATEGORY']['NON_OP_ITEMS']:
                    recapCategoryNonOPSubtotalACV = draft_dict['RECAP_BY_CATEGORY']['NON_OP_ITEMS'][
                        '@subtotalACV']
                else:
                    recapCategoryNonOPSubtotalACV = ''

                if '@subtotalPercentage' in draft_dict['RECAP_BY_CATEGORY']['NON_OP_ITEMS']:
                    recapCategoryNonOPSubtotalPercentage = draft_dict['RECAP_BY_CATEGORY']['NON_OP_ITEMS'][
                        '@subtotalPercentage']
                else:
                    recapCategoryNonOPSubtotalPercentage = ''

                if '@subtotalDeprec' in draft_dict['RECAP_BY_CATEGORY']['NON_OP_ITEMS']:
                    recapCategoryNonOPSubtotalDeprec = draft_dict['RECAP_BY_CATEGORY']['NON_OP_ITEMS'][
                        '@subtotalDeprec']
                else:
                    recapCategoryNonOPSubtotalDeprec = ''

                if '@subtotalRCV' in draft_dict['RECAP_BY_CATEGORY']['NON_OP_ITEMS']:
                    recapCategoryNonOPSubtotalRCV = draft_dict['RECAP_BY_CATEGORY']['NON_OP_ITEMS'][
                        '@subtotalRCV']
                else:
                    recapCategoryNonOPSubtotalRCV = ''

                if 'CATEGORY' in draft_dict['RECAP_BY_CATEGORY']['NON_OP_ITEMS']:
                    for category in draft_dict['RECAP_BY_CATEGORY']['NON_OP_ITEMS']['CATEGORY']:

                        categoryID = fileTransactionID + '||NonOP||' + str(
                            draft_dict['RECAP_BY_CATEGORY']['NON_OP_ITEMS']['CATEGORY'].index(category))

                        if '@acv' in category:
                            recapCategoryNonOPCategoryACV = category['@acv']
                        else:
                            recapCategoryNonOPCategoryACV = ''

                        if '@desc' in category:
                            recapCategoryNonOPCategoryDesc = category['@desc']
                        else:
                            recapCategoryNonOPCategoryDesc = ''

                        if '@rcv' in category:
                            recapCategoryNonOPCategoryRCV = category['@rcv']
                        else:
                            recapCategoryNonOPCategoryRCV = ''

                        roughDraft_recapByCategory_list.append([
                            categoryID,
                            recapCategoryNonOPCategoryACV,
                            recapCategoryNonOPCategoryDesc,
                            recapCategoryNonOPCategoryRCV,
                            fileTransactionID,
                            file
                        ])

                        if 'COV_INFO' in category:
                            if 'COV' in category['COV_INFO']:
                                for cov in category['COV_INFO']['COV']:
                                    if '@amount' in cov:
                                        nonOPCategoryCovAmt = cov['@amount']
                                    else:
                                        nonOPCategoryCovAmt = ''

                                    if '@name' in cov:
                                        nonOPCategoryCovName = cov['@name']
                                    else:
                                        nonOPCategoryCovName = ''

                                    if '@rate' in cov:
                                        nonOPCategoryCovRate = cov['@rate']
                                    else:
                                        nonOPCategoryCovRate = ''

                                    categoryType = 'nonOP'
                                    roughDraft_recapByCategoryCov_list.append([
                                        categoryID,
                                        categoryType,
                                        nonOPCategoryCovName,
                                        nonOPCategoryCovRate,
                                        nonOPCategoryCovAmt,
                                        fileTransactionID,
                                        file
                                    ])
            else:
                recapCategoryNonOPSubtotalACV = ''
                recapCategoryNonOPSubtotalPercentage = ''
                recapCategoryNonOPSubtotalDeprec = ''
                recapCategoryNonOPSubtotalRCV = ''

            if 'OP_ITEMS' in draft_dict['RECAP_BY_CATEGORY']:

                if '@subtotalACV' in draft_dict['RECAP_BY_CATEGORY']['OP_ITEMS']:
                    recapCategoryOPSubtotalACV = draft_dict['RECAP_BY_CATEGORY']['OP_ITEMS']['@subtotalACV']
                else:
                    recapCategoryOPSubtotalACV = ''

                if '@subtotalDeprec' in draft_dict['RECAP_BY_CATEGORY']['OP_ITEMS']:
                    recapCategoryOPSubtotalDeprec = draft_dict['RECAP_BY_CATEGORY']['OP_ITEMS'][
                        '@subtotalDeprec']
                else:
                    recapCategoryOPSubtotalDeprec = ''

                if '@subtotalPercentage' in draft_dict['RECAP_BY_CATEGORY']['OP_ITEMS']:
                    recapCategoryOPSubtotalPercentage = draft_dict['RECAP_BY_CATEGORY']['OP_ITEMS'][
                        '@subtotalPercentage']
                else:
                    recapCategoryOPSubtotalPercentage = ''

                if '@subtotalRCV' in draft_dict['RECAP_BY_CATEGORY']['OP_ITEMS']:
                    recapCategoryOPSubtotalRCV = draft_dict['RECAP_BY_CATEGORY']['OP_ITEMS']['@subtotalRCV']
                else:
                    recapCategoryOPSubtotalRCV = ''

                if 'CATEGORY' in draft_dict['RECAP_BY_CATEGORY']['OP_ITEMS']:
                    for category in draft_dict['RECAP_BY_CATEGORY']['OP_ITEMS']['CATEGORY']:

                        categoryID = fileTransactionID + '||OP||' + str(
                            draft_dict['RECAP_BY_CATEGORY']['OP_ITEMS']['CATEGORY'].index(category))

                        if '@desc' in category:
                            recapCategoryOPCategoryDesc = category['@desc']
                        else:
                            recapCategoryOPCategoryDesc = ''

                        if '@acv' in category:
                            recapCategoryOPCategoryACV = category['@acv']
                        else:
                            recapCategoryOPCategoryACV = ''

                        if '@rcv' in category:
                            recapCategoryOPCategoryRCV = category['@rcv']
                        else:
                            recapCategoryOPCategoryRCV = ''

                        roughDraft_recapByCategory_list.append([
                            categoryID,
                            recapCategoryOPCategoryDesc,
                            recapCategoryOPCategoryACV,
                            recapCategoryOPCategoryRCV,

                            fileTransactionID,
                            file
                        ])

                        if 'COV_INFO' in category:
                            if 'COV' in category['COV_INFO']:
                                for cov in category['COV_INFO']['COV']:
                                    if '@amount' in cov:
                                        OPCategoryCovAmt = cov['@amount']
                                    else:
                                        OPCategoryCovAmt = ''

                                    if '@name' in cov:
                                        OPCategoryCovName = cov['@name']
                                    else:
                                        OPCategoryCovName = ''

                                    if '@rate' in cov:
                                        OPCategoryCovRate = cov['@rate']
                                    else:
                                        OPCategoryCovRate = ''

                                    categoryType = 'OP'
                                    roughDraft_recapByCategoryCov_list.append([
                                        categoryID,
                                        categoryType,

                                        OPCategoryCovAmt,
                                        OPCategoryCovName,
                                        OPCategoryCovRate,

                                        fileTransactionID,
                                        file
                                    ])
            else:
                recapCategoryOPSubtotalACV = ''
                recapCategoryOPSubtotalDeprec = ''
                recapCategoryOPSubtotalPercentage = ''
                recapCategoryOPSubtotalRCV = ''

            if 'OVERHEAD' in draft_dict['RECAP_BY_CATEGORY']:
                if '@percentage' in draft_dict['RECAP_BY_CATEGORY']['OVERHEAD']:
                    recapCategoryOverheadPercentage = draft_dict['RECAP_BY_CATEGORY']['OVERHEAD']['@percentage']
                else:
                    recapCategoryOverheadPercentage = ''

                if '@rcv' in draft_dict['RECAP_BY_CATEGORY']['OVERHEAD']:
                    recapCategoryOverheadRCV = draft_dict['RECAP_BY_CATEGORY']['OVERHEAD']['@rcv']
                else:
                    recapCategoryOverheadRCV = ''
            else:
                recapCategoryOverheadPercentage = ''
                recapCategoryOverheadRCV = ''

            if 'PROFIT' in draft_dict['RECAP_BY_CATEGORY']:
                if '@percentage' in draft_dict['RECAP_BY_CATEGORY']['PROFIT']:
                    recapCategoryProfitPercentage = draft_dict['RECAP_BY_CATEGORY']['PROFIT']['@percentage']
                else:
                    recapCategoryProfitPercentage = ''

                if '@rcv' in draft_dict['RECAP_BY_CATEGORY']['PROFIT']:
                    recapCategoryProfitRCV = draft_dict['RECAP_BY_CATEGORY']['PROFIT']['@rcv']
                else:
                    recapCategoryProfitRCV = ''
            else:
                recapCategoryProfitPercentage = ''
                recapCategoryProfitRCV = ''

            if 'SALES_TAXES' in draft_dict['RECAP_BY_CATEGORY']:
                if 'SALES_TAX' in draft_dict['RECAP_BY_CATEGORY']['SALES_TAXES']:
                    for tax in draft_dict['RECAP_BY_CATEGORY']['SALES_TAXES']['SALES_TAX']:

                        if '@acv' in tax:
                            recapCategoryTaxACV = tax['@acv']
                        else:
                            recapCategoryTaxACV = ''

                        if '@deprec' in tax:
                            recapCategoryTaxDeprec = tax['@deprec']
                        else:
                            recapCategoryTaxDeprec = ''

                        if '@desc' in tax:
                            recapCategoryTaxDesc = tax['@desc']
                        else:
                            recapCategoryTaxDesc = ''

                        if '@percentage' in tax:
                            recapCategoryTaxPercentage = tax['@percentage']
                        else:
                            recapCategoryTaxPercentage = ''

                        if '@rcv' in tax:
                            recapCategoryTaxRCV = tax['@rcv']
                        else:
                            recapCategoryTaxRCV = ''

                        if 'COV_INFO' in tax:
                            if 'COV' in tax['COV_INFO']:
                                for cov in tax['COV_INFO']['COV']:

                                    if '@amount' in cov:
                                        covAmt = cov['@amount']
                                    else:
                                        covAmt = ''

                                    if '@name' in cov:
                                        covName = cov['@name']
                                    else:
                                        covName = ''

                                    if '@rate' in cov:
                                        covRate = cov['@rate']
                                    else:
                                        covRate = ''

                                    roughDraft_recapByCategorySalesTaxes_list.append([
                                        recapCategoryTaxACV,
                                        recapCategoryTaxDeprec,
                                        recapCategoryTaxDesc,
                                        recapCategoryTaxPercentage,
                                        recapCategoryTaxRCV,
                                        covAmt,
                                        covName,
                                        covRate,
                                        fileTransactionID,
                                        file
                                    ])
                else:
                    recapCategoryTaxACV = ''
                    recapCategoryTaxDeprec = ''
                    recapCategoryTaxDesc = ''
                    recapCategoryTaxPercentage = ''
                    recapCategoryTaxRCV = ''
            else:
                recapCategoryTaxACV = ''
                recapCategoryTaxDeprec = ''
                recapCategoryTaxDesc = ''
                recapCategoryTaxPercentage = ''
                recapCategoryTaxRCV = ''
        else:
            recapCategorySubtotalACV = ''
            recapCategorySubtotalDeprec = ''
            recapCategorySubtotalRCV = ''

            recapCategoryNonOPSubtotalACV = ''
            recapCategoryNonOPSubtotalPercentage = ''
            recapCategoryNonOPSubtotalDeprec = ''
            recapCategoryNonOPSubtotalRCV = ''

            recapCategoryOPSubtotalACV = ''
            recapCategoryOPSubtotalDeprec = ''
            recapCategoryOPSubtotalPercentage = ''
            recapCategoryOPSubtotalRCV = ''

            recapCategoryOverheadPercentage = ''
            recapCategoryOverheadRCV = ''

            recapCategoryProfitPercentage = ''
            recapCategoryProfitRCV = ''

            recapCategoryTaxACV = ''
            recapCategoryTaxDeprec = ''
            recapCategoryTaxDesc = ''
            recapCategoryTaxPercentage = ''
            recapCategoryTaxRCV = ''

        if 'RECAP_TAX_OP' in draft_dict:
            if 'LINE_ITEMS' in draft_dict['RECAP_TAX_OP']:

                if '@overhead' in draft_dict['RECAP_TAX_OP']['LINE_ITEMS']:
                    recapTaxLineItemsOverhead = draft_dict['RECAP_TAX_OP']['LINE_ITEMS']['@overhead']
                else:
                    recapTaxLineItemsOverhead = ''

                if '@profit' in draft_dict['RECAP_TAX_OP']['LINE_ITEMS']:
                    recapTaxLineItemsProfit = draft_dict['RECAP_TAX_OP']['LINE_ITEMS']['@profit']
                else:
                    recapTaxLineItemsProfit = ''

                if 'TAXES' in draft_dict['RECAP_TAX_OP']['LINE_ITEMS']:
                    if 'TAX_AMOUNT' in draft_dict['RECAP_TAX_OP']['LINE_ITEMS']['TAXES']:
                        for tax_amount in draft_dict['RECAP_TAX_OP']['LINE_ITEMS']['TAXES']['TAX_AMOUNT']:
                            recapTaxLineItemsAmt = tax_amount['@amount']
                            recapTaxLineItemsTaxNum = tax_amount['@taxNum']

                            roughDraft_recapTaxLineItemsTaxNum_list.append([
                                recapTaxLineItemsAmt,
                                recapTaxLineItemsTaxNum,
                                fileTransactionID,
                                file
                            ])
            else:
                recapTaxLineItemsOverhead = ''
                recapTaxLineItemsProfit = ''

            if 'OVERHEAD' in draft_dict['RECAP_TAX_OP']:
                if '@amount' in draft_dict['RECAP_TAX_OP']['OVERHEAD']:
                    recapTaxOverheadAmt = draft_dict['RECAP_TAX_OP']['OVERHEAD']['@amount']
                else:
                    recapTaxOverheadAmt = ''

                if '@rate' in draft_dict['RECAP_TAX_OP']['OVERHEAD']:
                    recapTaxOverheadRate = draft_dict['RECAP_TAX_OP']['OVERHEAD']['@rate']
                else:
                    recapTaxOverheadRate = ''
            else:
                recapTaxOverheadAmt = ''
                recapTaxOverheadRate = ''

            if 'PROFIT' in draft_dict['RECAP_TAX_OP']:
                if '@amount' in draft_dict['RECAP_TAX_OP']['PROFIT']:
                    recapTaxProfitAmt = draft_dict['RECAP_TAX_OP']['PROFIT']['@amount']
                else:
                    recapTaxProfitAmt = ''

                if '@rate' in draft_dict['RECAP_TAX_OP']['PROFIT']:
                    recapTaxProfitRate = draft_dict['RECAP_TAX_OP']['PROFIT']['@rate']
                else:
                    recapTaxProfitRate = ''
            else:
                recapTaxProfitAmt = ''
                recapTaxProfitRate = ''

            if 'TAXES' in draft_dict['RECAP_TAX_OP']:
                taxDetailCount = len(draft_dict['RECAP_TAX_OP']['TAXES']['TAX_DETAIL'])
                if taxDetailCount > 0:
                    for tax in draft_dict['RECAP_TAX_OP']['TAXES']['TAX_DETAIL']:
                        if '@taxNum' in tax:
                            taxDetailNumber = tax['@taxNum']
                        else:
                            taxDetailNumber = ''

                        if '@rate' in tax:
                            taxDetailRate = tax['@rate']
                        else:
                            taxDetailRate = ''

                        if '@desc' in tax:
                            taxDetailDesc = tax['@desc']
                        else:
                            taxDetailDesc = ''

                        roughDraft_recapTaxOP_list.append([
                            taxDetailNumber,
                            taxDetailRate,
                            taxDetailDesc,
                            fileTransactionID,
                            file
                        ])
        else:
            recapTaxLineItemsOverhead = ''
            recapTaxLineItemsProfit = ''
            recapTaxOverheadAmt = ''
            recapTaxOverheadRate = ''
            recapTaxProfitAmt = ''
            recapTaxProfitRate = ''

        if 'CLOSING_STATEMENT' in draft_dict:
            closingStatement = ' '.join(draft_dict['CLOSING_STATEMENT'].strip())
        else:
            closingStatement = ''

        if 'PT_TOTALS' in draft_dict:
            if '@ptNumPmts' in draft_dict['PT_TOTALS']:
                ptTotals = draft_dict['PT_TOTALS']['@ptNumPmts']
            else:
                ptTotals = ''
            if 'PT_PAYMENTS' in draft_dict['PT_TOTALS']:
                if 'PT_PAYMENT' in draft_dict['PT_TOTALS']['PT_PAYMENTS']:
                    for payment in draft_dict['PT_TOTALS']['PT_PAYMENTS']['PT_PAYMENT']:

                        if '@date' in payment:
                            paymentDate = payment['@date']
                        else:
                            paymentDate = ''

                        if '@userID' in payment:
                            paymentUserID = payment['@userID']
                        else:
                            paymentUserID = ''

                        roughDraft_payments_list.append([
                            paymentDate,
                            paymentUserID,
                            fileTransactionID,
                            file
                        ])
        else:
            ptTotals = ''

        roughDraft_final_list.append([
            fileXMLNS,
            fileMajorVersion,
            fileMinorVersion,
            headerCompName,
            headerDateCreated,
            estimateInfoCarrierID,
            estimateInfoClaimNumber,
            estimateInfoDeprMat,
            estimateInfoDeprNonMat,
            estimateInfoDeprTaxes,
            estimateInfoEstimateName,
            estimateInfoEstimateType,
            estimateInfoInspectionNotPerformed,
            estimateInfoInsuredName,
            estimateInfoLaborEff,
            estimateInfoPolicyNumber,
            estimateInfoOpeningStatement,
            dateLoss,
            dateCompleted,
            dateReceived,
            dateEntered,
            dateContacted,
            lineItemDetailContractorTotal,
            lineItemDetailHomeownerTotal,
            lineItemDetailTotal,
            recapByRoomTotal,
            recapCategorySubtotalACV,
            recapCategorySubtotalDeprec,
            recapCategorySubtotalRCV,
            recapCategoryNonOPSubtotalACV,
            recapCategoryNonOPSubtotalPercentage,
            recapCategoryNonOPSubtotalDeprec,
            recapCategoryNonOPSubtotalRCV,
            recapCategoryOPSubtotalACV,
            recapCategoryOPSubtotalDeprec,
            recapCategoryOPSubtotalPercentage,
            recapCategoryOPSubtotalRCV,
            recapCategoryOverheadPercentage,
            recapCategoryOverheadRCV,
            recapCategoryProfitPercentage,
            recapCategoryProfitRCV,
            recapCategoryTaxACV,
            recapCategoryTaxDeprec,
            recapCategoryTaxDesc,
            recapCategoryTaxPercentage,
            recapCategoryTaxRCV,
            recapTaxLineItemsOverhead,
            recapTaxLineItemsProfit,
            recapTaxOverheadAmt,
            recapTaxOverheadRate,
            recapTaxProfitAmt,
            recapTaxProfitRate,
            closingStatement,
            ptTotals,
            fileTransactionID,
            file
        ])

    def estimate_run(file, estimate_test):
        estimate_dict = estimate_schema.to_dict(estimate_text)

        file = file.key

        XactnetInfoAssignmentType = estimate_dict['XACTNET_INFO']['@assignmentType']
        XactnetInfoBusinessUnit = estimate_dict['XACTNET_INFO']['@businessUnit']

        if '@carrierId' in estimate_dict['XACTNET_INFO']:
            XactnetCarrierID = estimate_dict['XACTNET_INFO']['@carrierId']
        else:
            XactnetCarrierID = ''

        if '@carrierName' in estimate_dict['XACTNET_INFO']:
            XactnetCarrierName = estimate_dict['XACTNET_INFO']['@carrierName']
        else:
            XactnetCarrierName = ''

        if '@carrierOffice1' in estimate_dict['XACTNET_INFO']:
            XactnetCarrierOffice1 = estimate_dict['XACTNET_INFO']['@carrierOffice1']
        else:
            XactnetCarrierOffice1 = ''

        if '@carrierOffice2' in estimate_dict['XACTNET_INFO']:
            XactnetCarrierOffice2 = estimate_dict['XACTNET_INFO']['@carrierOffice2']
        else:
            XactnetCarrierOffice2 = ''

        if '@creatorsEmailAddress' in estimate_dict['XACTNET_INFO']:
            XactnetCreatorEmailAddress = estimate_dict['XACTNET_INFO']['@creatorsEmailAddress']
        else:
            XactnetCreatorEmailAddress = ''

        if '@creatorsFirstName' in estimate_dict['XACTNET_INFO']:
            XactnetCreatorFirstName = estimate_dict['XACTNET_INFO']['@creatorsFirstName']
        else:
            XactnetCreatorFirstName = ''

        if '@creatorsLastName' in estimate_dict['XACTNET_INFO']:
            XactnetCreatorLastName = estimate_dict['XACTNET_INFO']['@creatorsLastName']
        else:
            XactnetCreatorLastName = ''

        if '@creatorsUserNumber' in estimate_dict['XACTNET_INFO']:
            XactnetCreatorUserNumber = estimate_dict['XACTNET_INFO']['@creatorsUserNumber']
        else:
            XactnetCreatorUserNumber = ''

        if '@emergency' in estimate_dict['XACTNET_INFO']:
            XactnetEmergency = estimate_dict['XACTNET_INFO']['@emergency']
        else:
            XactnetEmergency = ''

        if '@estimateCount' in estimate_dict['XACTNET_INFO']:
            XactnetEstimateCount = estimate_dict['XACTNET_INFO']['@estimateCount']
        else:
            XactnetEstimateCount = ''

        if '@jobSizeCode' in estimate_dict['XACTNET_INFO']:
            XactnetJobSizeCode = estimate_dict['XACTNET_INFO']['@jobSizeCode']
        else:
            XactnetJobSizeCode = ''

        if '@mitigation' in estimate_dict['XACTNET_INFO']:
            XactnetMitigation = estimate_dict['XACTNET_INFO']['@mitigation']
        else:
            XactnetMitigation = ''

        if '@profileCode' in estimate_dict['XACTNET_INFO']:
            XactnetProfileCode = estimate_dict['XACTNET_INFO']['@profileCode']
        else:
            XactnetProfileCode = ''

        if '@recipientsXM8UserId' in estimate_dict['XACTNET_INFO']:
            XactnetRecipientsXM8UserID = estimate_dict['XACTNET_INFO']['@recipientsXM8UserId']
        else:
            XactnetRecipientsXM8UserID = ''

        if '@rotationTrade' in estimate_dict['XACTNET_INFO']:
            XactnetrotationTrade = estimate_dict['XACTNET_INFO']['@rotationTrade']
        else:
            XactnetrotationTrade = ''

        if '@senderId' in estimate_dict['XACTNET_INFO']:
            XactnetSenderID = estimate_dict['XACTNET_INFO']['@senderId']
        else:
            XactnetSenderID = ''

        if '@sendersOfficeDescription1' in estimate_dict['XACTNET_INFO']:
            XactnetSendersOfficeDescription1 = estimate_dict['XACTNET_INFO']['@sendersOfficeDescription1']
        else:
            XactnetSendersOfficeDescription1 = ''

        if '@sendersOfficeDescription2' in estimate_dict['XACTNET_INFO']:
            XactnetSendersOfficeDescription2 = estimate_dict['XACTNET_INFO']['@sendersOfficeDescription2']
        else:
            XactnetSendersOfficeDescription2 = ''

        if '@sendersXNAddress' in estimate_dict['XACTNET_INFO']:
            XactnetSendersXNAddress = estimate_dict['XACTNET_INFO']['@sendersXNAddress']
        else:
            XactnetSendersXNAddress = ''

        if '@transactionId' in estimate_dict['XACTNET_INFO']:
            XactnetTransactionId = estimate_dict['XACTNET_INFO']['@transactionId']
        else:
            XactnetTransactionId = ''

        if '@transactionType' in estimate_dict['XACTNET_INFO']:
            XactnetTransactionType = estimate_dict['XACTNET_INFO']['@transactionType']
        else:
            XactnetTransactionType = ''

        # Xactnet Info -> Attachments Branch
        attFileCount = len(estimate_dict['XACTNET_INFO']['ATTACHMENTS']['ATT_FILE'])
        if attFileCount > 0:
            for attachment in estimate_dict['XACTNET_INFO']['ATTACHMENTS']['ATT_FILE']:
                attFileDate = attachment['@fileDate']
                attFileDesc = attachment['@fileDesc']
                attFileName = attachment['@fileName']
                attFileType = attachment['@fileType']

                estimate_attachments_list.append([
                    attFileDate,
                    attFileDesc,
                    attFileName,
                    attFileType,
                    XactnetTransactionId,
                    file
                ])

        # Xactnet Info -> Control Points branch
        controlPointsReferral = estimate_dict['XACTNET_INFO']['CONTROL_POINTS']['@referral']
        controlPointsTestAssignment = estimate_dict['XACTNET_INFO']['CONTROL_POINTS']['@testAssignment']
        controlPointsCount = len(estimate_dict['XACTNET_INFO']['CONTROL_POINTS']['CONTROL_POINT'])
        if controlPointsCount > 0:
            for point in estimate_dict['XACTNET_INFO']['CONTROL_POINTS']['CONTROL_POINT']:
                controlPointStamp = point['@stamp']
                controlPointType = point['@type']

                estimate_control_points_list.append([
                    controlPointStamp,
                    controlPointType,
                    controlPointsReferral,
                    controlPointsTestAssignment,
                    XactnetTransactionId,
                    file
                ])

        # Xactnet Info -> Summary Branch
        summaryContractorItems = estimate_dict['XACTNET_INFO']['SUMMARY']['@contractorItems']
        summaryDeductible = estimate_dict['XACTNET_INFO']['SUMMARY']['@deductible']
        summaryEstimateLineItemTotal = estimate_dict['XACTNET_INFO']['SUMMARY']['@estimateLineItemTotal']
        summaryGrossEstimate = estimate_dict['XACTNET_INFO']['SUMMARY']['@grossEstimate']
        summaryHomeOwnerItems = estimate_dict['XACTNET_INFO']['SUMMARY']['@homeOwnerItems']
        summaryMinimumChargeAdjustments = estimate_dict['XACTNET_INFO']['SUMMARY'][
            '@minimumChargeAdjustments']
        summaryNetEstimate = estimate_dict['XACTNET_INFO']['SUMMARY']['@netEstimate']
        summaryNonRecoverableDepreciation = estimate_dict['XACTNET_INFO']['SUMMARY'][
            '@nonRecoverableDepreciation']
        summaryOverhead = estimate_dict['XACTNET_INFO']['SUMMARY']['@overhead']
        summaryPriceListLineItemTotal = estimate_dict['XACTNET_INFO']['SUMMARY']['@priceListLineItemTotal']
        summaryProfit = estimate_dict['XACTNET_INFO']['SUMMARY']['@profit']
        summaryRecoverableDepreciation = estimate_dict['XACTNET_INFO']['SUMMARY'][
            '@recoverableDepreciation']
        summarySalesTax = estimate_dict['XACTNET_INFO']['SUMMARY']['@salesTax']

        # Project Info branch
        projectInfoAssignmentCode = estimate_dict['PROJECT_INFO']['@assignmentCode']
        projectInfoCreated = estimate_dict['PROJECT_INFO']['@created']
        projectInfoName = estimate_dict['PROJECT_INFO']['@name']
        projectInfoProfile = estimate_dict['PROJECT_INFO']['@profile']
        projectInfoShowDeskAdjuster = estimate_dict['PROJECT_INFO']['@showDeskAdjuster']
        projectInfoShowIADeskAdjuster = estimate_dict['PROJECT_INFO']['@showIADeskAdjuster']
        projectInfoStatus = estimate_dict['PROJECT_INFO']['@status']
        projectInfoUserID = estimate_dict['PROJECT_INFO']['@userId']
        projectInfoVersion = estimate_dict['PROJECT_INFO']['@version']
        projectInfoXactnetAddress = estimate_dict['PROJECT_INFO']['@xactnetAddress']
        if 'NOTES' in estimate_dict['PROJECT_INFO']:
            projectInfoNotes = ' '.join(estimate_dict['PROJECT_INFO']['NOTES'].split())
        else:
            projectInfoNotes = ''

        # Params Branch
        paramsCheckpointPL = estimate_dict['PARAMS']['@checkpointPL']
        paramsCumulativeOP = estimate_dict['PARAMS']['@cumulativeOP']
        paramsDefaultRepairedBy = estimate_dict['PARAMS']['@defaultRepairedBy']

        if '@depMat' in estimate_dict['PARAMS']:
            paramsDepMat = estimate_dict['PARAMS']['@depMat']
        else:
            paramsDepMat = ''

        if '@depNonMat' in estimate_dict['PARAMS']:
            paramsDepNonMat = estimate_dict['PARAMS']['@depNonMat']
        else:
            paramsDepNonMat = ''
        if '@depTaxes' in estimate_dict['PARAMS']:
            paramsDepTaxes = estimate_dict['PARAMS']['@depTaxes']
        else:
            paramsDepTaxes = ''

        if '@maxDepr' in estimate_dict['PARAMS']:
            paramsMaxDepr = estimate_dict['PARAMS']['@maxDepr']
        else:
            paramsMaxDepr = ''

        if '@overhead' in estimate_dict['PARAMS']:
            paramsOverhead = estimate_dict['PARAMS']['@overhead']
        else:
            paramsOverhead = ''

        if '@plModifiedDateTime' in estimate_dict['PARAMS']:
            paramsPlModifiedDateTime = estimate_dict['PARAMS']['@plModifiedDateTime']
        else:
            paramsPlModifiedDateTime = ''

        if '@priceList' in estimate_dict['PARAMS']:
            paramsPriceList = estimate_dict['PARAMS']['@priceList']
        else:
            paramsPriceList = ''

        if '@profit' in estimate_dict['PARAMS']:
            paramsProfit = estimate_dict['PARAMS']['@profit']
        else:
            paramsProfit = ''

        if '@taxJurisdiction' in estimate_dict['PARAMS']:
            paramsTaxJurisdiction = estimate_dict['PARAMS']['@taxJurisdiction']
        else:
            paramsTaxJurisdiction = ''

        # ADM Branch
        if '@agentCode' in estimate_dict['ADM']:
            admAgentCode = estimate_dict['ADM']['@agentCode']
        else:
            admAgentCode = ''

        if '' in estimate_dict['ADM']:
            admDateContacted = estimate_dict['ADM']['@dateContacted']
        else:
            admDateContacted = ''

        if '' in estimate_dict['ADM']:
            admDateEntered = estimate_dict['ADM']['@dateEntered']
        else:
            admDateEntered = ''

        if '@dateInspected' in estimate_dict['ADM']:
            admDateInspected = estimate_dict['ADM']['@dateInspected']
        else:
            admDateInspected = ''

        if '@dateOfLoss' in estimate_dict['ADM']:
            admDateOfLoss = estimate_dict['ADM']['@dateOfLoss']
        else:
            admDateOfLoss = ''

        if 'dateProjCompleted' in estimate_dict['ADM']:
            admDateProjCompleted = estimate_dict['ADM']['@dateProjCompleted']
        else:
            admDateProjCompleted = ''

        if '@dateReceived' in estimate_dict['ADM']:
            admDateReceived = estimate_dict['ADM']['@dateReceived']
        else:
            admDateReceived = ''

        if '@denial' in estimate_dict['ADM']:
            admDenial = estimate_dict['ADM']['@denial']
        else:
            admDenial = ''

        if '@fileNumber' in estimate_dict['ADM']:
            admFileNumber = estimate_dict['ADM']['@fileNumber']
        else:
            admFileNumber = ''

        # Coverage Loss Branch
        admCat = estimate_dict['ADM']['COVERAGE_LOSS']['@catastrophe']
        admClaimNumber = estimate_dict['ADM']['COVERAGE_LOSS']['@claimNumber']

        if '@dedApplyAcrossAllUI' in estimate_dict['ADM']['COVERAGE_LOSS']:
            admDedApplyAcrossALLUI = estimate_dict['ADM']['COVERAGE_LOSS']['@dedApplyAcrossAllUI']
        else:
            admDedApplyAcrossALLUI = ''

        admPolicyNumber = estimate_dict['ADM']['COVERAGE_LOSS']['@policyNumber']

        # TOL Branch
        admCoverageLossType = estimate_dict['ADM']['COVERAGE_LOSS']['TOL']['@code']
        admCoverageLossDesc = estimate_dict['ADM']['COVERAGE_LOSS']['TOL']['@desc']

        admLossOfUseReserve = estimate_dict['ADM']['COVERAGE_LOSS']['COVERAGES']['@lossOfUseReserve']
        admLossOfUse = estimate_dict['ADM']['COVERAGE_LOSS']['COVERAGES']['@lossOfUse']
        admDoNotApplyLimits = estimate_dict['ADM']['COVERAGE_LOSS']['COVERAGES']['@doNotApplyLimits']

        coverageItemCount = len(estimate_dict['ADM']['COVERAGE_LOSS']['COVERAGES']['COVERAGE'])
        if coverageItemCount > 0:
            for coverage in estimate_dict['ADM']['COVERAGE_LOSS']['COVERAGES']['COVERAGE']:
                covApplyTo = coverage['@applyTo']
                covCoins = coverage['@coins']
                covName = coverage['@covName']
                covType = coverage['@covType']
                covDeductible = coverage['@deductible']
                covID = coverage['@id']
                covPolicyLimit = coverage['@policyLimit']
                covReserveAmt = coverage['@reserveAmt']
                covDeductApplied = coverage['LOSS_DATA']['@deductApplied']
                covOverLimits = coverage['LOSS_DATA']['@overLimits']

                estimate_coverage_list.append([
                    covApplyTo,
                    covCoins,
                    covName,
                    covType,
                    covDeductible,
                    covID,
                    covPolicyLimit,
                    covReserveAmt,
                    covDeductApplied,
                    covOverLimits,
                    admClaimNumber,
                    admPolicyNumber,
                    admCat,
                    XactnetTransactionId,
                    file
                ])

        # Claim Info branch
        if '@branch' in estimate_dict['CLAIM_INFO']['ADMIN_INFO']:
            adminBranch = estimate_dict['CLAIM_INFO']['ADMIN_INFO']['@branch']
        else:
            adminBranch = ''

        if '@accessName' in estimate_dict['CLAIM_INFO']['ADMIN_INFO']:
            adminAccessName = estimate_dict['CLAIM_INFO']['ADMIN_INFO']['@accessName']
        else:
            adminAccessName = ''

        if '@accessPhone' in estimate_dict['CLAIM_INFO']['ADMIN_INFO']:
            adminAccessPhone = estimate_dict['CLAIM_INFO']['ADMIN_INFO']['@accessPhone']
        else:
            adminAccessPhone = ''

        # Contacts Branch
        contactCount = len(estimate_dict['CONTACTS']['CONTACT'])
        if contactCount > 0:
            for item in estimate_dict['CONTACTS']['CONTACT']:
                contactID = XactnetTransactionId + '||' + str(
                    estimate_dict['CONTACTS']['CONTACT'].index(item))

                if '@birthDate' in item:
                    contactBirthdate = item['@birthDate']
                else:
                    contactBirthdate = ''

                if '@language' in item:
                    contactLanguage = item['@language']
                else:
                    contactLanguage = ''

                if '@name' in item:
                    contactName = item['@name']
                else:
                    contactName = ''

                if 'qcode' in item:
                    contactQCode = item['@qcode']
                else:
                    contactQCode = ''

                if '@reference' in item:
                    contactReference = item['@reference']
                else:
                    contactReference = ''

                if '@title' in item:
                    contactTitle = item['@title']
                else:
                    contactTitle = ''

                if '@type' in item:
                    contactType = item['@type']
                else:
                    contactType = ''

                    estimate_contacts_list.append([
                        contactID,
                        contactBirthdate,
                        contactLanguage,
                        contactName,
                        contactQCode,
                        contactReference,
                        contactTitle,
                        contactType
                    ])

                if 'ADDRESSES' in item:
                    if item['ADDRESSES'] != None:
                        addressCount = len(item['ADDRESSES']['ADDRESS'])
                        if addressCount > 0:
                            for address in item['ADDRESSES']['ADDRESS']:
                                contactAddressCity = address['@city']
                                contactAddressCounty = address['@country']
                                contactAddressPostal = address['@postal']
                                contactAddressState = address['@state']
                                contactAddressStreet = address['@street']
                                contactAddressType = address['@type']

                                estimate_address_list.append([
                                    contactID,
                                    contactAddressCity,
                                    contactAddressCounty,
                                    contactAddressPostal,
                                    contactAddressState,
                                    contactAddressStreet,
                                    contactAddressType,
                                    admClaimNumber,
                                    admPolicyNumber,
                                    addressCount,
                                    XactnetTransactionId,
                                    file
                                ])

                if 'CONTACTMETHODS' in item:
                    if item['CONTACTMETHODS'] != None:
                        if 'PHONE' in item['CONTACTMETHODS']:
                            if item['CONTACTMETHODS']['PHONE'] != None:
                                contactPhoneCount = len(item['CONTACTMETHODS']['PHONE'])
                                if contactPhoneCount > 0:
                                    for phone in item['CONTACTMETHODS']['PHONE']:
                                        contactPhoneType = phone['@type']
                                        contactPhoneNumber = phone['@number']

                                        estimate_phone_list.append([
                                            contactID,
                                            contactPhoneType,
                                            contactPhoneNumber,
                                            admClaimNumber,
                                            admPolicyNumber,
                                            contactPhoneCount,
                                            XactnetTransactionId,
                                            file
                                        ])
                                else:
                                    contactPhoneCount = 0
                                    contactPhoneType = ''
                                    contactPhoneNumber = ''

                                    estimate_phone_list.append([
                                        contactID,
                                        contactPhoneType,
                                        contactPhoneNumber,
                                        admClaimNumber,
                                        admPolicyNumber,
                                        contactPhoneCount,
                                        XactnetTransactionId,
                                        file
                                    ])

                        if 'EMAIL' in item['CONTACTMETHODS']:
                            if item['CONTACTMETHODS'] != None:
                                contactEmailCount = len(item['CONTACTMETHODS']['EMAIL'])
                                if contactEmailCount > 0:
                                    for email in item['CONTACTMETHODS']['EMAIL']:
                                        contactEmail = email['@address']

                                        estimate_email_list.append([
                                            contactID,
                                            contactEmail,
                                            admClaimNumber,
                                            admPolicyNumber,
                                            contactEmailCount,
                                            XactnetTransactionId,
                                            file
                                        ])
                                else:
                                    contactEmailCount = 0
                                    contactEmail = ''

                                    estimate_email_list.append([
                                        contactID,
                                        contactEmail,
                                        admClaimNumber,
                                        admPolicyNumber,
                                        contactEmailCount,
                                        XactnetTransactionId,
                                        file
                                    ])
                    else:
                        contactPhoneCount = 0
                        contactPhoneType = ''
                        contactPhoneNumber = ''

                        estimate_phone_list.append([
                            contactID,
                            contactPhoneType,
                            contactPhoneNumber,
                            admClaimNumber,
                            admPolicyNumber,
                            contactPhoneCount,
                            XactnetTransactionId,
                            file
                        ])

                        contactEmailCount = 0
                        contactEmail = ''

                        estimate_email_list.append([
                            contactID,
                            contactEmail,
                            admClaimNumber,
                            admPolicyNumber,
                            contactEmailCount,
                            XactnetTransactionId,
                            file
                        ])

        # Minimum Branch
        if 'EMBEDDED_PL' in estimate_dict:
            for minimum in estimate_dict['EMBEDDED_PL']['MINIMUMS']['MINIMUM']:
                minimumCount = len(estimate_dict['EMBEDDED_PL']['MINIMUMS']['MINIMUM'])
                minimumAmt = minimum['@amount']
                minimumCAT = minimum['@cat']
                minimumDesc = minimum['@desc']
                minimumID = minimum['@id']
                minimumSEL = minimum['@sel']

                estimate_EMBEDDED_PL_minimums_list.append([
                    minimumAmt,
                    minimumCAT,
                    minimumDesc,
                    minimumID,
                    minimumSEL,
                    XactnetTransactionId,
                    file
                ])

        estimate_final_list.append([
            XactnetInfoAssignmentType,
            XactnetInfoBusinessUnit,
            XactnetCarrierID,
            XactnetCarrierName,
            XactnetCarrierOffice1,
            XactnetCarrierOffice2,
            XactnetCreatorEmailAddress,
            XactnetCreatorFirstName,
            XactnetCreatorLastName,
            XactnetCreatorUserNumber,
            XactnetEmergency,
            XactnetEstimateCount,
            XactnetJobSizeCode,
            XactnetMitigation,
            XactnetProfileCode,
            XactnetRecipientsXM8UserID,
            XactnetrotationTrade,
            XactnetSenderID,
            XactnetSendersOfficeDescription1,
            XactnetSendersOfficeDescription2,
            XactnetSendersXNAddress,
            XactnetTransactionId,
            XactnetTransactionType,

            summaryContractorItems,
            summaryDeductible,
            summaryEstimateLineItemTotal,
            summaryGrossEstimate,
            summaryHomeOwnerItems,
            summaryMinimumChargeAdjustments,
            summaryNetEstimate,
            summaryNonRecoverableDepreciation,
            summaryOverhead,
            summaryPriceListLineItemTotal,
            summaryProfit,
            summaryRecoverableDepreciation,
            summarySalesTax,

            projectInfoAssignmentCode,
            projectInfoCreated,
            projectInfoName,
            projectInfoProfile,
            projectInfoShowDeskAdjuster,
            projectInfoShowIADeskAdjuster,
            projectInfoStatus,
            projectInfoUserID,
            projectInfoVersion,
            projectInfoXactnetAddress,
            projectInfoNotes,

            paramsCheckpointPL,
            paramsCumulativeOP,
            paramsDefaultRepairedBy,
            paramsDepMat,
            paramsDepNonMat,
            paramsDepTaxes,
            paramsMaxDepr,
            paramsOverhead,
            paramsPlModifiedDateTime,
            paramsPriceList,
            paramsProfit,
            paramsTaxJurisdiction,

            admAgentCode,
            admDateContacted,
            admDateEntered,
            admDateInspected,
            admDateOfLoss,
            admDateProjCompleted,
            admDateReceived,
            admDenial,
            admFileNumber,
            admCoverageLossType,
            admCoverageLossDesc,
            admLossOfUseReserve,
            admLossOfUse,
            admDoNotApplyLimits,

            adminBranch,
            adminAccessName,
            adminAccessPhone,

            file
        ])

    # Rough Draft List Setup
    roughDraft_final_list = []
    roughDraft_coverage_list = []
    roughDraft_summaryTaxes_list = []
    roughDraft_coversheet_address_list = []
    roughDraft_coversheet_phone_list = []
    roughDraft_contacts_address_list = []
    roughDraft_contacts_phone_list = []
    roughDraft_contacts_list = []
    roughDraft_lineItemDetailTotals_list = []
    roughDraft_lineItemDetailSFTotals_list = []
    roughDraft_lineItemDetailSF_list = []
    roughDraft_lineItemDetailItems_list = []
    roughDraft_recapByRoomDetail_list = []
    roughDraft_recapByCategory_list = []
    roughDraft_recapByCategoryCov_list = []
    roughDraft_recapByCategorySalesTaxes_list = []
    roughDraft_recapTaxOP_list = []
    roughDraft_recapTaxLineItemsTaxNum_list = []
    roughDraft_payments_list = []

    # Notification List Setup
    notification_final_list = []
    notification_phone_list = []
    notification_email_list = []

    # Estimate List Setup
    estimate_final_list = []
    estimate_contacts_list = []
    estimate_EMBEDDED_PL_minimums_list = []
    estimate_address_list = []
    estimate_coverage_list = []
    estimate_phone_list = []
    estimate_email_list = []
    estimate_attachments_list = []
    estimate_control_points_list = []

    # Parsing List Setup
    fileFailures_list = []
    parsing_summary_list = []

    # Parsed File Counters
    total_items_scanned_counter = 0
    estimate_success_counter = 0
    rough_draft_success_counter = 0
    notification_success_counter = 0
    estimate_failure_counter = 0
    rough_draft_failure_counter = 0
    notification_failure_counter = 0

    scriptStartTime = datetime.now()

    with open('/home/hadoop/processed/xactimate/credentials/aws_credentials.txt') as json_file:
        aws_credentials = json.load(json_file)

    athena_client, s3_client, s3_resource = create_aws_objects(user='SVC_L4_XACTIMATEPROD', aws_credentials=aws_credentials)

    athenaParsedRoughDraft_list, athenaParsedNotification_list, athenaParsedEstimate_list, athenaParsedFailures_list = retreive_athena_parsed_files()
    xactimate_bucket = s3_resource.Bucket('xactimateanalysisprod')
    fs = s3fs.S3FileSystem(key=aws_credentials['SVC_L4_XACTIMATEPROD']['ACCESS_KEY'], secret=aws_credentials['SVC_L4_XACTIMATEPROD']['SECRET_KEY'])

    rough_draft_schema = xmlschema.XMLSchema('/home/hadoop/processed/xactimate/schema_files/generic_roughdraft_v9.17.17.xsd')
    notification_schema = xmlschema.XMLSchema('/home/hadoop/processed/xactimate/schema_files/StandardStatusExport.xsd')
    estimate_schema = xmlschema.XMLSchema('/home/hadoop/processed/xactimate/schema_files/StandardCarrierExport-v1.7.xsd')

    for file in xactimate_bucket.objects.all():

        total_items_scanned_counter = total_items_scanned_counter + 1

        if 'estimate' in file.key.lower() and not (file.key in athenaParsedRoughDraft_list or file.key in athenaParsedEstimate_list or file.key in athenaParsedFailures_list):
            estimate_object = s3_client.get_object(Bucket='xactimateanalysisprod',Key=file.key)
            estimate_text = estimate_object['Body'].read().decode('utf-8')
            try:
                if rough_draft_schema.is_valid(estimate_text):
                    roughDraft_run(file, estimate_text)
                    rough_draft_success_counter = rough_draft_success_counter + 1
            except OSError:
                rough_draft_failure_counter = rough_draft_failure_counter + 1
                fileFailures_list.append([
                    datetime.now(),
                    'RoughDraft',
                    file.key
                ])

            try:
                if estimate_schema.is_valid(estimate_text):
                    estimate_run(file, estimate_text)
                    estimate_success_counter = estimate_success_counter + 1
            except OSError:
                estimate_failure_counter = estimate_failure_counter + 1
                fileFailures_list.append([
                    datetime.now(),
                    'Estimate',
                    file.key
                ])

        if 'notification' in file.key.lower() and not (file.key in athenaParsedNotification_list or file.key in athenaParsedFailures_list):
            notification_object = s3_client.get_object(Bucket='xactimateanalysisprod', Key=file.key)
            notification_text = notification_object['Body'].read().decode('utf-8')
            try:
                if notification_schema.is_valid(notification_text):
                    notification_run(file, notification_text)
                    notification_success_counter = notification_success_counter + 1
            except OSError:
                notification_failure_counter = notification_failure_counter + 1
                fileFailures_list.append([
                    datetime.now(),
                    'Notification',
                    file.key
                ])


    if roughDraft_final_list != []:
        export_rough_draft_files()

    if estimate_final_list != []:
        export_estimate_files()

    if notification_final_list != []:
        export_notification_files()

    scriptEndTime = datetime.now()

    parsing_summary_list.append([
        scriptStartTime,
        scriptEndTime,
        (scriptEndTime - scriptStartTime).total_seconds(),
        estimate_success_counter,
        rough_draft_success_counter,
        notification_success_counter,
        estimate_failure_counter,
        rough_draft_failure_counter,
        notification_failure_counter,
        total_items_scanned_counter
    ])

    export_parsing_support_files()

if __name__ == '__main__':
    main()

