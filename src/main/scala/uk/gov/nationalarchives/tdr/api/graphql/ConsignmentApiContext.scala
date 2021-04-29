package uk.gov.nationalarchives.tdr.api.graphql
import uk.gov.nationalarchives.tdr.api.service._
import uk.gov.nationalarchives.tdr.keycloak.Token

case class ConsignmentApiContext(accessToken: Token,
                                 antivirusMetadataService: AntivirusMetadataService,
                                 clientFileMetadataService: ClientFileMetadataService,
                                 consignmentService: ConsignmentService,
                                 ffidMetadataService: FFIDMetadataService,
                                 fileMetadataService: FileMetadataService,
                                 fileService: FileService,
                                 finalTransferConfirmationService: FinalTransferConfirmationService,
                                 seriesService: SeriesService,
                                 transferAgreementService: TransferAgreementService,
                                 transferringBodyService: TransferringBodyService,
                                 consignmentStatusService: ConsignmentStatusService,
                                 fileStatusService: FileStatusService
                                 )
