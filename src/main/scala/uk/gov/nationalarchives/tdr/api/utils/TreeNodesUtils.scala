package uk.gov.nationalarchives.tdr.api.utils

import com.typesafe.config.Config
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.directoryTypeIdentifier
import uk.gov.nationalarchives.tdr.api.service.ReferenceGeneratorService.Reference
import uk.gov.nationalarchives.tdr.api.service.{ReferenceGeneratorService, UUIDSource}
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils.{TreeNode, TreeNodeInput}

import java.io.{File => JIOFile}
import java.util.UUID
import scala.annotation.tailrec

class TreeNodesUtils(uuidSource: UUIDSource, referenceGeneratorService: ReferenceGeneratorService, config: Config) {

  @tailrec
  private def innerFunction(originalPath: String, clientsideUUID: Option[UUID], typeIdentifier: String, nodes: Map[String, TreeNode]): Map[String, TreeNode] = {
    val jioFile = new JIOFile(originalPath)
    val parentPath = Option(jioFile.getParent)
    val name = jioFile.getName
    val uuid = clientsideUUID.getOrElse(uuidSource.uuid)
    val treeNode = TreeNode(uuid, name, parentPath, typeIdentifier, None)
    val nextMap = nodes + (originalPath -> treeNode)
    if (parentPath.isEmpty) {
      nextMap
    } else {
      innerFunction(parentPath.get, None, directoryTypeIdentifier, nextMap)
    }
  }

  def generateNodes(inputs: Set[TreeNodeInput], typeIdentifier: String): Map[String, TreeNode] = {
    val generatedNodes = inputs.flatMap { input =>
      val path = input.filePath
      val pathWithoutInitialSlash: String = if (path.startsWith("/")) path.tail else path
      val clientsideUUID = input.clientsideUuid
      innerFunction(pathWithoutInitialSlash, clientsideUUID, typeIdentifier, Map())
    }.toMap
    val generatedReferences = referenceGeneratorService.getReferences(generatedNodes.size)
    generatedReferences
      .zip(generatedNodes.view)
      .map { case (reference, (key, treenode)) =>
        key -> treenode.copy(reference = Some(reference))
      }
      .toMap
  }
}

object TreeNodesUtils {
  def apply(uuidSource: UUIDSource, referenceGeneratorService: ReferenceGeneratorService, config: Config): TreeNodesUtils =
    new TreeNodesUtils(uuidSource, referenceGeneratorService, config)

  case class TreeNodeInput(filePath: String, clientsideUuid: Option[UUID])
  case class TreeNode(id: UUID, name: String, parentPath: Option[String], treeNodeType: String, reference: Option[Reference])
}
