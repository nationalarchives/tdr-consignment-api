package uk.gov.nationalarchives.tdr.api.utils

import com.typesafe.config.Config
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.directoryTypeIdentifier
import uk.gov.nationalarchives.tdr.api.service.ReferenceGeneratorService.Reference
import uk.gov.nationalarchives.tdr.api.service.{ReferenceGeneratorService, UUIDSource}
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils.TreeNode

import java.io.{File => JIOFile}
import java.util.UUID
import scala.annotation.tailrec

class TreeNodesUtils(uuidSource: UUIDSource, referenceGeneratorService: ReferenceGeneratorService, config: Config) {

  private val referenceGeneratorFeatureBlock: Boolean = config.getBoolean("featureAccessBlock.assignFileReferences")

  @tailrec
  private def innerFunction(originalPath: String, typeIdentifier: String, nodes: Map[String, TreeNode]): Map[String, TreeNode] = {
    val jioFile = new JIOFile(originalPath)
    val parentPath = Option(jioFile.getParent)
    val name = jioFile.getName
    val treeNode = TreeNode(uuidSource.uuid, name, parentPath, typeIdentifier, None)
    val nextMap = nodes + (originalPath -> treeNode)
    if (parentPath.isEmpty) {
      nextMap
    } else {
      innerFunction(parentPath.get, directoryTypeIdentifier, nextMap)
    }
  }

  def generateNodes(filePaths: Set[String], typeIdentifier: String): Map[String, TreeNode] = {
    val generatedNodes = filePaths.flatMap { path =>
      val pathWithoutInitialSlash: String = if (path.startsWith("/")) path.tail else path
      innerFunction(pathWithoutInitialSlash, typeIdentifier, Map())
    }.toMap
    if (referenceGeneratorFeatureBlock) {
      generatedNodes
    } else {
      val generatedReferences = referenceGeneratorService.getReferences(generatedNodes.size)
      generatedReferences
        .zip(generatedNodes.view)
        .map { case (reference, (key, treenode)) =>
          key -> treenode.copy(reference = Some(reference))
        }
        .toMap
    }
  }
}

object TreeNodesUtils {
  def apply(uuidSource: UUIDSource, referenceGeneratorService: ReferenceGeneratorService, config: Config): TreeNodesUtils =
    new TreeNodesUtils(uuidSource, referenceGeneratorService, config)
  case class TreeNode(id: UUID, name: String, parentPath: Option[String], treeNodeType: String, reference: Option[Reference])
}
