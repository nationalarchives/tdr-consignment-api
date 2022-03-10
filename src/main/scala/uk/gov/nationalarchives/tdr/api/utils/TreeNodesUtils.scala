package uk.gov.nationalarchives.tdr.api.utils

import uk.gov.nationalarchives.tdr.api.model.file.NodeType.directoryTypeIdentifier
import uk.gov.nationalarchives.tdr.api.service.UUIDSource
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils.TreeNode

import java.io.{File => JIOFile}
import java.util.UUID
import scala.annotation.tailrec

class TreeNodesUtils(uuidSource: UUIDSource) {
  @tailrec
  private def innerFunction(originalPath: String, typeIdentifier: String, nodes: Map[String, TreeNode]): Map[String, TreeNode] = {
    val jioFile = new JIOFile(originalPath)
    val parentPath = Option(jioFile.getParent)
    val name = jioFile.getName
    val treeNode = TreeNode(uuidSource.uuid, name, parentPath, typeIdentifier)
    val nextMap = nodes + (originalPath -> treeNode)
    if (parentPath.isEmpty) {
      nextMap
    } else {
      innerFunction(parentPath.get, directoryTypeIdentifier, nextMap)
    }
  }

  def generateNodes(filePaths: Set[String], typeIdentifier: String): Map[String, TreeNode] = {
    filePaths.flatMap(path => {
      val pathWithoutInitialSlash: String = if (path.startsWith("/")) path.tail else path
      innerFunction(pathWithoutInitialSlash, typeIdentifier, Map())
    }).toMap
  }
}

object TreeNodesUtils {
  def apply(uuidSource: UUIDSource): TreeNodesUtils = new TreeNodesUtils(uuidSource)
  case class TreeNode(id: UUID, name: String, parentPath: Option[String], treeNodeType: String)
}
