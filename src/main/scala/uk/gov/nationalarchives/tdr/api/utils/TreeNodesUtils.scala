package uk.gov.nationalarchives.tdr.api.utils

import java.io.{File => JIOFile}
import java.util.UUID
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.{fileTypeIdentifier, folderTypeIdentifier}
import uk.gov.nationalarchives.tdr.api.service.UUIDSource
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils.TreeNode

import scala.annotation.tailrec

class TreeNodesUtils(uuidSource: UUIDSource) {
  def generateNodes(filePaths: Set[String]): Map[String, TreeNode] = {
    @tailrec
    def innerFunction(originalPath: String, fileType: String, nodes: Map[String, TreeNode]): Map[String, TreeNode] = {
      val pathWithoutInitialSlash: String = if (originalPath.startsWith("/")) originalPath.tail else originalPath
      val jioFile = new JIOFile(pathWithoutInitialSlash)
      val parentPath = Option(jioFile.getParent)
      val name = jioFile.getName
      val treeNode = TreeNode(uuidSource.uuid, name, parentPath, fileType)
      val nextMap = nodes + (pathWithoutInitialSlash -> treeNode)
      if (parentPath.isEmpty) {
        nextMap
      } else {
        innerFunction(parentPath.get, folderTypeIdentifier, nextMap)
      }
    }

    filePaths.flatMap(path => {
      innerFunction(path, fileTypeIdentifier, Map())
    }).toMap
  }
}

object TreeNodesUtils {
  def apply(uuidSource: UUIDSource): TreeNodesUtils = new TreeNodesUtils(uuidSource)
  case class TreeNode(id: UUID, name: String, parentPath: Option[String], treeNodeType: String)
}
