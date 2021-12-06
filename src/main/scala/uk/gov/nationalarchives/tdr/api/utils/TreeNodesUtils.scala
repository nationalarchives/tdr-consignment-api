package uk.gov.nationalarchives.tdr.api.utils

import java.io.{File => JIOFile}
import java.util.UUID

import uk.gov.nationalarchives.tdr.api.model.file.FileType.{fileTypeIdentifier, folderTypeIdentifier}
import uk.gov.nationalarchives.tdr.api.service.UUIDSource

import scala.annotation.tailrec

class TreeNodesUtils(uuidSource: UUIDSource) {
  def generateNodes(filePaths: Set[String]): Map[String, TreeNode] = {
    val pathToNode: Map[String, TreeNode] = Map()

    @tailrec
    def innerFunction(originalPath: String, fileType: String, nodes: Map[String, TreeNode]): Map[String, TreeNode] = {
      val jioFile = new JIOFile(originalPath)
      val parentPath = Option(jioFile.getParent)
      val name = jioFile.getName
      val treeNode = TreeNode(uuidSource.uuid, name, parentPath, fileType)

      if (parentPath.isEmpty && !nodes.contains(originalPath)) {
        nodes + (originalPath -> treeNode)
      } else if (nodes.contains(parentPath.get))
      { nodes } else {
        innerFunction(parentPath.get, folderTypeIdentifier, nodes + (originalPath -> treeNode))
      }
    }

    filePaths.flatten(path => {
      innerFunction(path, fileTypeIdentifier, pathToNode)
    }).toMap
  }
}

object TreeNodesUtils {
  def apply(uuidSource: UUIDSource): TreeNodesUtils = new TreeNodesUtils(uuidSource)
}

case class TreeNode(id: UUID, name: String, parentPath: Option[String], treeNodeType: String)
