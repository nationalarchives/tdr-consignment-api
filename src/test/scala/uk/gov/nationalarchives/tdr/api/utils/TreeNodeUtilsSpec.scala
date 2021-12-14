package uk.gov.nationalarchives.tdr.api.utils

import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.model.file.NodeType

class TreeNodeUtilsSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {

  "generateNodes" should "generate the correct nodes for a nested path" in {
    val result = TreeNodesUtils(new FixedUUIDSource).generateNodes(Set("/a/path/with/some/nested/folders/file"))
    val expectedSize = 8;
    result.size should equal(expectedSize)
    List(
      ("/", None, ""),
      ("/a", Some("/"), "a"),
      ("/a/path", Some("/a"), "path"),
      ("/a/path/with", Some("/a/path"), "with"),
      ("/a/path/with/some", Some("/a/path/with"), "some"),
      ("/a/path/with/some/nested",Some("/a/path/with/some"), "nested"),
      ("/a/path/with/some/nested/folders", Some("/a/path/with/some/nested"), "folders")
    ).foreach(pathWithParent => {
      val (path, parent, name) = pathWithParent
      val treeNode = result.get(path)
      treeNode.isDefined should be(true)
      treeNode.get.treeNodeType should equal(NodeType.folderTypeIdentifier)
      treeNode.get.parentPath should equal(parent)
      treeNode.get.name should equal(name)
    })
    val folderPath = "/a/path/with/some/nested/folders"
    val fileTreeNode = result.get(s"$folderPath/file")
    fileTreeNode.isDefined should be(true)
    fileTreeNode.get.treeNodeType should be(NodeType.fileTypeIdentifier)
    fileTreeNode.get.name should equal("file")
    fileTreeNode.get.parentPath.get should be(folderPath)
  }

  "generateNodes" should "generate the correct nodes for a single file" in {
    val fileName = "file"
    val result = TreeNodesUtils(new FixedUUIDSource).generateNodes(Set(fileName))
    result.size should equal(1)
    val treeNode = result.get(fileName)
    treeNode.isDefined should be(true)
    treeNode.get.treeNodeType should be(NodeType.fileTypeIdentifier)
    treeNode.get.parentPath.isEmpty should be(true)
    treeNode.get.name should be(fileName)
  }
}
