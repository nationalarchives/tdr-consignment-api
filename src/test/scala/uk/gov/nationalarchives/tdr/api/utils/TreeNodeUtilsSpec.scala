package uk.gov.nationalarchives.tdr.api.utils

import com.typesafe.config.ConfigFactory
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.fileTypeIdentifier
import uk.gov.nationalarchives.tdr.api.service.ReferenceGeneratorService
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils.TreeNodeInput

class TreeNodeUtilsSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  val referenceGeneratorServiceMock: ReferenceGeneratorService = mock[ReferenceGeneratorService]

  "generateNodes" should "generate the correct nodes for a nested path" in {
    when(referenceGeneratorServiceMock.getReferences(any[Int])).thenReturn(List("ref1", "ref2", "ref3", "ref4", "ref5", "ref6", "ref7", "ref8"))
    val filePath = "/a/path/with/some/nested/folders/file"
    val matchId = "1"
    val inputs = Set(TreeNodeInput(filePath, Some(matchId)))
    val result = TreeNodesUtils(
      new FixedUUIDSource,
      referenceGeneratorServiceMock,
      ConfigFactory.load()
    ).generateNodes(inputs, fileTypeIdentifier)
    val expectedSize = 7
    result.size should equal(expectedSize)
    val fileNode = result("a/path/with/some/nested/folders/file")
    fileNode.name should equal("file")
    fileNode.treeNodeType should equal(NodeType.fileTypeIdentifier)
    fileNode.reference should equal(Some("ref2"))
    fileNode.parentPath should equal(Some("a/path/with/some/nested/folders"))
    fileNode.matchId should equal(Some(matchId))
    List(
      ("a", None, "a", "ref7"),
      ("a/path", Some("a"), "path", "ref5"),
      ("a/path/with", Some("a/path"), "with", "ref3"),
      ("a/path/with/some", Some("a/path/with"), "some", "ref4"),
      ("a/path/with/some/nested", Some("a/path/with/some"), "nested", "ref1"),
      ("a/path/with/some/nested/folders", Some("a/path/with/some/nested"), "folders", "ref6")
    ).foreach(pathWithParent => {
      val (path, parent, name, reference) = pathWithParent
      val treeNode = result.get(path)
      treeNode.isDefined should be(true)
      treeNode.get.treeNodeType should equal(NodeType.directoryTypeIdentifier)
      treeNode.get.parentPath should equal(parent)
      treeNode.get.name should equal(name)
      treeNode.get.reference should equal(Some(reference))
      treeNode.get.matchId should equal(None)
    })
    val folderPath = "a/path/with/some/nested/folders"
    val fileTreeNode = result.get(s"$folderPath/file")
    fileTreeNode.isDefined should be(true)
    fileTreeNode.get.treeNodeType should be(NodeType.fileTypeIdentifier)
    fileTreeNode.get.name should equal("file")
    fileTreeNode.get.parentPath.get should be(folderPath)
    fileTreeNode.get.reference should be(Some("ref2"))
  }

  "generateNodes" should "generate the correct nodes for a single file" in {
    when(referenceGeneratorServiceMock.getReferences(any[Int])).thenReturn(List("ref1"))
    val fileName = "file"
    val matchId = "1"
    val inputs = Set(TreeNodeInput(fileName, Some(matchId)))
    val result = TreeNodesUtils(new FixedUUIDSource, referenceGeneratorServiceMock, ConfigFactory.load()).generateNodes(inputs, fileTypeIdentifier)
    result.size should equal(1)
    val treeNode = result.get(fileName)
    treeNode.isDefined should be(true)
    treeNode.get.treeNodeType should be(NodeType.fileTypeIdentifier)
    treeNode.get.parentPath.isEmpty should be(true)
    treeNode.get.name should be(fileName)
    treeNode.get.reference should be(Some("ref1"))
    treeNode.get.matchId.get should be(matchId)
  }

  "generateNodes" should "throw an Exception if call to the reference generator service fails" in {
    when(referenceGeneratorServiceMock.getReferences(any[Int])).thenThrow(new Exception("some exception"))
    val fileName = "file"
    val inputs = Set(TreeNodeInput(fileName))
    val result = intercept[Exception] {
      TreeNodesUtils(
        new FixedUUIDSource,
        referenceGeneratorServiceMock,
        ConfigFactory.load()
      ).generateNodes(inputs, fileTypeIdentifier)
    }
    assert(result.getMessage === "some exception")
  }
}
