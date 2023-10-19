package uk.gov.nationalarchives.tdr.api.utils

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.fileTypeIdentifier
import uk.gov.nationalarchives.tdr.api.service.ReferenceGeneratorService

class TreeNodeUtilsSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  val referenceGeneratorServiceMock: ReferenceGeneratorService = mock[ReferenceGeneratorService]

  "generateNodes" should "generate the correct nodes for a nested path" in {
    when(referenceGeneratorServiceMock.getReferences(any[Int])).thenReturn(List("ref1", "ref2", "ref3", "ref4", "ref5", "ref6", "ref7", "ref8"))
    val result = TreeNodesUtils(
      new FixedUUIDSource,
      referenceGeneratorServiceMock,
      ConfigFactory.load().withValue("featureAccessBlock.assignFileReferences", ConfigValueFactory.fromAnyRef("false"))
    ).generateNodes(Set("/a/path/with/some/nested/folders/file"), fileTypeIdentifier)
    val expectedSize = 7
    result.size should equal(expectedSize)
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
    val fileName = "file"
    val result = TreeNodesUtils(new FixedUUIDSource, referenceGeneratorServiceMock, ConfigFactory.load()).generateNodes(Set(fileName), fileTypeIdentifier)
    result.size should equal(1)
    val treeNode = result.get(fileName)
    treeNode.isDefined should be(true)
    treeNode.get.treeNodeType should be(NodeType.fileTypeIdentifier)
    treeNode.get.parentPath.isEmpty should be(true)
    treeNode.get.name should be(fileName)
  }

  "generateNodes" should "throw an Exception if call to the reference generator service fails" in {
    when(referenceGeneratorServiceMock.getReferences(any[Int])).thenThrow(new Exception("some exception"))
    val fileName = "file"
    val result = intercept[Exception] {
      TreeNodesUtils(
        new FixedUUIDSource,
        referenceGeneratorServiceMock,
        ConfigFactory.load().withValue("featureAccessBlock.assignFileReferences", ConfigValueFactory.fromAnyRef("false"))
      ).generateNodes(Set(fileName), fileTypeIdentifier)
    }
    assert(result.getMessage === "some exception")
  }
}
