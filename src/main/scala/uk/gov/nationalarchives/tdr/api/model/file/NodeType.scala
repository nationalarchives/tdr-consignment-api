package uk.gov.nationalarchives.tdr.api.model.file

import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException

object NodeType {
  val folderTypeIdentifier = "Folder"
  val fileTypeIdentifier = "File"

  implicit class FilePathHelper(path: String) {
    private def deconstructed: Array[String] = path.split("/")
    private def isFile: Boolean = name.contains(".")

    def name: String = deconstructed.last
    def parent: Option[String] =
      if (deconstructed.length == 1) None else Some(deconstructed.dropRight(1).mkString("/"))

    def fileType: String = {
      if (isFile) fileTypeIdentifier else folderTypeIdentifier
    }
  }

  implicit class FileTypeHelper(value: String) {
    def validateType: String = {
      value match {
        case _ if isFileType | isFolderType => value
        case _ => throw InputDataException(s"Invalid file type '$value' for path")
      }
    }

    def isFileType: Boolean = {
      value == fileTypeIdentifier
    }

    def isFolderType: Boolean = {
      value == folderTypeIdentifier
    }
  }
}
