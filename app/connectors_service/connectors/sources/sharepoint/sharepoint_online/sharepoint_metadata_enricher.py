#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
from typing import Any, Dict, List, Optional

from connectors.sources.sharepoint.sharepoint_online.constants import GRAPH_API_URL

# ODC-specific managed properties for SharePoint Graph API calls
# Includes both ODC and ODP (Operations Document Portal) fields
ODC_MANAGED_PROPERTIES = "ActivityID,BusinessUnit,OPDCategory,LOB,Division,DocumentType,FinancialYear,Quarter,Month,Owner,Reviewer,Approver,Status,Priority,Confidentiality,Retention,Compliance,RelatedProjects,Tags,Keywords,Notes,ProjectID,ProjectName,ProjectTitle,ProjectType,ProjectSector,Phase,Region,Country,FocusCountry,GrantType,GrantWindow,GrantRecipient,Theme,ShortName,AllDocuments,Disclosable,Disclosed,NonIFAD,PLF,Sensitive,BorrowerID,CopyValidation,DocumentTypeID,ODCIntegration_CIMission"


class SharePointMetadataEnricher:

    def __init__(self, logger=None, graph_api_client=None):
        self.logger = logger
        self._graph_api_client = graph_api_client

    def _log_debug(self, message: str):
        if self.logger:
            self.logger.debug(message)

    def _log_info(self, message: str):
        if self.logger:
            self.logger.info(message)

    def _log_warning(self, message: str):
        if self.logger:
            self.logger.warning(message)

    def _is_odc_site(self, site):
        """Check if site is an ODC site based on the official ODC site URLs."""
        if not site:
            return False

        web_url = site.get("webUrl", "").lower()

        # Official ODC site URLs
        odc_sites = [
            "aprop",
            "lacop",
            "esaop",
            "nenop",
            "wcaop",
            "epop",
        ]

        # Check if the site URL matches any ODC site (with or without trailing slash)
        for odc_site in odc_sites:
            if f"/sites/{odc_site.rstrip('/')}" in web_url:
                return True

        return False

    def get_odc_managed_properties(self):
        """Get the ODC managed properties string for Graph API calls."""
        return ODC_MANAGED_PROPERTIES

    def should_include_odc_properties(self, site):
        """Check if ODC properties should be included in Graph API calls for this site."""
        return self._is_odc_site(site)

    async def get_drive_list_mapping(self, site_id, site_drives_method, site_lists_method):
        """
        Get mapping between drives and their corresponding SharePoint lists.
        This is needed to fetch custom metadata for drive items.
        """
        drive_list_mapping = {}

        try:
            # Get all drives for the site
            async for drive in site_drives_method(site_id):
                drive_id = drive.get("id")

                # Get all lists for the site
                async for site_list in site_lists_method(site_id):
                    list_id = site_list.get("id")
                    list_name = site_list.get("name") or site_list.get("displayName", "")

                    # Try to match drive with list - document libraries are usually lists
                    # This is a heuristic approach - in reality the mapping can be complex
                    if (
                        "document" in list_name.lower()
                        or "library" in list_name.lower()
                        or list_name.lower() in drive.get("name", "").lower()
                    ):
                        drive_list_mapping[drive_id] = list_id
                        self._log_info(
                            f"Mapped drive '{drive.get('name')}' ({drive_id}) to list '{list_name}' ({list_id})"
                        )
                        break

        except Exception as e:
            self._log_warning(
                f"Error creating drive-list mapping for site {site_id}: {str(e)}"
            )

        return drive_list_mapping

    async def get_drive_item_list_fields(self, drive_id, item_id):
        """
        Get custom metadata fields for a drive item via the listItem/fields endpoint.
        This is the working approach that retrieves SharePoint custom metadata.
        """
        if not self._graph_api_client:
            self._log_warning("No Graph API client available for fetching metadata fields")
            return {}

        try:
            url = f"{GRAPH_API_URL}/drives/{drive_id}/items/{item_id}/listItem/fields"
            response = await self._graph_api_client.fetch(url)
            self._log_info(
                f"Retrieved {len(response)} custom fields for drive item {item_id}"
            )
            return response

        except Exception as e:
            if "404" in str(e) or "NotFound" in str(e):
                self._log_debug(
                    f"No listItem/fields found for drive item {item_id} (404)"
                )
            else:
                self._log_debug(
                    f"Failed to get listItem/fields for item {item_id}: {str(e)}"
                )
            return {}

    async def enrich_drive_item_with_list_metadata(
        self, drive_item, site_id=None, drive_list_mapping=None
    ):
        """
        Enrich a drive item with custom metadata using the working listItem/fields approach.
        """
        try:
            # Get the drive ID and item ID
            item_id = drive_item.get("id")
            drive_id = None

            # Try to get drive ID from parentReference
            parent_ref = drive_item.get("parentReference", {})
            if parent_ref:
                drive_id = parent_ref.get("driveId")

            if not drive_id or not item_id:
                self._log_debug(
                    f"Missing drive_id or item_id for drive item {item_id}"
                )
                return drive_item

            # Use the working approach: /drives/{drive_id}/items/{item_id}/listItem/fields
            custom_fields = await self.get_drive_item_list_fields(drive_id, item_id)

            if custom_fields:
                enriched_item = drive_item.copy()
                # Add the SharePoint list fields to the drive item
                enriched_item["fields"] = custom_fields
                self._log_info(
                    f"Enriched drive item {item_id} with {len(custom_fields)} SharePoint fields from listItem/fields"
                )
                return enriched_item
            else:
                self._log_debug(
                    f"No SharePoint listItem/fields found for drive item {item_id}"
                )

        except Exception as e:
            self._log_warning(
                f"Error enriching drive item {drive_item.get('id')} with listItem/fields metadata: {str(e)}"
            )

        return drive_item

    def _extract_metadata_from_sharepoint_fields(
        self,
        document: Dict[str, Any],
        site: Optional[Dict[str, Any]] = None,
        site_drive: Optional[Dict[str, Any]] = None,
        site_list: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        self._log_info(
            f"Extracting metadata for document {document.get('_id', 'unknown')}"
        )

        metadata = {}
        fields = document.get("fields", {})

        # Helper function to extract value from complex SharePoint field structures
        def extract_field_value(field_data):
            if isinstance(field_data, dict):
                # Handle managed metadata fields with Label/TermGuid structure
                if "Label" in field_data:
                    return field_data["Label"]
                # Handle lookup fields or other object structures
                elif "DisplayName" in field_data:
                    return field_data["DisplayName"]
                elif "Value" in field_data:
                    return field_data["Value"]
            elif isinstance(field_data, list):
                # Handle arrays of managed metadata or lookup fields
                if field_data and isinstance(field_data[0], dict):
                    return [extract_field_value(item) for item in field_data]
                else:
                    return field_data
            else:
                # Handle simple string/number/boolean values
                return field_data
            return None

        # Determine category from site URL using ODC site detection
        if site and site.get("webUrl"):
            if self._is_odc_site(site):
                metadata["Category"] = "ODC"
            else:
                site_url = site["webUrl"].lower()
                if "xdesk" in site_url:
                    metadata["Category"] = "Xdesk"
                else:
                    metadata["Category"] = "General"
        else:
            metadata["Category"] = None

        # Core business metadata
        metadata["Division"] = fields.get("BusinessUnit")
        metadata["Department"] = fields.get("BusinessUnit")

        # Document type - handle both simple and complex field structures
        doc_type = extract_field_value(fields.get("DocumentType"))
        metadata["Content-Type"] = doc_type or self._determine_content_type(document)

        # Project information - handle complex field structures
        project_id = extract_field_value(fields.get("ProjectID"))
        if project_id:
            metadata["ProjectID"] = project_id
        else:
            # Fallback to hidden field
            metadata["ProjectID"] = fields.get("ProjectID_Hidden")

        metadata["ProjectType"] = fields.get("ProjectType")
        metadata["ProjectName"] = fields.get("ProjectName")
        metadata["ProjectTitle"] = fields.get("ProjectTitle")
        metadata["ProjectSector"] = fields.get("ProjectSector")

        # Handle ShortName and AllDocuments (project short names)
        short_name = extract_field_value(fields.get("ShortName"))
        all_documents = extract_field_value(fields.get("AllDocuments"))
        metadata["ShortName"] = short_name
        metadata["AllDocuments"] = all_documents

        # Geographic and temporal metadata
        metadata["Region"] = fields.get("Region")
        metadata["Country"] = fields.get("Country")
        metadata["CountryID"] = fields.get("CountryID")
        metadata["FocusCountryIDs"] = fields.get("FocusCountryIDs")

        # Handle complex FocusCountry field
        focus_country = extract_field_value(fields.get("FocusCountry"))
        metadata["FocusCountry"] = focus_country

        metadata["Year"] = fields.get("Year")
        metadata["Phase"] = fields.get("Phase")
        metadata["PhaseID"] = fields.get("PhaseID")

        # Grant and financing information
        metadata["GrantType"] = fields.get("GrantType")
        metadata["GrantWindow"] = fields.get("GrantWindow")
        grant_recipient = extract_field_value(fields.get("GrantRecipient"))
        metadata["GrantRecipient"] = grant_recipient
        metadata["BorrowerID"] = fields.get("BorrowerID")

        # Themes and topics
        themes = extract_field_value(fields.get("Theme"))
        metadata["Theme"] = themes

        # Document classification and status
        metadata["Disclosable"] = fields.get("Disclosable")
        metadata["Disclosed"] = fields.get("Disclosed")
        metadata["NonIFAD"] = fields.get("NonIfad")
        metadata["PLF"] = fields.get("PLF")
        metadata["Sensitive"] = fields.get("Sensitive")
        metadata["IsInDocSet"] = fields.get("IsInDocSet")
        metadata["OPDIsLink"] = fields.get("OPDIsLink")

        # Validation and compliance
        metadata["CopyValidation"] = fields.get("CopyValidation")
        metadata["SentToRMS"] = fields.get("SentToRMS")

        # System and integration fields
        metadata["ODCIntegration_CIMission"] = fields.get("ODCIntegration_CIMission")
        metadata["SystemSource"] = fields.get("ODCIntegration_SystemSource")
        metadata["DocumentTypeID"] = fields.get("DocumentTypeID")

        # Project reference field
        metadata["Project"] = fields.get("Project")

        # Additional common SharePoint fields
        metadata["Title"] = fields.get("Title")
        metadata["Author"] = fields.get("Author")
        metadata["Editor"] = fields.get("Editor")
        metadata["Created"] = fields.get("Created")
        metadata["Modified"] = fields.get("Modified")
        metadata["FileLeafRef"] = fields.get("FileLeafRef")
        metadata["FileDirRef"] = fields.get("FileDirRef")
        metadata["ContentType"] = fields.get("ContentType")
        metadata["FileType"] = fields.get("File_x0020_Type")

        # Document icon and size
        metadata["DocIcon"] = fields.get("DocIcon")
        metadata["FileSizeDisplay"] = fields.get("FileSizeDisplay")

        # Document ID and linking
        dlc_doc_id = fields.get("_dlc_DocIdUrl")
        if isinstance(dlc_doc_id, dict) and "Description" in dlc_doc_id:
            metadata["DocumentID"] = dlc_doc_id["Description"]
            metadata["DocumentIDUrl"] = dlc_doc_id.get("Url")

        # Version information
        metadata["UIVersionString"] = fields.get("_UIVersionString")

        # Activity information (for ODC compatibility)
        metadata["ActivityID"] = fields.get("ActivityID")
        metadata["ActivityName"] = fields.get("ActivityName")

        # Legacy status field handling
        metadata["Status"] = fields.get("OPDStatus") or fields.get("Status")

        # Check for any OPD/ODC category fields
        odc_category = fields.get("OPDCategory") or fields.get("ODCCategory")
        if odc_category:
            metadata["Category"] = odc_category

        self._log_info(
            f"Extracted metadata with {len([v for v in metadata.values() if v is not None])} non-null fields"
        )
        return metadata

    def _determine_content_type(self, document: Dict[str, Any]) -> str:
        object_type = document.get("object_type", "")

        if object_type == "drive_item":
            name = document.get("name", "")
            if "folder" in document:
                return "Folder"
            elif name:
                ext = os.path.splitext(name)[-1].lower()
                if ext in [".ppt", ".pptx"]:
                    return "Presentation"
                elif ext in [".doc", ".docx", ".pdf"]:
                    return "Document"
                elif ext in [".xls", ".xlsx"]:
                    return "Spreadsheet"
                elif ext in [".mp4", ".avi", ".mov"]:
                    return "Video"
                elif ext in [".jpg", ".jpeg", ".png", ".gif"]:
                    return "Image"
                else:
                    return "Document"
            else:
                return "Document"
        elif object_type == "site_page":
            return "Web Page"
        elif object_type == "list_item":
            return "List Item"
        elif object_type == "list_item_attachment":
            return "Attachment"
        else:
            return "Document"

    def _site_path_from_web_url(self, web_url: str) -> str:
        url_parts = web_url.split("/sites/")
        site_path_parts = url_parts[1:]
        return "/sites/".join(site_path_parts)

    def build_metadata_array(
        self,
        document: Dict[str, Any],
        site: Optional[Dict[str, Any]] = None,
        site_drive: Optional[Dict[str, Any]] = None,
        site_list: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        self._log_info(
            f"Building metadata array for document {document.get('_id', 'unknown')}"
        )
        metadata_pairs = []

        try:
            # Extract SharePoint-specific metadata
            sharepoint_metadata = self._extract_metadata_from_sharepoint_fields(
                document, site, site_drive, site_list
            )

            # Standard metadata that should always be present
            metadata_pairs.append(
                {"key": "Category", "value": sharepoint_metadata.get("Category")}
            )

            # Site Name
            site_name = None
            if site:
                site_name = (
                    site.get("displayName") or site.get("name") or site.get("title")
                )
            metadata_pairs.append({"key": "Site Name", "value": site_name})

            # Document Library / Drive Name
            library_name = None
            if site_drive:
                library_name = site_drive.get("name") or site_drive.get("displayName")
            elif site_list:
                library_name = site_list.get("name") or site_list.get("displayName")
            metadata_pairs.append({"key": "Document Library", "value": library_name})

            # Division and Department (required fields)
            metadata_pairs.append(
                {"key": "Division", "value": sharepoint_metadata.get("Division")}
            )
            metadata_pairs.append(
                {"key": "Department", "value": sharepoint_metadata.get("Department")}
            )

            # Content-Type (required field)
            metadata_pairs.append(
                {
                    "key": "Content-Type",
                    "value": sharepoint_metadata.get("Content-Type"),
                }
            )

            # File Type/Extension
            file_extension = None
            file_name = (
                document.get("name")
                or document.get("_original_filename")
                or document.get("FileName", "")
            )
            if file_name and "." in file_name:
                file_extension = os.path.splitext(file_name)[-1].lower()
            metadata_pairs.append({"key": "File Type", "value": file_extension})

            # File Path/Location
            file_path = None
            if document.get("webUrl"):
                file_path = document["webUrl"]
            elif document.get("parentReference", {}).get("path"):
                file_path = document["parentReference"]["path"]
            elif site and site.get("webUrl"):
                site_path = self._site_path_from_web_url(site["webUrl"])
                if file_name:
                    file_path = f"{site_path}/{file_name}"
                else:
                    file_path = site_path
            metadata_pairs.append({"key": "File Path", "value": file_path})

            # Add all SharePoint metadata fields for all documents
            sharepoint_fields = [
                # Project and Activity Information
                "ActivityID",
                "ActivityName",
                "ProjectID",
                "ProjectType",
                "ProjectName",
                "ProjectTitle",
                "ProjectSector",
                "Project",
                "ShortName",
                "AllDocuments",
                # Geographic and Temporal
                "Region",
                "Country",
                "CountryID",
                "FocusCountry",
                "FocusCountryIDs",
                "Year",
                "Phase",
                "PhaseID",
                # Grant and Financing
                "GrantType",
                "GrantWindow",
                "GrantRecipient",
                "BorrowerID",
                # Themes and Classification
                "Theme",
                "Status",
                "DocumentTypeID",
                # Flags and Status
                "Disclosable",
                "Disclosed",
                "NonIFAD",
                "PLF",
                "Sensitive",
                "IsInDocSet",
                "OPDIsLink",
                # Validation and Compliance
                "CopyValidation",
                "SentToRMS",
                # System and Integration
                "SystemSource",
                "ODCIntegration_CIMission",
                # Standard SharePoint Fields
                "Title",
                "Author",
                "Editor",
                "Created",
                "Modified",
                "FileLeafRef",
                "FileDirRef",
                "ContentType",
                "FileType",
                "DocIcon",
                "FileSizeDisplay",
                "DocumentID",
                "DocumentIDUrl",
                "UIVersionString",
            ]

            for field in sharepoint_fields:
                if (
                    field in sharepoint_metadata
                    and sharepoint_metadata[field] is not None
                ):
                    metadata_pairs.append(
                        {"key": field, "value": sharepoint_metadata[field]}
                    )

            # Additional technical metadata
            metadata_pairs.append(
                {"key": "Object Type", "value": document.get("object_type")}
            )
            metadata_pairs.append(
                {"key": "Document ID", "value": document.get("_id")}
            )
            metadata_pairs.append(
                {
                    "key": "Last Modified",
                    "value": document.get("_timestamp")
                    or document.get("lastModifiedDateTime"),
                }
            )

            # Size information for files
            if document.get("size"):
                metadata_pairs.append(
                    {"key": "File Size", "value": document.get("size")}
                )

            # Creator information
            created_by = None
            if document.get("createdBy", {}).get("user", {}).get("displayName"):
                created_by = document["createdBy"]["user"]["displayName"]
            elif document.get("createdBy", {}).get("user", {}).get("email"):
                created_by = document["createdBy"]["user"]["email"]
            metadata_pairs.append({"key": "Created By", "value": created_by})

            # Modified by information
            modified_by = None
            if document.get("lastModifiedBy", {}).get("user", {}).get("displayName"):
                modified_by = document["lastModifiedBy"]["user"]["displayName"]
            elif document.get("lastModifiedBy", {}).get("user", {}).get("email"):
                modified_by = document["lastModifiedBy"]["user"]["email"]
            metadata_pairs.append({"key": "Modified By", "value": modified_by})

            self._log_info(f"Built {len(metadata_pairs)} metadata pairs")

        except Exception as e:
            self._log_warning(
                f"Error building metadata array for document {document.get('_id')}: {str(e)}"
            )
            # Return minimal metadata on error
            metadata_pairs = [
                {"key": "Category", "value": None},
                {"key": "Site Name", "value": None},
                {"key": "Document Library", "value": None},
                {"key": "Division", "value": None},
                {"key": "Department", "value": None},
                {"key": "Content-Type", "value": None},
                {"key": "File Type", "value": None},
                {"key": "File Path", "value": None},
            ]

        return metadata_pairs

    def enrich_document_with_metadata(
        self,
        document: Dict[str, Any],
        site: Optional[Dict[str, Any]] = None,
        site_drive: Optional[Dict[str, Any]] = None,
        site_list: Optional[Dict[str, Any]] = None,
        enrich_metadata_enabled: bool = True,
    ) -> Dict[str, Any]:
        if not enrich_metadata_enabled:
            return document

        self._log_info(
            f"Enriching document {document.get('_id', 'unknown')} with metadata"
        )
        enriched_document = document.copy()

        try:
            metadata_array = self.build_metadata_array(
                enriched_document, site, site_drive, site_list
            )
            enriched_document["metadata"] = metadata_array

            self._log_info(
                f"Successfully enriched document with {len(metadata_array)} metadata pairs"
            )

        except Exception as e:
            self._log_warning(
                f"Failed to enrich document {enriched_document.get('_id')} with metadata: {str(e)}"
            )
            # Ensure at least an empty metadata array with required fields
            enriched_document["metadata"] = [
                {"key": "Category", "value": None},
                {"key": "Site Name", "value": None},
                {"key": "Document Library", "value": None},
                {"key": "Division", "value": None},
                {"key": "Department", "value": None},
                {"key": "Content-Type", "value": None},
                {"key": "File Type", "value": None},
                {"key": "File Path", "value": None},
            ]

        return enriched_document
