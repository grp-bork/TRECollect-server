"""
XML parsers for TRECollect: form XMLs and site_metadata.xml.
"""

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any


class SiteXMLParser:
    """
    Simple parser for site_metadata.xml (root <siteMetadata>).
    Extracts and holds only site_name and submitted_at (from child elements).
    """

    def __init__(self) -> None:
        self.site_name: str | None = None
        self.submitted_at: str | None = None

    def parse_file(self, path: str | Path) -> "SiteXMLParser":
        """
        Parse a site_metadata.xml file and populate this instance's attributes.

        Args:
            path: Path to the XML file (string or Path).

        Returns:
            self (for chaining).

        Raises:
            FileNotFoundError: If the file does not exist.
            ET.ParseError: If the XML is malformed.
            ValueError: If root element is not "siteMetadata".
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"XML file not found: {path}")
        tree = ET.parse(path)
        root = tree.getroot()
        if root.tag != "siteMetadata":
            raise ValueError(f"Expected root element 'siteMetadata', got '{root.tag}'")
        self._populate(root)
        return self

    def _populate(self, root: ET.Element) -> None:
        """Extract siteName and submittedAt text from child elements."""
        site_name_el = root.find("siteName")
        submitted_el = root.find("submittedAt")
        self.site_name = site_name_el.text.strip() if site_name_el is not None and site_name_el.text else None
        self.submitted_at = submitted_el.text.strip() if submitted_el is not None and submitted_el.text else None


class FormXMLParser:
    """
    Parses a form XML file (root <form>) and holds the result as attributes.

    Attributes (from the <form> element):
        form_id: formId attribute
        site_id: siteName attribute
        created_at: createdAt attribute
        submitted_at: submittedAt attribute
        logsheet_version: logsheetVersion attribute
        fields: nested dict built recursively from the <fields> subtree
    """

    def __init__(self, attr_prefix: str = "@", text_key: str = "#text"):
        self.attr_prefix = attr_prefix
        self.text_key = text_key

        self.form_id: str | None = None
        self.site_id: str | None = None
        self.created_at: str | None = None
        self.submitted_at: str | None = None
        self.logsheet_version: str | None = None
        self.fields: dict[str, Any] = {}

    def parse_file(self, path: str | Path) -> "FormXMLParser":
        """
        Parse an XML file and populate this instance's attributes.

        Args:
            path: Path to the form XML file (string or Path).

        Returns:
            self (for chaining).

        Raises:
            FileNotFoundError: If the file does not exist.
            ET.ParseError: If the XML is malformed.
            ValueError: If root element is not "form".
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"XML file not found: {path}")
        tree = ET.parse(path)
        root = tree.getroot()
        if root.tag != "form":
            raise ValueError(f"Expected root element 'form', got '{root.tag}'")
        self._populate_from_form(root)
        return self

    def _populate_from_form(self, form_el: ET.Element) -> None:
        """Extract form attributes and fields from the <form> element."""
        attrs = form_el.attrib
        self.form_id = attrs.get("formId")
        self.site_id = attrs.get("siteName")
        self.created_at = attrs.get("createdAt")
        self.submitted_at = attrs.get("submittedAt")
        self.logsheet_version = attrs.get("logsheetVersion")

        fields_el = form_el.find("fields")
        if fields_el is not None:
            self.fields = self._element_to_dict(fields_el)
        else:
            self.fields = {}

    def _element_to_dict(self, element: ET.Element) -> dict[str, Any]:
        """Convert an Element to a nested dictionary (attributes, text, children)."""
        result: dict[str, Any] = {}

        for key, value in element.attrib.items():
            result[f"{self.attr_prefix}{key}"] = value

        if element.text and element.text.strip():
            result[self.text_key] = element.text.strip()

        children_by_tag: dict[str, list[dict[str, Any]]] = {}
        for child in element:
            child_dict = self._element_to_dict(child)
            children_by_tag.setdefault(child.tag, []).append(child_dict)

        for tag, dicts in children_by_tag.items():
            if len(dicts) == 1:
                result[tag] = dicts[0]
            else:
                result[tag] = dicts

        if element.tail and element.tail.strip():
            result[f"{self.attr_prefix}tail"] = element.tail.strip()

        return result
