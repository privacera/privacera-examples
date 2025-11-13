#!/usr/bin/env python3
"""
Script to download tags from Ranger Admin API and build resource paths based on hierarchy.

This script:
1. Takes admin URL and service name as inputs
2. Fetches the service definition to get resource hierarchy
3. Calls the /tags/download/{serviceName} API endpoint
4. Extracts RangerServiceResource objects from ServiceTags response
5. Builds resource paths for each tagged resource based on hierarchy order
"""

import argparse
import json
import re
import sys
from typing import Dict, List, Any, Optional
import requests


class ResourceNode:
    """Represents a node in the resource hierarchy tree."""
    
    def __init__(self, resource_def: Dict[str, Any]):
        self.name = resource_def.get('name', '')
        self.level = resource_def.get('level', 1)
        self.parent_name = resource_def.get('parent')
        self.type = resource_def.get('type', '')
        self.label = resource_def.get('label', '')
        self.mandatory = resource_def.get('mandatory', False)
        self.children: List['ResourceNode'] = []
        self.resource_def = resource_def

    def __repr__(self):
        return f"ResourceNode(name={self.name}, level={self.level}, parent={self.parent_name})"


class ResourceHierarchyBuilder:
    """Builds resource hierarchies from RangerResourceDef objects."""
    
    def __init__(self, resources: List[Dict[str, Any]]):
        self.resources = resources
        self.name_to_node: Dict[str, ResourceNode] = {}
        self.root_nodes: List[ResourceNode] = []
        
    def build_hierarchy(self) -> List[ResourceNode]:
        """
        Build the resource hierarchy tree.
        
        Returns:
            List of root ResourceNode objects representing all hierarchies
        """
        # Create nodes for all resources
        for resource in self.resources:
            node = ResourceNode(resource)
            self.name_to_node[node.name] = node
        
        # Build parent-child relationships
        for node in self.name_to_node.values():
            if node.parent_name and node.parent_name in self.name_to_node:
                parent_node = self.name_to_node[node.parent_name]
                parent_node.children.append(node)
            else:
                # Node without parent or parent not found - treat as root
                self.root_nodes.append(node)
        
        # Sort root nodes by level, then by name
        self.root_nodes.sort(key=lambda n: (n.level, n.name))
        
        # Sort children for each node
        self._sort_children(self.root_nodes)
        
        return self.root_nodes
    
    def get_ordered_resource_names(self) -> List[str]:
        """
        Get resource names ordered by hierarchy level.
        
        Returns:
            List of resource names ordered from root to leaf (by level, then by name)
        """
        # Collect all nodes and sort by level, then by name
        all_nodes = list(self.name_to_node.values())
        all_nodes.sort(key=lambda n: (n.level, n.name))
        
        # Return just the names in order
        return [node.name for node in all_nodes]
    
    def _sort_children(self, nodes: List[ResourceNode]):
        """Recursively sort children of all nodes."""
        for node in nodes:
            node.children.sort(key=lambda n: (n.level, n.name))
            self._sort_children(node.children)


def fetch_api(
    admin_url: str, 
    service_name: str, 
    api_path: str,
    session: Optional[requests.Session] = None
) -> Dict[str, Any]:
    """
    Fetch data from the Ranger Admin API.
    
    Args:
        admin_url: Base URL of the Ranger Admin server
        service_name: Name of the service
        api_path: API path (the service name will be appended)
        session: Optional requests Session object for cookie handling
    
    Returns:
        JSON response as a dictionary
    
    Raises:
        requests.RequestException: If the API call fails
        ValueError: If the response is invalid
    """
    # Ensure admin_url doesn't end with a slash
    admin_url = admin_url.rstrip('/')
    
    # Ensure api_path starts with a slash
    if not api_path.startswith('/'):
        api_path = '/' + api_path
    
    # Construct the API endpoint
    endpoint = f"{admin_url}{api_path}/{service_name}"
    
    # Prepare request
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    try:
        # Use session if provided, otherwise use requests directly
        if session is not None:
            response = session.get(endpoint, headers=headers, timeout=30, allow_redirects=False)
        else:
            response = requests.get(endpoint, headers=headers, timeout=30, allow_redirects=False)
        
        # Check if response is successful
        if response.status_code == 304:
            # 304 Not Modified - no tags updated
            print(f"  Response: 304 Not Modified (no updates)")
            return {}
        
        # Handle redirects
        if response.status_code in (301, 302, 303, 307, 308):
            location = response.headers.get('Location', '')
            raise ValueError(
                f"Server redirected to: {location}\n"
                f"This usually means authentication is required."
            )
        
        response.raise_for_status()
        
        # Get response text first for debugging
        response_text = response.text.strip()
        
        # Check if response is empty
        if not response_text:
            print(f"  Warning: Empty response from server")
            return {}
        
        # Check if response is HTML (likely an error/login page)
        if response_text.startswith('<') or 'html' in response.headers.get('Content-Type', '').lower():
            # Show a helpful error message
            if 'login' in response_text.lower() or 'authentication' in response_text.lower():
                raise ValueError(
                    f"Server returned HTML (likely login page) instead of JSON.\n"
                    f"This usually means authentication is required.\n"
                    f"Response preview: {response_text[:200]}"
                )
            else:
                raise ValueError(
                    f"Server returned HTML instead of JSON.\n"
                    f"Response preview: {response_text[:300]}"
                )
        
        try:
            return response.json()
        except json.JSONDecodeError as e:
            # Show first 500 chars of response for debugging
            preview = response_text[:500] if len(response_text) > 500 else response_text
            raise ValueError(
                f"Invalid JSON response: {e}\n"
                f"Response status: {response.status_code}\n"
                f"Response content (first 500 chars): {preview}\n"
                f"Full response length: {len(response_text)} bytes"
            )
            
    except requests.exceptions.RequestException as e:
        response_status = 'N/A'
        response_text = ''
        if hasattr(e, 'response') and e.response:
            response_status = e.response.status_code
            try:
                response_text = e.response.text[:200]  # First 200 chars
            except:
                pass
        
        error_msg = (
            f"Failed to fetch data: {e}\n"
            f"URL: {endpoint}\n"
            f"Status Code: {response_status}"
        )
        if response_text:
            error_msg += f"\nResponse: {response_text}"
        
        raise requests.exceptions.RequestException(error_msg)


def fetch_service_tags(
    admin_url: str,
    service_name: str,
    session: Optional[requests.Session] = None
) -> Dict[str, Any]:
    """Fetch service tags from the service/tags/download endpoint."""
    return fetch_api(admin_url, service_name, 'service/tags/download', session)


def fetch_service_definition(
    admin_url: str, 
    service_name: str,
    session: Optional[requests.Session] = None
) -> Dict[str, Any]:
    """Fetch service definition from the policies/download endpoint."""
    return fetch_api(admin_url, service_name, '/service/plugins/policies/download', session)


def extract_resources(service_policies: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract RangerResourceDef objects from ServicePolicies response.
    
    Args:
        service_policies: The ServicePolicies JSON response
    
    Returns:
        List of resource definitions
    """
    if 'serviceDef' not in service_policies:
        raise ValueError("Response does not contain 'serviceDef' field")
    
    service_def = service_policies['serviceDef']
    
    if 'resources' not in service_def:
        raise ValueError("serviceDef does not contain 'resources' field")
    
    resources = service_def.get('resources', [])
    
    if not resources:
        print("Warning: No resources found in service definition")
        return []
    
    return resources


def extract_service_resources(service_tags: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract RangerServiceResource objects from ServiceTags response.
    
    Args:
        service_tags: The ServiceTags JSON response
    
    Returns:
        List of service resource objects
    """
    # Handle empty response
    if not service_tags:
        print("Warning: Empty response from tags/download endpoint")
        return []
    
    if 'serviceResources' not in service_tags:
        # This might be a valid response if no tags exist
        print("Warning: Response does not contain 'serviceResources' field")
        return []
    
    service_resources = service_tags.get('serviceResources', [])
    
    if not service_resources:
        print("Info: No tagged resources found for this service")
        return []
    
    return service_resources


def build_resource_path(
    service_resource: Dict[str, Any],
    resource_hierarchy: List[str]
) -> Optional[str]:
    """
    Build a resource path from service resource based on hierarchy order.
    
    Args:
        service_resource: The RangerServiceResource object
        resource_hierarchy: Ordered list of resource names (from root to leaf)
    
    Returns:
        Resource path string or None if no valid resources found
    """
    resource_elements = service_resource.get('resourceElements', {})
    
    if not resource_elements:
        return None
    
    path_parts = []
    
    # Build path according to hierarchy order
    for resource_name in resource_hierarchy:
        if resource_name in resource_elements:
            resource_element = resource_elements[resource_name]
            values = resource_element.get('values', [])
            
            # Take the first value if available
            if values:
                path_parts.append(values[0])
    
    if not path_parts:
        return None
    
    return '/'.join(path_parts)


def extract_tag_definitions(service_tags: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract tag definitions from ServiceTags response.
    
    Args:
        service_tags: The ServiceTags JSON response
    
    Returns:
        List of tag definition objects
    """
    # Handle empty response
    if not service_tags:
        print("Warning: Empty response from tags/download endpoint")
        return []
    
    if 'tagDefinitions' not in service_tags:
        print("Warning: Response does not contain 'tagDefinitions' field")
        return []
    
    tag_definitions = service_tags.get('tagDefinitions', {})
    
    # tagDefinitions is a dictionary where keys are tag IDs and values are tag definition objects
    if not tag_definitions:
        print("Info: No tag definitions found")
        return []
    
    # Convert dictionary to list of tag definitions
    tag_def_list = []
    for tag_id, tag_def in tag_definitions.items():
        # Add the tag_id to the definition for reference
        tag_def_with_id = dict(tag_def)
        tag_def_with_id['_tag_id'] = tag_id
        tag_def_list.append(tag_def_with_id)
    
    return tag_def_list


def normalize_attribute_type(attr_type: str) -> str:
    """
    Normalize attribute type to valid MDS types.
    
    Valid MDS attribute types are: String, Boolean, Integer, Number
    If the type is not one of these, it defaults to "String".
    
    Args:
        attr_type: The attribute type from Ranger
    
    Returns:
        Normalized attribute type (String, Boolean, Integer, or Number)
    """
    if not attr_type:
        return "String"
    
    # Normalize to title case for comparison
    normalized = attr_type.strip()
    
    # Valid MDS types (case-insensitive)
    valid_types = {
        "string": "String",
        "boolean": "Boolean",
        "integer": "Integer",
        "number": "Number"
    }
    
    # Check if it's a valid type (case-insensitive)
    lower_type = normalized.lower()
    if lower_type in valid_types:
        return valid_types[lower_type]
    
    # Default to String if not valid
    return "String"


def convert_tag_definition_to_mds_format(tag_def: Dict[str, Any], tag_type_override: Optional[str] = None) -> Dict[str, Any]:
    """
    Convert a Ranger tag definition to MDS TagRequest format.
    
    Args:
        tag_def: Ranger tag definition object
        tag_type_override: Optional tag type to override the default (applied to all tags)
    
    Returns:
        Dictionary in MDS TagRequest format
        
    Examples:
        Input (Ranger with attributeDefinitions dict):
        {
            "id": 1,
            "name": "PII",
            "source": "Internal",
            "attributeDefinitions": {"sensitivity_level": {"type": "string"}}
        }
        
        Input (Ranger with attributeDefs array):
        {
            "id": 4,
            "name": "PII",
            "source": "Internal",
            "attributeDefs": [{"name": "sensitivity", "type": "string"}]
        }
        
        Output (MDS):
        {
            "tag": {
                "name": "PII",
                "type": "Internal",
                "attributeDefinitions": {
                    "sensitivity": {"type": "string"}
                },
                "source": "API"
            }
        }
    """
    # Extract tag name and convert to uppercase
    tag_name = tag_def.get('name', '')
    if not tag_name:
        raise ValueError("Tag definition must have a 'name' field")
    tag_name = tag_name.upper()
    
    # Use override if provided, otherwise extract from Ranger's 'source' field (or 'type' if available)
    # In Ranger, the 'source' field often contains the tag type
    # Default to 'Internal' if nothing is found
    if tag_type_override:
        tag_type = tag_type_override
    else:
        tag_type = tag_def.get('type') or tag_def.get('source') or 'Internal'
    
    # Retrieve source from tag definition, fallback to 'API' if not present
    source = tag_def.get('source', 'API')
    
    # Extract attribute definitions if available
    # Ranger may have attributeDefinitions (dict) or attributeDefs (array)
    attribute_definitions = {}
    
    # First, check if attributeDefinitions already exists as a dictionary
    if 'attributeDefinitions' in tag_def and tag_def.get('attributeDefinitions'):
        attribute_defs_raw = tag_def.get('attributeDefinitions')
        # Ensure it's a dictionary
        if isinstance(attribute_defs_raw, dict):
            # Process each attribute definition and normalize types
            for attr_name, attr_def in attribute_defs_raw.items():
                if isinstance(attr_def, dict):
                    attr_def_dict = dict(attr_def)  # Create a copy
                    # Normalize the type if present
                    if 'type' in attr_def_dict and attr_def_dict['type']:
                        attr_def_dict['type'] = normalize_attribute_type(str(attr_def_dict['type']))
                    attribute_definitions[attr_name] = attr_def_dict
                else:
                    # If it's not a dict, create a basic structure
                    attribute_definitions[attr_name] = attr_def
    
    # If attributeDefinitions is not present, check for attributeDefs (array format)
    if not attribute_definitions and 'attributeDefs' in tag_def:
        attribute_defs_array = tag_def.get('attributeDefs', [])
        if isinstance(attribute_defs_array, list):
            # Convert array format to dictionary format
            # [{"name": "sensitivity", "type": "abc"}] -> {"sensitivity": {"type": "String"}}
            for attr_def in attribute_defs_array:
                if isinstance(attr_def, dict):
                    attr_name = attr_def.get('name')
                    if attr_name:
                        # Create the attribute definition dict with type and any other fields
                        attr_def_dict = {}
                        if 'type' in attr_def:
                            # Normalize the attribute type
                            attr_def_dict['type'] = normalize_attribute_type(str(attr_def.get('type')))
                        # Add other fields if present (required, description, etc.)
                        if 'required' in attr_def:
                            attr_def_dict['required'] = attr_def.get('required')
                        if 'description' in attr_def:
                            attr_def_dict['description'] = attr_def.get('description')
                        if 'options' in attr_def:
                            attr_def_dict['options'] = attr_def.get('options')
                        # Add the attribute definition to the dictionary
                        attribute_definitions[attr_name] = attr_def_dict
    
    # Build the MDS format
    mds_tag = {
        "name": tag_name,
        "type": tag_type,
        "source": source
    }
    
    # Add attributeDefinitions if present (only if not empty)
    if attribute_definitions:
        mds_tag["attributeDefinitions"] = attribute_definitions
    
    return {
        "tag": mds_tag
    }


def send_tag_to_mds(
    mds_url: str,
    tag_request: Dict[str, Any],
    session: Optional[requests.Session] = None
) -> Dict[str, Any]:
    """
    Send a tag creation request to MDS API.
    
    Args:
        mds_url: Base URL of the MDS server (e.g., http://localhost:6080/omni-metadata)
        tag_request: Tag request in MDS format
        session: Optional requests Session object for cookie handling
    
    Returns:
        JSON response as a dictionary
    
    Raises:
        requests.RequestException: If the API call fails
        ValueError: If the response is invalid
    """
    # Ensure mds_url doesn't end with a slash
    mds_url = mds_url.rstrip('/')
    
    # Construct the API endpoint
    endpoint = f"{mds_url}/api/v1/metadata/tags"
    
    # Prepare request
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    try:
        # Use session if provided, otherwise use requests directly
        if session is not None:
            response = session.post(
                endpoint, 
                headers=headers, 
                json=tag_request,
                timeout=30,
                allow_redirects=False
            )
        else:
            response = requests.post(
                endpoint, 
                headers=headers, 
                json=tag_request,
                timeout=30,
                allow_redirects=False
            )
        
        # Get response text and try to parse JSON (store for reuse)
        response_text = response.text.strip()
        response_json = None
        try:
            response_json = response.json()
        except json.JSONDecodeError:
            response_json = None
        
        # Handle redirects
        if response.status_code in (301, 302, 303, 307, 308):
            location = response.headers.get('Location', '')
            raise ValueError(
                f"Server redirected to: {location}\n"
                f"This usually means authentication is required."
            )
        
        # Check for tag conflict error (tag already exists)
        # This can happen even with 200/201 status codes in some cases
        if response_json is not None:
            status = response_json.get('status', '')
            error_code = response_json.get('errorCode', '')
            message = response_json.get('message', '')
            
            # Check if it's a conflict error (tag already exists)
            if status == 'FAILED' and '1301:CONFLICT' in error_code and 'already exists' in message:
                # Extract tag name from message (format: "A tag with name 'TAG_NAME' already exists...")
                tag_match = re.search(r"name '([^']+)'", message)
                if tag_match:
                    duplicate_tag_name = tag_match.group(1)
                    # Return a special response indicating conflict
                    return {
                        "status": "CONFLICT",
                        "tagName": duplicate_tag_name,
                        "message": message,
                        "errorCode": error_code
                    }
        
        # Check if response is successful (after checking for conflicts)
        if response.status_code in (200, 201):
            if response_json is not None:
                return response_json
            else:
                # Some successful responses might not have JSON body
                return {"status": "success", "statusCode": response.status_code}
        
        # For other error responses, use the parsed JSON or create error details
        error_details = {}
        if response_json is not None:
            error_details = response_json
        else:
            error_details = {"message": response_text[:500] if len(response_text) > 500 else response_text}
        
        raise requests.exceptions.HTTPError(
            f"Failed to create tag: HTTP {response.status_code}\n"
            f"Response: {error_details}"
        )
            
    except requests.exceptions.RequestException as e:
        error_msg = (
            f"Failed to send tag to MDS: {e}\n"
            f"URL: {endpoint}"
        )
        raise requests.exceptions.RequestException(error_msg)


def update_tag_definition_with_missing_attributes(
    tag_definition: Dict[str, Any],
    tag_attributes: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Update tag definition with missing attributes from tag attributes.
    
    If a tag has attributes that are not defined in the tag definition's
    attributeDefinitions, add them with type "string".
    
    Args:
        tag_definition: Tag definition object (MDS format with "tag" key)
        tag_attributes: Tag attributes from the tags object
    
    Returns:
        Updated tag definition with missing attributes added
    """
    if not tag_attributes:
        return tag_definition
    
    # Get the tag object from the definition
    tag = tag_definition.get('tag', {})
    if not tag:
        return tag_definition
    
    # Get existing attribute definitions
    attribute_definitions = tag.get('attributeDefinitions', {})
    if not attribute_definitions:
        attribute_definitions = {}
    
    # Check each attribute in tag_attributes
    for attr_name, attr_value in tag_attributes.items():
        # If attribute is not in attributeDefinitions, add it
        if attr_name not in attribute_definitions:
            attribute_definitions[attr_name] = {
                "type": "String"
            }
    
    # Update the tag definition
    tag['attributeDefinitions'] = attribute_definitions
    tag_definition['tag'] = tag
    
    return tag_definition


def get_leaf_resource_type(
    service_resource: Dict[str, Any],
    resource_hierarchy: List[str]
) -> Optional[str]:
    """
    Get the leaf resource type from a service resource.
    
    The leaf resource type is the type of the last (most specific) resource
    in the hierarchy that has a value in the service resource.
    
    Args:
        service_resource: The RangerServiceResource object
        resource_hierarchy: Ordered list of resource names (from root to leaf)
    
    Returns:
        The leaf resource type name, or None if not found
    """
    resource_elements = service_resource.get('resourceElements', {})
    
    if not resource_elements:
        return None
    
    # Find the last resource in hierarchy that has a value
    # Traverse hierarchy in reverse (from leaf to root) to find the last one with values
    for resource_name in reversed(resource_hierarchy):
        if resource_name in resource_elements:
            resource_element = resource_elements[resource_name]
            values = resource_element.get('values', [])
            if values:
                # Return the resource name as the type (e.g., "column", "table", "schema")
                return resource_name
    
    return None


def batch_list(items: List[Any], batch_size: int) -> List[List[Any]]:
    """
    Split a list into batches of specified size.
    
    Args:
        items: List of items to batch
        batch_size: Size of each batch (0 or negative means no batching - return all items in one batch)
    
    Returns:
        List of batches, where each batch is a list of items
    """
    if batch_size <= 0:
        return [items]
    
    batches = []
    for i in range(0, len(items), batch_size):
        batches.append(items[i:i + batch_size])
    return batches


def send_resources_to_mds(
    mds_url: str,
    service_name: str,
    service_type: str,
    resources: List[Dict[str, str]],
    session: Optional[requests.Session] = None
) -> Dict[str, Any]:
    """
    Send resource sync request to MDS API.
    
    Args:
        mds_url: Base URL of the MDS server (e.g., http://localhost:6080/omni-metadata)
        service_name: Name of the service
        service_type: Type of the service (e.g., "SNOWFLAKE")
        resources: List of resource objects with "resource" and "resourceType" fields
        session: Optional requests Session object for cookie handling
    
    Returns:
        JSON response as a dictionary
    
    Raises:
        requests.RequestException: If the API call fails
        ValueError: If the response is invalid
    """
    # Ensure mds_url doesn't end with a slash
    mds_url = mds_url.rstrip('/')
    
    # Construct the API endpoint
    endpoint = f"{mds_url}/api/v1/metadata/resources/sync"
    
    # Build the payload
    payload = {
        "serviceName": service_name,
        "serviceType": service_type,
        "resources": resources
    }
    
    
    # Prepare request
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    try:
        # Use session if provided, otherwise use requests directly
        if session is not None:
            response = session.post(
                endpoint, 
                headers=headers, 
                json=payload,
                timeout=30,
                allow_redirects=False
            )
        else:
            response = requests.post(
                endpoint, 
                headers=headers, 
                json=payload,
                timeout=30,
                allow_redirects=False
            )
        
        # Handle redirects
        if response.status_code in (301, 302, 303, 307, 308):
            location = response.headers.get('Location', '')
            raise ValueError(
                f"Server redirected to: {location}\n"
                f"This usually means authentication is required."
            )
        
        # Check if response is successful
        if response.status_code in (200, 201):
            try:
                return response.json()
            except json.JSONDecodeError:
                # Some successful responses might not have JSON body
                return {"status": "success", "statusCode": response.status_code}
        
        # For error responses, try to get JSON error details
        response_text = response.text.strip()
        error_details = {}
        try:
            error_details = response.json()
        except json.JSONDecodeError:
            error_details = {"message": response_text[:500] if len(response_text) > 500 else response_text}
        
        raise requests.exceptions.HTTPError(
            f"Failed to sync resources: HTTP {response.status_code}\n"
            f"Response: {error_details}"
        )
            
    except requests.exceptions.RequestException as e:
        error_msg = (
            f"Failed to send resources to MDS: {e}\n"
            f"URL: {endpoint}"
        )
        raise requests.exceptions.RequestException(error_msg)


def build_tag_resource_mappings(
    service_resources: List[Dict[str, Any]],
    resource_paths: List[str],
    resource_to_tag_ids: Dict[str, Any],
    tags: Dict[str, Any],
    tag_definitions: Dict[str, Any],
    service_name: str,
    service_type: str
) -> List[Dict[str, Any]]:
    """
    Build tag-resource mappings from service resources.
    
    Args:
        service_resources: List of service resource objects
        resource_paths: List of resource paths (aligned with service_resources by index)
        resource_to_tag_ids: Map of resource index to tag IDs
        tags: Map of tag ID to tag objects (may contain attributes)
        tag_definitions: Map of tag ID to tag definition objects (contains tag names)
        service_name: Service name
        service_type: Service type
    
    Returns:
        List of tag-resource mapping objects
    """
    mappings = []
    
    for idx, (service_resource, resource_path) in enumerate(zip(service_resources, resource_paths)):
        if not resource_path:
            continue
        
        # Get resource ID from the service resource
        # resourceToTagIds uses resource ID as key, not array index
        resource_id = service_resource.get('id')
        if resource_id is None:
            # Fallback to array index if resource ID is not available
            resource_id = idx
        
        # Get tag IDs for this resource (try both string and int keys)
        tag_ids = None
        if str(resource_id) in resource_to_tag_ids:
            tag_ids = resource_to_tag_ids[str(resource_id)]
        elif resource_id in resource_to_tag_ids:
            tag_ids = resource_to_tag_ids[resource_id]
        
        if not tag_ids:
            continue
        
        # Create a mapping for each tag associated with this resource
        for tag_id in tag_ids:
            # Get tag object (try both string and int keys)
            tag = None
            if str(tag_id) in tags:
                tag = tags[str(tag_id)]
            elif tag_id in tags:
                tag = tags[tag_id]
            
            if not tag:
                continue
            
            # Get tag name from tag definition by matching tag.type with definition name
            # Note: tag IDs from resourceToTagIds are tag IDs (from tags object), not tag definition IDs
            # The tag's 'type' field should match a tag definition's 'name' field
            tag_name = None
            tag_type = tag.get('type')
            
            if tag_type:
                # Search through tag definitions to find one with matching name
                # This is the primary method since tag IDs don't match definition IDs
                for def_id, def_obj in tag_definitions.items():
                    if def_obj.get('name') == tag_type:
                        tag_name = def_obj.get('name')
                        break
            
            # Fallback: use tag.type directly if no matching definition found
            if not tag_name:
                tag_name = tag_type
            
            if not tag_name:
                continue
            
            # Convert tag name to uppercase
            tag_name = tag_name.upper()
            
            # Get tag attributes if present
            tag_attributes = {}
            if 'attributes' in tag:
                tag_attributes = tag.get('attributes', {})
            elif 'attributeValues' in tag:
                # Some formats might use attributeValues
                tag_attributes = tag.get('attributeValues', {})
            
            # Build the mapping object
            mapping = {
                "tagName": tag_name,
                "resourcePath": resource_path,
                "serviceName": service_name,
                "serviceType": service_type
            }
            
            # Add tag attributes if present (only if not empty)
            if tag_attributes:
                mapping["tagAttributes"] = tag_attributes
            
            mappings.append(mapping)
    
    return mappings


def send_tag_resource_mappings_to_mds(
    mds_url: str,
    mappings: List[Dict[str, Any]],
    session: Optional[requests.Session] = None
) -> Dict[str, Any]:
    """
    Send tag-resource mappings to MDS API.
    
    Args:
        mds_url: Base URL of the MDS server (e.g., http://localhost:6080/omni-metadata)
        mappings: List of tag-resource mapping objects
        session: Optional requests Session object for cookie handling
    
    Returns:
        JSON response as a dictionary
    
    Raises:
        requests.RequestException: If the API call fails
        ValueError: If the response is invalid
    """
    # Ensure mds_url doesn't end with a slash
    mds_url = mds_url.rstrip('/')
    
    # Construct the API endpoint
    endpoint = f"{mds_url}/api/v1/metadata/tags/resource-mappings"
    
    # Build the payload
    payload = {
        "tagResourceMappings": mappings
    }
    
    # Prepare request
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    try:
        # Use session if provided, otherwise use requests directly
        if session is not None:
            response = session.patch(
                endpoint, 
                headers=headers, 
                json=payload,
                timeout=30,
                allow_redirects=False
            )
        else:
            response = requests.patch(
                endpoint, 
                headers=headers, 
                json=payload,
                timeout=30,
                allow_redirects=False
            )
        
        # Handle redirects
        if response.status_code in (301, 302, 303, 307, 308):
            location = response.headers.get('Location', '')
            raise ValueError(
                f"Server redirected to: {location}\n"
                f"This usually means authentication is required."
            )
        
        # Check if response is successful (200, 201) or partial success (206)
        if response.status_code in (200, 201, 206):
            try:
                response_data = response.json()
                # Return response even if it's PARTIAL_SUCCESS - let caller handle it
                return response_data
            except json.JSONDecodeError:
                # Some successful responses might not have JSON body
                return {"status": "success", "statusCode": response.status_code}
        
        # For error responses, try to get JSON error details
        response_text = response.text.strip()
        error_details = {}
        try:
            error_details = response.json()
        except json.JSONDecodeError:
            error_details = {"message": response_text[:500] if len(response_text) > 500 else response_text}
        
        raise requests.exceptions.HTTPError(
            f"Failed to create tag-resource mappings: HTTP {response.status_code}\n"
            f"Response: {error_details}"
        )
            
    except requests.exceptions.RequestException as e:
        error_msg = (
            f"Failed to send tag-resource mappings to MDS: {e}\n"
            f"URL: {endpoint}"
        )
        raise requests.exceptions.RequestException(error_msg)


def format_resource_path_output(
    service_resource: Dict[str, Any],
    resource_path: str,
    resource_index: int,
    include_tags: bool = False,
    tag_map: Optional[Dict[str, Any]] = None,
    resource_to_tag_ids: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Format the output for a resource path.
    
    Args:
        service_resource: The RangerServiceResource object
        resource_path: The built resource path
        resource_index: Index of the resource in the serviceResources list
        include_tags: Whether to include tag information
        tag_map: Map of tag IDs to tag objects (keys may be int or str)
        resource_to_tag_ids: Map of resource index to tag IDs (keys may be int or str)
    
    Returns:
        Dictionary with formatted output
    """
    output = {
        'resourcePath': resource_path,
        'serviceName': service_resource.get('serviceName'),
        'guid': service_resource.get('guid'),
        'resourceSignature': service_resource.get('resourceSignature'),
        'resourceElements': service_resource.get('resourceElements', {})
    }
    
    if include_tags and tag_map and resource_to_tag_ids:
        tags = []
        
        # Try both string and int keys for resource index
        tag_ids = None
        if str(resource_index) in resource_to_tag_ids:
            tag_ids = resource_to_tag_ids[str(resource_index)]
        elif resource_index in resource_to_tag_ids:
            tag_ids = resource_to_tag_ids[resource_index]
        
        if tag_ids:
            for tag_id in tag_ids:
                # Try both string and int keys for tag ID
                tag = None
                if str(tag_id) in tag_map:
                    tag = tag_map[str(tag_id)]
                elif tag_id in tag_map:
                    tag = tag_map[tag_id]
                
                if tag:
                    tags.append({
                        'id': tag_id,
                        'type': tag.get('type'),
                        'attributes': tag.get('attributes', {})
                    })
        
        output['tags'] = tags
    
    return output


def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(
        description='Download tags from Ranger Admin and build resource paths based on hierarchy',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 get_tagged_resources.py --admin-url http://localhost:6080 --service-name cm_hive
  python3 get_tagged_resources.py --admin-url https://ranger.example.com --service-name hdfs
  python3 get_tagged_resources.py --admin-url http://localhost:6080 --service-name cm_hive --output-json paths.json
  python3 get_tagged_resources.py --admin-url http://localhost:6080 --service-name cm_hive --include-tags
  python3 get_tagged_resources.py --admin-url http://localhost:6080 --service-name cm_hive --save-tags-json tags.json
  python3 get_tagged_resources.py --admin-url http://localhost:6080 --service-name cm_hive --sync-tags-to-mds
  python3 get_tagged_resources.py --admin-url http://localhost:6080 --service-name cm_hive --sync-tags-to-mds --tag-type Internal
  python3 get_tagged_resources.py --admin-url http://localhost:6080 --service-name privacera_snowflake --sync-resources-to-mds --service-type SNOWFLAKE
  python3 get_tagged_resources.py --admin-url http://localhost:6080 --service-name privacera_snowflake --sync-tag-resource-mappings --service-type SNOWFLAKE
        """
    )
    
    parser.add_argument(
        '--admin-url',
        required=True,
        help='Base URL of the Ranger Admin server (e.g., http://localhost:6080)'
    )
    
    parser.add_argument(
        '--service-name',
        required=True,
        help='Name of the service to download tags for'
    )
    
    parser.add_argument(
        '--include-tags',
        action='store_true',
        help='Include tag information in the output'
    )
    
    parser.add_argument(
        '--save-tags-json',
        help='Optional: Save the raw tags/download API response to a JSON file'
    )
    
    parser.add_argument(
        '--sync-tags-to-mds',
        action='store_true',
        help='Sync tag definitions to MDS (Metadata Service)'
    )
    
    parser.add_argument(
        '--tag-type',
        default='Internal',
        help='Tag type to apply to all tag definitions when syncing to MDS. Supported values: "Sensitive", "Confidential", "Internal", "Public". Default: "Internal"'
    )
    
    parser.add_argument(
        '--sync-resources-to-mds',
        action='store_true',
        help='Sync resources to MDS (Metadata Service)'
    )
    
    
    
    parser.add_argument(
        '--service-type',
        required=True,
        help='Service type for resource sync and tag-resource mappings (e.g., "SNOWFLAKE", "HIVE")'
    )
    
    parser.add_argument(
        '--sync-tag-resource-mappings',
        action='store_true',
        help='Sync tag-resource mappings to MDS (Metadata Service)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Batch size for syncing resources and tag-resource mappings to MDS. Default: 100. Set to 0 to disable batching (send all at once).'
    )
    
    args = parser.parse_args()
    
    # Validate tag type is one of the supported values
    supported_tag_types = {'Sensitive', 'Confidential', 'Internal', 'Public'}
    if args.tag_type not in supported_tag_types:
        parser.error(
            f"Invalid tag type: '{args.tag_type}'. "
            f"Supported values are: {', '.join(sorted(supported_tag_types))}"
        )
    
    try:
        # Print input parameters
        print(f"Admin URL: {args.admin_url}")
        print(f"Service Name: {args.service_name}")
        
        print(f"Service Type: {args.service_type}")
        print(f"Tag Type: {args.tag_type}")
        print()
        
        # Fetch service definition to get hierarchy
        # Create a session to handle cookies
        session = requests.Session()
        
        service_policies = fetch_service_definition(
            args.admin_url, 
            args.service_name,
            session
        )
        
        # Extract resources and build hierarchy
        resources = extract_resources(service_policies)
        
        if not resources:
            print("Error: No resources found in service definition. Cannot build paths.")
            return 1
        
        builder = ResourceHierarchyBuilder(resources)
        root_nodes = builder.build_hierarchy()
        resource_hierarchy = builder.get_ordered_resource_names()
        
        # Initialize variables for payloads (will be populated and sent at the end)
        converted_tag_definitions = []
        tag_definitions_map = {}  # Map tag name (uppercase) -> converted tag definition
        mds_resources = []
        mappings = []
        
        # Fetch service tags
        try:
            service_tags = fetch_service_tags(
                args.admin_url, 
                args.service_name,
                session
            )
            
            # Save raw tags response if requested
            if args.save_tags_json:
                with open(args.save_tags_json, 'w') as f:
                    json.dump(service_tags, f, indent=2, default=str)
            
            # Extract tag definitions (needed for both sync and mappings)
            tag_definitions_raw = extract_tag_definitions(service_tags)
            
            # Convert tag definitions to MDS format and store them
            # Map by tag name (uppercase) for easy lookup when processing mappings
            
            if tag_definitions_raw:
                for tag_def in tag_definitions_raw:
                    # Validate that tag definition has a name field
                    if 'name' not in tag_def or not tag_def.get('name'):
                        raise ValueError(
                            f"Tag definition is missing required 'name' field. "
                            f"Tag definition: {tag_def}"
                        )
                    
                    tag_name = tag_def.get('name')
                    try:
                        mds_tag_request = convert_tag_definition_to_mds_format(
                            tag_def, 
                            tag_type_override=args.tag_type
                        )
                        tag_name_upper = tag_name.upper()
                        converted_tag_definitions.append(mds_tag_request)
                        tag_definitions_map[tag_name_upper] = mds_tag_request
                    except Exception as e:
                        pass  # Silently skip failed conversions
            
        except Exception as e:
            print(f"Error fetching tags: {e}")
            return 1
        
        # Extract service resources
        service_resources = extract_service_resources(service_tags)
        
        if not service_resources:
            return 0
        
        # Build resource paths
        
        output_data = []
        # Get tag information (needed for both include-tags and tag-resource mappings)
        tag_map = service_tags.get('tags', {})
        resource_to_tag_ids = service_tags.get('resourceToTagIds', {})
        tag_definitions_dict = service_tags.get('tagDefinitions', {})
        
        # Collect resource paths for tag-resource mappings
        resource_paths_list = []
        
        for idx, service_resource in enumerate(service_resources):
            resource_path = build_resource_path(
                service_resource,
                resource_hierarchy
            )
            
            if resource_path:
                output_item = format_resource_path_output(
                    service_resource,
                    resource_path,
                    idx,
                    args.include_tags,
                    tag_map if args.include_tags else None,
                    resource_to_tag_ids if args.include_tags else None
                )
                output_data.append(output_item)
                resource_paths_list.append(resource_path)
                
                # Collect resource for MDS sync if requested
                if args.sync_resources_to_mds:
                    resource_type = get_leaf_resource_type(service_resource, resource_hierarchy)
                    if resource_type:
                        mds_resources.append({
                            "resource": resource_path,
                            "resourceType": resource_type
                        })
            else:
                resource_paths_list.append(None)  # Keep alignment for mapping
        
        # Sync tag-resource mappings to MDS if requested
        if args.sync_tag_resource_mappings:
            # Build tag-resource mappings using original indices
            # resource_paths_list is aligned with service_resources by index
            # We need to pass the full lists but only process resources with valid paths
            if resource_paths_list and any(resource_paths_list):
                # Build mappings (function will skip None paths)
                mappings = build_tag_resource_mappings(
                    service_resources,
                    resource_paths_list,
                    resource_to_tag_ids,
                    tag_map,
                    tag_definitions_dict,
                    args.service_name,
                    args.service_type
                )
                
                if mappings:
                    # Check and update tag definitions with missing attributes
                    updated_tags = set()
                    for mapping in mappings:
                        tag_name = mapping.get('tagName', '').upper()
                        tag_attributes = mapping.get('tagAttributes', {})
                        
                        if tag_attributes and tag_name in tag_definitions_map:
                            # Check if tag definition needs updating
                            tag_def = tag_definitions_map[tag_name]
                            updated_tag_def = update_tag_definition_with_missing_attributes(
                                tag_def,
                                tag_attributes
                            )
                            
                            # Check if any attributes were added
                            original_attrs = tag_def.get('tag', {}).get('attributeDefinitions', {})
                            updated_attrs = updated_tag_def.get('tag', {}).get('attributeDefinitions', {})
                            
                            if len(updated_attrs) > len(original_attrs):
                                # Attributes were added
                                tag_definitions_map[tag_name] = updated_tag_def
                                updated_tags.add(tag_name)
                    
                    # Update the converted_tag_definitions list with updated definitions
                    if updated_tags:
                        for i, tag_def in enumerate(converted_tag_definitions):
                            tag_name = tag_def.get('tag', {}).get('name', '').upper()
                            if tag_name in tag_definitions_map:
                                converted_tag_definitions[i] = tag_definitions_map[tag_name]
        
        # Now send all payloads in order: tag definitions → resources → mappings
        if args.sync_tags_to_mds or args.sync_resources_to_mds or args.sync_tag_resource_mappings:
            # Build MDS URL from admin_url
            mds_url = f"{args.admin_url.rstrip('/')}/omni-metadata"
            
            # Step 1: Send tag definitions first
            if args.sync_tags_to_mds and converted_tag_definitions:
                created_count = 0  # Successfully created tag definitions
                existing_count = 0  # Tag definitions that already exist
                failed_count = 0  # Tag definitions that failed to create
                existing_tag_names = []  # List to collect existing tag names
                failed_tag_names = []  # List to collect failed tag names
                
                for mds_tag_request in converted_tag_definitions:
                    tag_name = mds_tag_request.get('tag', {}).get('name', 'unknown')
                    try:
                        # Send to MDS
                        response = send_tag_to_mds(
                            mds_url,
                            mds_tag_request,
                            session
                        )
                        
                        # Check if response indicates a conflict (tag already exists)
                        if isinstance(response, dict) and response.get('status') == 'CONFLICT':
                            duplicate_tag_name = response.get('tagName', tag_name)
                            existing_tag_names.append(duplicate_tag_name)
                            existing_count += 1
                        else:
                            created_count += 1
                        
                    except ValueError as e:
                        failed_tag_names.append(tag_name)
                        failed_count += 1
                    except requests.exceptions.RequestException as e:
                        failed_tag_names.append(tag_name)
                        failed_count += 1
                    except Exception as e:
                        failed_tag_names.append(tag_name)
                        failed_count += 1
                
                # Print summary counts
                print(f"\nTag Definition Sync Summary:")
                print(f"  Created: {created_count}")
                print(f"  Already Exist: {existing_count}")
                print(f"  Failed: {failed_count}")
                print(f"  Total Processed: {created_count + existing_count + failed_count}")
                
                # Print existing tag names
                if existing_tag_names:
                    print(f"\n⚠️  The following {len(existing_tag_names)} tag definition(s) already exist in MDS (skipped):")
                    for existing_tag in existing_tag_names:
                        print(f"  - {existing_tag}")
                
                # Print failed tag names
                if failed_tag_names:
                    print(f"\n❌ The following {len(failed_tag_names)} tag definition(s) failed to sync:")
                    for failed_tag in failed_tag_names:
                        print(f"  - {failed_tag}")
            
            # Step 2: Send resources
            if args.sync_resources_to_mds and mds_resources:
                try:
                    resource_batches = batch_list(mds_resources, args.batch_size)
                    total_synced = 0
                    
                    for batch_idx, resource_batch in enumerate(resource_batches):
                        batch_num = batch_idx + 1
                        total_batches = len(resource_batches)
                        
                        if total_batches > 1:
                            print(f"Syncing resource batch {batch_num}/{total_batches} ({len(resource_batch)} resources)...")
                        
                        response = send_resources_to_mds(
                            mds_url,
                            args.service_name,
                            args.service_type,
                            resource_batch,
                            session
                        )
                        total_synced += len(resource_batch)
                    
                    print(f"Successfully synced {total_synced} resource(s) to MDS in {len(resource_batches)} batch(es)")
                    
                except ValueError as e:
                    print(f"❌ Failed to sync resources: {e}")
                    return 1
                except requests.exceptions.RequestException as e:
                    print(f"❌ Failed to sync resources to MDS: {e}")
                    return 1
                except Exception as e:
                    print(f"❌ Unexpected error syncing resources: {e}")
                    return 1
            
            # Step 3: Send tag-resource mappings
            if args.sync_tag_resource_mappings and mappings:
                try:
                    mapping_batches = batch_list(mappings, args.batch_size)
                    total_synced = 0
                    total_success_count = 0
                    total_failure_count = 0
                    
                    for batch_idx, mapping_batch in enumerate(mapping_batches):
                        batch_num = batch_idx + 1
                        total_batches = len(mapping_batches)
                        
                        if total_batches > 1:
                            print(f"Syncing tag-resource mapping batch {batch_num}/{total_batches} ({len(mapping_batch)} mappings)...")
                        
                        response = send_tag_resource_mappings_to_mds(
                            mds_url,
                            mapping_batch,
                            session
                        )
                        
                        # Check response status and handle PARTIAL_SUCCESS
                        response_status = response.get('status', 'UNKNOWN')
                        
                        if response_status == 'PARTIAL_SUCCESS':
                            # Extract counts from response (handle both field name variations)
                            # Try successfulMappings first, then successfulUpdates, default to 0
                            successful_mappings = response.get('successfulMappings')
                            if successful_mappings is None:
                                successful_mappings = response.get('successfulUpdates', 0)
                            
                            # Try failedMappings first, then failedUpdates, default to 0
                            failed_mappings = response.get('failedMappings')
                            if failed_mappings is None:
                                failed_mappings = response.get('failedUpdates', 0)
                            
                            total_requests = response.get('totalRequests', 0)
                            failed_results = response.get('failedResults', [])
                            
                            # Increment counts
                            total_success_count += successful_mappings
                            total_failure_count += failed_mappings
                            
                            # Log failed results data
                            if failed_results:
                                print(f"\n[FAILED RESULTS] Batch {batch_num} failed results:")
                                print(json.dumps(failed_results, indent=2))
                                print()
                            
                            total_synced += successful_mappings
                        elif response_status in ('SUCCESS', 'success'):
                            # Full success - all mappings in batch succeeded
                            batch_success_count = len(mapping_batch)
                            total_success_count += batch_success_count
                            total_synced += batch_success_count
                        else:
                            # Unknown status - assume all failed
                            total_failure_count += len(mapping_batch)
                            print(f"  ⚠️  Unknown status '{response_status}' for batch {batch_num}, treating as failure")
                    
                    # Print final summary
                    print(f"Tag-resource mapping sync completed:")
                    print(f"  Total successful: {total_success_count}")
                    print(f"  Total failed: {total_failure_count}")
                    print(f"  Total processed: {total_success_count + total_failure_count} in {len(mapping_batches)} batch(es)")
                    
                except ValueError as e:
                    print(f"❌ Failed to sync tag-resource mappings: {e}")
                    return 1
                except requests.exceptions.RequestException as e:
                    print(f"❌ Failed to sync tag-resource mappings to MDS: {e}")
                    return 1
                except Exception as e:
                    print(f"❌ Unexpected error syncing tag-resource mappings: {e}")
                    return 1
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        return 130
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        if '--verbose' in sys.argv or '-v' in sys.argv:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())

