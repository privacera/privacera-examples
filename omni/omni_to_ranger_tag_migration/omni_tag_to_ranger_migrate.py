#!/usr/bin/env python3
"""
Script to download tags and tag-resource mappings from Omni Metadata Service and push them to Ranger.

This script:
1. Fetches tag definitions from Omni Metadata Service
2. Fetches tag-resource mappings from Omni Metadata Service
3. Converts Omni data to Ranger ServiceTags format
4. Pushes the data to Ranger Admin API using PUT service/tags/importservicetags
"""

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import requests

# Default base URL for Privacera Cloud API (can be overridden via BASE_URL env var or --base-url arg)
DEFAULT_BASE_URL = "https://api.privaceracloud.com/api"


def setup_logging(log_file: Optional[str] = None, log_level: str = 'INFO') -> None:
    """
    Set up logging configuration with both file and console handlers.
    
    Args:
        log_file: Path to log file. If None, generates a timestamped log file name.
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Generate default log file name if not provided
    if log_file is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f"omni_tag_to_ranger_migrate_{timestamp}.log"
    
    # Ensure log directory exists
    log_dir = os.path.dirname(log_file) if os.path.dirname(log_file) else '.'
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    # Convert string log level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Remove existing handlers to avoid duplicates
    root_logger.handlers = []
    
    # File handler - logs everything with detailed format
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)  # Log everything to file
    file_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(file_handler)
    
    # Console handler - logs INFO and above with simpler format
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # Log initial message
    logging.info(f"Logging initialized. Log file: {os.path.abspath(log_file)}")
    logging.info(f"Log level: {log_level.upper()}")


def fetch_omni_api(
    omni_url: str,
    api_path: str,
    params: Optional[Dict[str, Any]] = None,
    session: Optional[requests.Session] = None,
    timeout: Optional[int] = None,
    max_retries: int = 3,
    retry_delay: int = 2
) -> Dict[str, Any]:
    """
    Fetch data from the Omni Metadata Service API with retry logic for timeouts.
    
    Args:
        omni_url: Base URL of the Omni Metadata Service (e.g., https://omni-pcloud-dev-apiserver.nextgen.privacera.us/api/620071085fa8135a90e23482d437ac0c83bd5a15716995fc6402c212cda9cbc8/omni-metadata)
        api_path: API path (e.g., /api/v1/metadata/tags)
        params: Optional query parameters
        session: Optional requests Session object for cookie handling
        timeout: Optional timeout in seconds. Defaults to 120 for export endpoints, 30 for others
        max_retries: Maximum number of retry attempts for timeout errors (default: 3)
        retry_delay: Initial delay in seconds between retries, doubles with each retry (default: 2)
    
    Returns:
        JSON response as a dictionary
    
    Raises:
        requests.RequestException: If the API call fails after all retries
        ValueError: If the response is invalid
    """
    # Ensure omni_url doesn't end with a slash
    omni_url = omni_url.rstrip('/')
    
    # Ensure api_path starts with a slash
    if not api_path.startswith('/'):
        api_path = '/' + api_path
    
    # Construct the API endpoint by concatenating (not using urljoin which replaces path)
    endpoint = f"{omni_url}{api_path}"
    
    # Determine timeout based on endpoint type if not explicitly provided
    if timeout is None:
        # Use longer timeout for export endpoints that process large amounts of data
        if '/export' in api_path:
            timeout = 120  # 2 minutes for export endpoints
        else:
            timeout = 30  # 30 seconds for regular endpoints
    
    # Build full URL with query parameters for logging
    if params:
        query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        full_url = f"{endpoint}?{query_string}"
    else:
        full_url = endpoint
    
    # Prepare request
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    # Retry logic for timeout errors
    last_exception = None
    for attempt in range(max_retries + 1):
        # Log request details (only on first attempt or if retrying)
        if attempt == 0:
            logging.debug(f"\n[OMNI API REQUEST]")
            logging.debug(f"  URL: {full_url}")
            logging.debug(f"  Method: GET")
            logging.debug(f"  Headers: {json.dumps(headers, indent=2)}")
            if params:
                logging.debug(f"  Query Parameters: {json.dumps(params, indent=2)}")
            logging.debug(f"  Timeout: {timeout} seconds")
            logging.debug(f"  Payload: None (GET request)")
        elif attempt > 0:
            delay = retry_delay * (2 ** (attempt - 1))  # Exponential backoff
            logging.warning(f"  ⚠️  Retry attempt {attempt}/{max_retries} after {delay} seconds...")
            time.sleep(delay)
        
        try:
            # Use session if provided, otherwise use requests directly
            if session is not None:
                response = session.get(endpoint, headers=headers, params=params, timeout=timeout, allow_redirects=False)
            else:
                response = requests.get(endpoint, headers=headers, params=params, timeout=timeout, allow_redirects=False)
            
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
            
            # Log response details
            logging.debug(f"\n[OMNI API RESPONSE]")
            logging.debug(f"  Status Code: {response.status_code}")
            logging.debug(f"  Headers: {json.dumps(dict(response.headers), indent=2)}")
            
            # Check if response is empty
            if not response_text:
                logging.warning(f"  Warning: Empty response from server")
                logging.debug(f"  Response Body: (empty)")
                return {}
            
            # Check if response is HTML (likely an error/login page)
            if response_text.startswith('<') or 'html' in response.headers.get('Content-Type', '').lower():
                logging.debug(f"  Response Body (HTML): {response_text[:500]}")
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
                response_json = response.json()
                response_size = len(json.dumps(response_json, default=str))
                logging.debug(f"  Response Body (JSON): {response_size} bytes")
                
                # Log success on retry
                if attempt > 0:
                    logging.info(f"  ✅ Request succeeded on retry attempt {attempt}")
                
                return response_json
            except json.JSONDecodeError as e:
                preview = response_text[:500] if len(response_text) > 500 else response_text
                raise ValueError(
                    f"Invalid JSON response: {e}\n"
                    f"Response status: {response.status_code}\n"
                    f"Response content (first 500 chars): {preview}\n"
                    f"Full response length: {len(response_text)} bytes"
                )
                
        except (requests.exceptions.ReadTimeout, requests.exceptions.Timeout) as e:
            last_exception = e
            if attempt < max_retries:
                # Will retry on next iteration
                continue
            else:
                # Final attempt failed, raise the exception
                response_status = 'N/A'
                response_text = ''
                if hasattr(e, 'response') and e.response:
                    response_status = e.response.status_code
                    try:
                        response_text = e.response.text[:200]
                    except:
                        pass
                
                error_msg = (
                    f"Failed to fetch data after {max_retries + 1} attempts: {e}\n"
                    f"URL: {endpoint}\n"
                    f"Status Code: {response_status}\n"
                    f"Timeout: {timeout} seconds"
                )
                if response_text:
                    error_msg += f"\nResponse: {response_text}"
                
                raise requests.exceptions.RequestException(error_msg)
                
        except requests.exceptions.RequestException as e:
            # For non-timeout errors, don't retry
            response_status = 'N/A'
            response_text = ''
            if hasattr(e, 'response') and e.response:
                response_status = e.response.status_code
                try:
                    response_text = e.response.text[:200]
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
    
    # This should never be reached, but just in case
    if last_exception:
        raise requests.exceptions.RequestException(f"Failed after {max_retries + 1} attempts: {last_exception}")


def fetch_tag_definitions(
    omni_url: str,
    page: int = 0,
    size: int = 100,
    session: Optional[requests.Session] = None
) -> List[Dict[str, Any]]:
    """
    Fetch all tag definitions from Omni Metadata Service.
    
    Args:
        omni_url: Base URL of the Omni Metadata Service
        page: Page number (0-indexed)
        size: Page size
        session: Optional requests Session object
    
    Returns:
        List of tag definition objects
    """
    all_tags = []
    has_more = True
    current_page = page
    
    while has_more:
        params = {
            'sortBy': 'name',
            'sortOrder': 'asc',
            'page': current_page,
            'size': size
        }
        
        response = fetch_omni_api(
            omni_url,
            '/api/v1/metadata/tags',
            params=params,
            session=session
        )
        
        content = response.get('content', [])
        all_tags.extend(content)
        
        has_more = response.get('hasNext', False)
        current_page += 1
        
        if content:
            logging.info(f"  Fetched {len(content)} tag definitions (page {current_page})")
    
    return all_tags


def fetch_and_send_tags_page_by_page(
    omni_url: str,
    ranger_url: str,
    service_name: str,
    page_size: int = 20,
    session: Optional[requests.Session] = None,
    ranger_username: Optional[str] = None,
    ranger_password: Optional[str] = None
) -> int:
    """
    Fetch tag definitions page by page and send each page to Ranger immediately.
    
    Args:
        omni_url: Base URL of the Omni Metadata Service
        ranger_url: Base URL of the Ranger Admin server
        service_name: Service name
        page_size: Page size (default: 20)
        session: Optional requests Session object
        ranger_username: Optional username for Ranger basic authentication
        ranger_password: Optional password for Ranger basic authentication
    
    Returns:
        Total number of tag definitions processed
    """
    total_tags = 0
    has_next = True
    current_page = 0
    
    while has_next:
        params = {
            'sortBy': 'name',
            'sortOrder': 'asc',
            'page': current_page,
            'size': page_size
        }
        
        logging.info(f"\nFetching tag definitions (page {current_page}, size {page_size})...")
        response = fetch_omni_api(
            omni_url,
            '/api/v1/metadata/tags',
            params=params,
            session=session
        )
        
        tag_definitions = response.get('content', [])
        has_next = response.get('hasNext', False)
        
        if tag_definitions:
            logging.info(f"  Fetched {len(tag_definitions)} tag definitions from page {current_page}")
            
            # Build ServiceTags payload with only tag definitions (no mappings)
            service_tags = build_service_tags_payload(
                service_name,
                tag_definitions,
                [],  # Empty tag-resource mappings
                None,  # service_type not needed for tag definitions only
                ranger_url=ranger_url,
                session=session,
                ranger_username=ranger_username,
                ranger_password=ranger_password
            )
            
            tag_def_count = len(service_tags.get('tagDefinitions', {}))
            logging.info(f"  Built payload with {tag_def_count} tag definitions")
            
            # Send to Ranger
            logging.info(f"  Sending tag definitions to Ranger...")
            try:
                ranger_response = send_service_tags_to_ranger(
                    ranger_url,
                    service_tags,
                    session=session,
                    ranger_username=ranger_username,
                    ranger_password=ranger_password
                )
                logging.info(f"  ✅ Successfully sent {len(tag_definitions)} tag definitions to Ranger")
                total_tags += len(tag_definitions)
            except Exception as e:
                logging.error(f"  ❌ Failed to send tag definitions to Ranger: {e}")
                raise
        
        # Move to next page
        current_page += 1
    
    return total_tags


def fetch_tag_resource_mappings(
    omni_url: str,
    service_name: str,
    service_type: str,
    session: Optional[requests.Session] = None
) -> List[Dict[str, Any]]:
    """
    Fetch tag-resource mappings from Omni Metadata Service.
    
    Args:
        omni_url: Base URL of the Omni Metadata Service
        service_name: Service name (e.g., privacera_snowflake)
        service_type: Service type (e.g., SNOWFLAKE)
        session: Optional requests Session object
    
    Returns:
        List of mapping objects with resourcePath, resourceType, and tags
    """
    params = {
        'serviceName': service_name,
        'serviceType': service_type
    }
    
    response = fetch_omni_api(
        omni_url,
        '/api/v1/metadata/tags/resource-mappings/export',
        params=params,
        session=session
    )
    
    mappings = response.get('mappings', [])
    logging.info(f"  Fetched {len(mappings)} tag-resource mappings")
    
    return mappings


def fetch_and_send_tag_resource_mappings_page_by_page(
    omni_url: str,
    ranger_url: str,
    service_name: str,
    service_type: str,
    session: Optional[requests.Session] = None,
    ranger_username: Optional[str] = None,
    ranger_password: Optional[str] = None
) -> Tuple[int, int]:
    """
    Fetch tag-resource mappings page by page using cursor-based pagination and send each page to Ranger immediately.
    
    Args:
        omni_url: Base URL of the Omni Metadata Service
        ranger_url: Base URL of the Ranger Admin server
        service_name: Service name (e.g., privacera_snowflake)
        service_type: Service type (e.g., SNOWFLAKE)
        session: Optional requests Session object
        ranger_username: Optional username for Ranger basic authentication
        ranger_password: Optional password for Ranger basic authentication
    
    Returns:
        Tuple of (total_mappings, total_resources) processed
    """
    start_time = time.time()
    total_mappings = 0
    total_resources = 0
    has_more = True
    cursor = None
    page_number = 0
    
    while has_more:
        params = {
            'serviceName': service_name,
            'serviceType': service_type
        }
        
        # Add cursor parameter if available (for pagination)
        if cursor:
            params['cursor'] = cursor
        
        logging.info(f"\nFetching tag-resource mappings (page {page_number})...")
        if cursor:
            logging.debug(f"  Using cursor: {cursor[:50]}...")
        
        response = fetch_omni_api(
            omni_url,
            '/api/v1/metadata/tags/resource-mappings/export',
            params=params,
            session=session
        )
        
        mappings = response.get('mappings', [])
        paging = response.get('paging', {})
        has_more = paging.get('hasMore', False)
        next_cursor = paging.get('nextCursor')
        
        # Filter out mappings with empty tags (not needed in Ranger)
        filtered_mappings = [
            mapping for mapping in mappings
            if mapping.get('tags') and len(mapping.get('tags', [])) > 0
        ]
        
        if mappings:
            logging.info(f"  Fetched {len(mappings)} tag-resource mappings from page {page_number}")
            if len(filtered_mappings) < len(mappings):
                logging.info(f"  Filtered out {len(mappings) - len(filtered_mappings)} mappings with empty tags")
        
        if filtered_mappings:
            # Split mappings into batches of 10
            batch_size = 10
            mapping_batches = [
                filtered_mappings[i:i + batch_size]
                for i in range(0, len(filtered_mappings), batch_size)
            ]
            total_batches = len(mapping_batches)
            
            logging.debug(f"  Splitting {len(filtered_mappings)} mappings into {total_batches} batches (max {batch_size} mappings per batch)")
            
            # Send batches in parallel
            max_workers = 5
            responses = []
            errors = []
            page_successful_resources = 0
            page_successful_batches = 0
            
            def send_mapping_batch(batch_num: int, batch_mappings: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], int]:
                """Helper function to build and send a batch of mappings."""
                # Build ServiceTags payload for this batch
                service_tags = build_service_tags_payload(
                    service_name,
                    [],  # Empty tag definitions
                    batch_mappings,
                    service_type,
                    ranger_url=ranger_url,
                    session=session,
                    ranger_username=ranger_username,
                    ranger_password=ranger_password
                )
                
                tags_count = len(service_tags.get('tags', {}))
                resources_count = len(service_tags.get('serviceResources', []))
                mappings_count = sum(len(tag_ids) for tag_ids in service_tags.get('resourceToTagIds', {}).values())
                
                logging.debug(f"  Batch {batch_num}/{total_batches}: Built payload with {tags_count} tags, {resources_count} resources, {mappings_count} mappings")
                
                # Send to Ranger
                response = send_service_tags_to_ranger(
                    ranger_url,
                    service_tags,
                    session=session,
                    ranger_username=ranger_username,
                    ranger_password=ranger_password
                )
                return response, resources_count
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all batches
                future_to_batch = {
                    executor.submit(send_mapping_batch, i + 1, batch): (i + 1, batch)
                    for i, batch in enumerate(mapping_batches)
                }
                
                # Process completed batches
                failed_batches_count = 0  # Track number of failed batches for logging
                failed_batches_dir = get_failed_batches_directory()
                for future in as_completed(future_to_batch):
                    batch_num, batch = future_to_batch[future]
                    batch_mappings_count = len(batch)
                    try:
                        response, resources_count = future.result()
                        responses.append(response)
                        page_successful_resources += resources_count
                        page_successful_batches += 1
                        logging.debug(f"  ✅ Batch {batch_num}/{total_batches} ({batch_mappings_count} mappings) sent successfully")
                    except Exception as e:
                        error_msg = str(e)
                        errors.append((batch_num, batch_mappings_count, error_msg))
                        # Build payload for failed batch and save to individual JSON file
                        try:
                            service_tags_payload = build_service_tags_payload(
                                service_name,
                                [],
                                batch,
                                service_type,
                                ranger_url=ranger_url,
                                session=session,
                                ranger_username=ranger_username,
                                ranger_password=ranger_password
                            )
                            batch_info = {
                                "batch_num": batch_num,
                                "total_batches": total_batches,
                                "mappings_count": batch_mappings_count,
                                "error": error_msg,
                                "payload": service_tags_payload
                            }
                        except Exception as payload_error:
                            logging.error(f"  ⚠️  Failed to build payload for batch {batch_num}: {payload_error}")
                            batch_info = {
                                "batch_num": batch_num,
                                "total_batches": total_batches,
                                "mappings_count": batch_mappings_count,
                                "error": error_msg,
                                "payload": None,
                                "payload_error": str(payload_error)
                            }
                        
                        # Save each failed batch to its own file
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        failed_batch_file = os.path.join(
                            failed_batches_dir,
                            f"failed_batch_page{page_number}_batch{batch_num}_{timestamp}.json"
                        )
                        
                        try:
                            with open(failed_batch_file, 'w', encoding='utf-8') as f:
                                json.dump({
                                    "service_name": service_name,
                                    "service_type": service_type,
                                    "ranger_url": ranger_url,
                                    "page_number": page_number,
                                    "failed_batches": [batch_info],  # Single batch in array for consistency
                                    "created_at": datetime.now().isoformat()
                                }, f, indent=2, default=str)
                            failed_batches_count += 1
                            logging.warning(f"  ⚠️  Saved failed batch {batch_num} to {os.path.basename(failed_batch_file)}")
                        except Exception as save_error:
                            logging.error(f"  ❌ Failed to save failed batch {batch_num} to JSON file: {save_error}")
                        
                        logging.error(f"  ❌ Batch {batch_num}/{total_batches} ({batch_mappings_count} mappings) failed: {e}")
                
                # Log summary of failed batches for this page
                if failed_batches_count > 0:
                    logging.warning(f"  ⚠️  Saved {failed_batches_count} failed batch(es) from page {page_number} to individual files")
            
            # Log page-level summary (INFO level so it always appears)
            logging.info(f"  ✅ Page {page_number}: Sent {page_successful_resources} resources ({page_successful_batches}/{total_batches} batches successful)")
            total_mappings += len(filtered_mappings)
            total_resources += page_successful_resources
        elif mappings:
            logging.info(f"  All {len(mappings)} mappings on page {page_number} had empty tags - skipping")
        else:
            logging.info(f"  No mappings found on page {page_number}")
        
        # Update cursor for next iteration
        cursor = next_cursor
        page_number += 1
        
        # If no more data, break the loop
        if not has_more:
            break
    
    # Log final summary with time taken
    elapsed_time = time.time() - start_time
    logging.info(f"\n  No more pages available. Total mappings processed: {total_mappings}")
    logging.info(f"  Total successful resources sent to Ranger: {total_resources}")
    logging.info(f"  Time taken: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    
    return total_mappings, total_resources


def retry_failed_batches_from_json(
    failed_batches_file: str,
    ranger_url: str,
    max_retries: int = 5,
    session: Optional[requests.Session] = None,
    ranger_username: Optional[str] = None,
    ranger_password: Optional[str] = None
) -> Tuple[int, int]:
    """
    Retry failed batches from a JSON file.
    
    Args:
        failed_batches_file: Path to JSON file containing failed batches
        ranger_url: Base URL of the Ranger Admin server
        max_retries: Maximum number of retry attempts (default: 5)
        session: Optional requests Session object
        ranger_username: Optional username for Ranger basic authentication
        ranger_password: Optional password for Ranger basic authentication
    
    Returns:
        Tuple of (successful_count, failed_count)
    """
    if not os.path.exists(failed_batches_file):
        logging.warning(f"  ⚠️  Failed batches file not found: {failed_batches_file}")
        return 0, 0
    
    try:
        with open(failed_batches_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        logging.error(f"  ❌ Failed to read failed batches file {failed_batches_file}: {e}")
        return 0, 0
    
    failed_batches = data.get('failed_batches', [])
    if not failed_batches:
        logging.info(f"  ✅ No failed batches to retry in {failed_batches_file}")
        return 0, 0
    
    logging.info(f"\n  Retrying {len(failed_batches)} failed batches from {failed_batches_file}...")
    
    successful_count = 0
    still_failed_batches = []
    
    # Retry each batch up to max_retries times
    for batch_info in failed_batches:
        batch_num = batch_info.get('batch_num', 0)
        mappings_count = batch_info.get('mappings_count', 0)
        payload = batch_info.get('payload')
        original_error = batch_info.get('error', 'Unknown error')
        
        if payload is None:
            logging.error(f"  ❌ Batch {batch_num}: Cannot retry - payload is None (original error: {original_error})")
            still_failed_batches.append(batch_info)
            continue
        
        retry_attempt = 0
        success = False
        
        while retry_attempt < max_retries and not success:
            retry_attempt += 1
            
            # Calculate exponential backoff delay: 2^retry_attempt seconds (max 30 seconds)
            if retry_attempt > 1:
                delay = min(2 ** retry_attempt, 30)
                logging.info(f"  Waiting {delay} seconds before retry attempt {retry_attempt} for batch {batch_num}...")
                time.sleep(delay)
            
            try:
                logging.info(f"  Retrying Batch {batch_num} ({mappings_count} mappings) - attempt {retry_attempt}/{max_retries}...")
                response = send_service_tags_to_ranger(
                    ranger_url,
                    payload,
                    session=session,
                    ranger_username=ranger_username,
                    ranger_password=ranger_password
                )
                logging.info(f"  ✅ Batch {batch_num} retry successful after {retry_attempt} attempt(s)")
                successful_count += 1
                success = True
            except Exception as e:
                error_msg = str(e)
                logging.error(f"  ❌ Batch {batch_num} retry attempt {retry_attempt} failed: {e}")
                if retry_attempt == max_retries:
                    # Update error message with latest error
                    batch_info['error'] = error_msg
                    batch_info['last_retry_attempt'] = retry_attempt
                    still_failed_batches.append(batch_info)
        
        if not success:
            logging.error(f"  ❌ Batch {batch_num} failed after {max_retries} retry attempts")
    
    # Update the JSON file with batches that still failed
    if still_failed_batches:
        try:
            data['failed_batches'] = still_failed_batches
            data['last_retry_at'] = datetime.now().isoformat()
            data['retry_summary'] = {
                'successful': successful_count,
                'still_failed': len(still_failed_batches),
                'total_attempted': len(failed_batches)
            }
            with open(failed_batches_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)
            logging.warning(f"  ⚠️  Updated {failed_batches_file} with {len(still_failed_batches)} batches that still failed")
        except Exception as save_error:
            logging.error(f"  ❌ Failed to update failed batches file: {save_error}")
    else:
        # All batches succeeded, rename the file to indicate success
        try:
            # Keep file in the same directory (failed_batches/)
            file_dir = os.path.dirname(failed_batches_file)
            file_basename = os.path.basename(failed_batches_file)
            success_file = os.path.join(file_dir, file_basename.replace('.json', '_retried_successfully.json'))
            os.rename(failed_batches_file, success_file)
            logging.info(f"  ✅ All batches succeeded! Renamed file to {os.path.basename(success_file)}")
        except Exception as rename_error:
            logging.warning(f"  ⚠️  All batches succeeded but could not rename file: {rename_error}")
    
    return successful_count, len(still_failed_batches)


# Cache for service resource hierarchies to avoid repeated API calls
_service_hierarchy_cache: Dict[str, List[str]] = {}


def fetch_ranger_service_definition(
    ranger_url: str,
    service_type: str,
    session: Optional[requests.Session] = None,
    ranger_username: Optional[str] = None,
    ranger_password: Optional[str] = None
) -> Dict[str, Any]:
    """
    Fetch service definition from Ranger Admin API.
    
    Args:
        ranger_url: Base URL of the Ranger Admin server
        service_type: Service type (e.g., DATABRICKS_UNITY_CATALOG, SNOWFLAKE)
        session: Optional requests Session object
        ranger_username: Optional username for basic authentication
        ranger_password: Optional password for basic authentication
    
    Returns:
        Service definition dictionary containing resources array
    
    Raises:
        requests.RequestException: If the API call fails
        ValueError: If the response is invalid
    """
    # Ensure ranger_url doesn't end with a slash
    ranger_url = ranger_url.rstrip('/')
    
    # Construct the API endpoint
    endpoint = f"{ranger_url}/service/plugins/definitions/name/{service_type}"
    
    # Prepare request
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    # Prepare auth if credentials are provided
    auth = None
    if ranger_username and ranger_password:
        auth = (ranger_username, ranger_password)
    
    logging.debug(f"\n[RANGER API REQUEST - Service Definition]")
    logging.debug(f"  URL: {endpoint}")
    logging.debug(f"  Method: GET")
    if auth:
        logging.debug(f"  Authentication: Basic Auth (username: {ranger_username})")
    
    try:
        # Use session if provided, otherwise use requests directly
        if session is not None:
            response = session.get(
                endpoint,
                headers=headers,
                auth=auth,
                timeout=30,
                allow_redirects=False
            )
        else:
            response = requests.get(
                endpoint,
                headers=headers,
                auth=auth,
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
        
        response.raise_for_status()
        
        # Parse JSON response
        response_json = response.json()
        logging.debug(f"  Status Code: {response.status_code}")
        logging.debug(f"  Successfully fetched service definition for {service_type}")
        
        return response_json
            
    except requests.exceptions.RequestException as e:
        response_status = 'N/A'
        response_text = ''
        if hasattr(e, 'response') and e.response:
            response_status = e.response.status_code
            try:
                response_text = e.response.text[:200]
            except:
                pass
        
        error_msg = (
            f"Failed to fetch service definition: {e}\n"
            f"URL: {endpoint}\n"
            f"Status Code: {response_status}"
        )
        if response_text:
            error_msg += f"\nResponse: {response_text}"
        
        raise requests.exceptions.RequestException(error_msg)


def build_resource_hierarchy_from_service_definition(
    service_definition: Dict[str, Any]
) -> List[str]:
    """
    Build resource hierarchy list from Ranger service definition.
    
    The hierarchy is built by ordering resources by their 'level' field,
    ensuring parent-child relationships are respected.
    
    Args:
        service_definition: Service definition dictionary from Ranger API
    
    Returns:
        List of resource names ordered by hierarchy level (e.g., ['catalog', 'schema', 'table', 'column'])
    """
    resources = service_definition.get('resources', [])
    
    if not resources:
        logging.warning("  No resources found in service definition, using default hierarchy")
        return ['database', 'schema', 'table', 'column']
    
    # Sort resources by level (ascending order)
    sorted_resources = sorted(resources, key=lambda r: r.get('level', 999))
    
    # Extract resource names in order
    hierarchy = [resource.get('name') for resource in sorted_resources if resource.get('name')]
    
    logging.debug(f"  Built resource hierarchy: {hierarchy}")
    return hierarchy


def get_resource_hierarchy(
    ranger_url: str,
    service_type: str,
    session: Optional[requests.Session] = None,
    ranger_username: Optional[str] = None,
    ranger_password: Optional[str] = None
) -> List[str]:
    """
    Get resource hierarchy for a service type, using cache if available.
    
    Args:
        ranger_url: Base URL of the Ranger Admin server
        service_type: Service type (e.g., DATABRICKS_UNITY_CATALOG, SNOWFLAKE)
        session: Optional requests Session object
        ranger_username: Optional username for basic authentication
        ranger_password: Optional password for basic authentication
    
    Returns:
        List of resource names ordered by hierarchy level
    """
    # Check cache first
    cache_key = f"{ranger_url}:{service_type}"
    if cache_key in _service_hierarchy_cache:
        logging.debug(f"  Using cached hierarchy for {service_type}: {_service_hierarchy_cache[cache_key]}")
        return _service_hierarchy_cache[cache_key]
    
    # Fetch from Ranger API
    logging.debug(f"  Fetching resource hierarchy for service type: {service_type}")
    try:
        service_definition = fetch_ranger_service_definition(
            ranger_url,
            service_type,
            session=session,
            ranger_username=ranger_username,
            ranger_password=ranger_password
        )
        
        hierarchy = build_resource_hierarchy_from_service_definition(service_definition)
        
        # Cache the hierarchy
        _service_hierarchy_cache[cache_key] = hierarchy
        
        logging.debug(f"  ✅ Successfully fetched hierarchy for {service_type}: {hierarchy}")
        return hierarchy
        
    except Exception as e:
        logging.warning(f"  ⚠️  Failed to fetch hierarchy for {service_type}: {e}")
        logging.warning(f"  Falling back to default hierarchy: ['database', 'schema', 'table', 'column']")
        # Fallback to default hierarchy
        default_hierarchy = ['database', 'schema', 'table', 'column']
        _service_hierarchy_cache[cache_key] = default_hierarchy
        return default_hierarchy


def parse_resource_path(
    resource_path: str,
    resource_type: str,
    service_type: Optional[str] = None,
    hierarchy: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Parse a resource path into Ranger resource elements.
    
    Args:
        resource_path: Resource path (e.g., "OMNI_TEST/SALES_SCHEMA1/SALES_DATA")
        resource_type: Resource type (e.g., "TABLE", "COLUMN")
        service_type: Optional service type for custom hierarchy mapping (deprecated, use hierarchy parameter)
        hierarchy: Optional list of resource names ordered by hierarchy level (e.g., ['catalog', 'schema', 'table', 'column'])
                   If not provided, defaults to ['database', 'schema', 'table', 'column']
    
    Returns:
        Resource elements dictionary
    
    Examples:
        "OMNI_TEST/SALES_SCHEMA1/SALES_DATA" (TABLE) with hierarchy ['database', 'schema', 'table'] ->
        {
            "database": {"values": ["OMNI_TEST"]},
            "schema": {"values": ["SALES_SCHEMA1"]},
            "table": {"values": ["SALES_DATA"]}
        }
        
        "catalog1/schema1/table1/col1" (COLUMN) with hierarchy ['catalog', 'schema', 'table', 'column'] ->
        {
            "catalog": {"values": ["catalog1"]},
            "schema": {"values": ["schema1"]},
            "table": {"values": ["table1"]},
            "column": {"values": ["col1"]}
        }
    """
    parts = [p for p in resource_path.split('/') if p.strip()]  # Remove empty parts
    resource_elements = {}
    
    # Use provided hierarchy or fallback to default
    if hierarchy is None:
        # Default hierarchy for backward compatibility
        hierarchy = ['database', 'schema', 'table', 'column']
        if service_type:
            logging.debug(f"  Using default hierarchy for service_type={service_type}. Consider fetching dynamic hierarchy.")
    
    # Map parts to hierarchy levels
    for i, part in enumerate(parts):
        if i < len(hierarchy):
            resource_elements[hierarchy[i]] = {"values": [part]}
    
    return resource_elements


def convert_tag_definition_to_ranger(
    omni_tag_def: Dict[str, Any],
    tag_def_id: int
) -> Tuple[int, Dict[str, Any]]:
    """
    Convert an Omni tag definition to Ranger TagDef format.
    
    Args:
        omni_tag_def: Omni tag definition object
        tag_def_id: ID to assign to the tag definition
    
    Returns:
        Tuple of (tag_def_id, RangerTagDef object)
    """
    tag_name = omni_tag_def.get('tagName', '')
    source = omni_tag_def.get('source', 'Internal')
    
    # Convert attributeDefinitions from Omni format to Ranger format
    attribute_defs = []
    omni_attr_defs = omni_tag_def.get('attributeDefinitions', [])
    
    for attr_def in omni_attr_defs:
        if isinstance(attr_def, dict):
            attr_name = attr_def.get('key') or attr_def.get('name', '')
            attr_type = attr_def.get('type', 'string')
            if attr_name:
                attribute_defs.append({
                    "name": attr_name,
                    "type": attr_type
                })
    
    ranger_tag_def = {
        "id": tag_def_id,
        "name": tag_name,
        "source": source
    }
    
    if attribute_defs:
        ranger_tag_def["attributeDefs"] = attribute_defs
    
    return tag_def_id, ranger_tag_def


def convert_tag_to_ranger(
    tag_name: str,
    tag_attributes: Dict[str, Any],
    tag_id: int
) -> Tuple[int, Dict[str, Any]]:
    """
    Convert an Omni tag (with attributes) to Ranger Tag format.
    
    Args:
        tag_name: Tag name
        tag_attributes: Tag attributes dictionary
        tag_id: ID to assign to the tag
    
    Returns:
        Tuple of (tag_id, RangerTag object)
    """
    # Convert attributes to string values (Ranger expects Map<String, String>)
    attributes = {}
    if tag_attributes:
        for key, value in tag_attributes.items():
            attributes[key] = str(value) if value is not None else ""
    
    ranger_tag = {
        "id": tag_id,
        "type": tag_name,
        "owner": 0,  # OWNER_SERVICERESOURCE
        "attributes": attributes if attributes else None
    }
    
    return tag_id, ranger_tag


def build_service_tags_payload(
    service_name: str,
    tag_definitions: List[Dict[str, Any]],
    tag_resource_mappings: List[Dict[str, Any]],
    service_type: Optional[str] = None,
    ranger_url: Optional[str] = None,
    session: Optional[requests.Session] = None,
    ranger_username: Optional[str] = None,
    ranger_password: Optional[str] = None
) -> Dict[str, Any]:
    """
    Build a ServiceTags payload for Ranger API.
    
    Args:
        service_name: Service name
        tag_definitions: List of Omni tag definitions
        tag_resource_mappings: List of Omni tag-resource mappings
        service_type: Optional service type (e.g., DATABRICKS_UNITY_CATALOG, SNOWFLAKE) for dynamic hierarchy
        ranger_url: Optional Ranger URL for fetching service definition (required if service_type is provided)
        session: Optional requests Session object
        ranger_username: Optional username for Ranger basic authentication
        ranger_password: Optional password for Ranger basic authentication
    
    Returns:
        ServiceTags payload dictionary
    """
    # Fetch resource hierarchy if service_type is provided
    hierarchy = None
    if service_type and ranger_url:
        try:
            hierarchy = get_resource_hierarchy(
                ranger_url,
                service_type,
                session=session,
                ranger_username=ranger_username,
                ranger_password=ranger_password
            )
        except Exception as e:
            logging.warning(f"  Failed to fetch hierarchy for {service_type}, using default: {e}")
            hierarchy = None
    
    # Maps to store tag definitions and tags with their IDs
    tag_definitions_map: Dict[int, Dict[str, Any]] = {}  # tag_def_id -> RangerTagDef
    tag_name_to_def_id: Dict[str, int] = {}  # tag_name -> tag_def_id
    tags_map: Dict[int, Dict[str, Any]] = {}  # tag_id -> RangerTag
    tag_key_to_id: Dict[Tuple[str, str], int] = {}  # (tag_name, attr_signature) -> tag_id
    
    # Maps for resources
    service_resources: List[Dict[str, Any]] = []
    resource_key_to_index: Dict[str, int] = {}  # resource_path -> resource_index
    resource_to_tag_ids: Dict[str, List[int]] = {}  # resource_id (as string) -> [tag_id, ...]
    
    # Generate IDs
    next_tag_def_id = 1
    next_tag_id = 1
    next_resource_id = 1  # Start from 1 for service resources
    
    # Step 1: Process tag definitions
    for omni_tag_def in tag_definitions:
        tag_name = omni_tag_def.get('tagName', '')
        if not tag_name:
            continue
        
        tag_def_id, ranger_tag_def = convert_tag_definition_to_ranger(omni_tag_def, next_tag_def_id)
        tag_definitions_map[tag_def_id] = ranger_tag_def
        tag_name_to_def_id[tag_name] = tag_def_id
        next_tag_def_id += 1
    
    # Step 2: Process tag-resource mappings
    for mapping in tag_resource_mappings:
        resource_path = mapping.get('resourcePath', '')
        resource_type = mapping.get('resourceType', '')
        tags = mapping.get('tags', [])
        
        if not resource_path or not tags:
            continue
        
        # Parse resource path into resource elements using dynamic hierarchy
        resource_elements = parse_resource_path(resource_path, resource_type, service_type, hierarchy)
        
        # Get or create service resource
        if resource_path not in resource_key_to_index:
            service_resource = {
                "id": next_resource_id,
                "serviceName": service_name,
                "resourceElements": resource_elements
            }
            service_resources.append(service_resource)
            resource_key_to_index[resource_path] = next_resource_id
            resource_to_tag_ids[str(next_resource_id)] = []
            next_resource_id += 1
        
        resource_id = resource_key_to_index[resource_path]
        resource_id_str = str(resource_id)
        
        # Process each tag for this resource
        for tag_obj in tags:
            tag_name = tag_obj.get('tagName', '')
            tag_attributes = tag_obj.get('attributes', {})
            
            if not tag_name:
                continue
            
            # Create a signature for the tag (name + sorted attributes)
            attr_signature = json.dumps(sorted(tag_attributes.items()), sort_keys=True) if tag_attributes else ""
            tag_key = (tag_name, attr_signature)
            
            # Get or create tag
            if tag_key not in tag_key_to_id:
                tag_id, ranger_tag = convert_tag_to_ranger(tag_name, tag_attributes, next_tag_id)
                tags_map[tag_id] = ranger_tag
                tag_key_to_id[tag_key] = tag_id
                next_tag_id += 1
            
            tag_id = tag_key_to_id[tag_key]
            
            # Add tag to resource
            if tag_id not in resource_to_tag_ids[resource_id_str]:
                resource_to_tag_ids[resource_id_str].append(tag_id)
  
    
    # Convert tagDefinitions and tags maps to use string keys (as required by Ranger)
    tag_definitions_dict = {str(k): v for k, v in tag_definitions_map.items()}
    tags_dict = {str(k): v for k, v in tags_map.items()}
    
    service_tags = {
        "op": "add_or_update",
        "serviceName": service_name,
        "tagDefinitions": tag_definitions_dict,
        "tags": tags_dict,
        "serviceResources": service_resources,
        "resourceToTagIds": resource_to_tag_ids,
        "isDelta": False
    }
    
    return service_tags


def split_service_tags_into_batches(
    service_tags: Dict[str, Any],
    batch_size: int = 10
) -> List[Dict[str, Any]]:
    """
    Split a ServiceTags payload into batches of resources.
    
    Each batch will contain at most `batch_size` resources, along with their
    associated tags and tag definitions.
    
    Args:
        service_tags: Full ServiceTags payload dictionary
        batch_size: Number of resources per batch (default: 10)
    
    Returns:
        List of ServiceTags payload dictionaries, each containing at most batch_size resources
    """
    service_resources = service_tags.get('serviceResources', [])
    
    # If no resources or fewer than batch_size, return original payload
    if not service_resources or len(service_resources) <= batch_size:
        return [service_tags]
    
    batches = []
    total_resources = len(service_resources)
    
    # Split resources into batches
    for i in range(0, total_resources, batch_size):
        batch_resources = service_resources[i:i + batch_size]
        batch_resource_ids = {str(res['id']) for res in batch_resources}
        
        # Extract resourceToTagIds for this batch
        batch_resource_to_tag_ids = {
            resource_id: tag_ids
            for resource_id, tag_ids in service_tags.get('resourceToTagIds', {}).items()
            if resource_id in batch_resource_ids
        }
        
        # Collect all tag IDs used in this batch
        # Convert to strings to match the string keys in service_tags.get('tags', {})
        batch_tag_ids = set()
        for tag_ids in batch_resource_to_tag_ids.values():
            batch_tag_ids.update(str(tag_id) for tag_id in tag_ids)
        
        # Extract tags used in this batch
        batch_tags = {
            tag_id: tag
            for tag_id, tag in service_tags.get('tags', {}).items()
            if tag_id in batch_tag_ids
        }
        
        # Extract tag definitions used in this batch
        # Tag definitions are referenced by tag name in tags
        batch_tag_def_ids = set()
        for tag in batch_tags.values():
            tag_type = tag.get('type', '')
            # Find tag definition ID by tag name
            for def_id, tag_def in service_tags.get('tagDefinitions', {}).items():
                if tag_def.get('name') == tag_type:
                    batch_tag_def_ids.add(def_id)
                    break
        
        batch_tag_definitions = {
            def_id: tag_def
            for def_id, tag_def in service_tags.get('tagDefinitions', {}).items()
            if def_id in batch_tag_def_ids
        }
        
        # Build batch payload
        batch_payload = {
            "op": service_tags.get('op', 'add_or_update'),
            "serviceName": service_tags.get('serviceName'),
            "tagDefinitions": batch_tag_definitions,
            "tags": batch_tags,
            "serviceResources": batch_resources,
            "resourceToTagIds": batch_resource_to_tag_ids,
            "isDelta": service_tags.get('isDelta', False)
        }
        
        batches.append(batch_payload)
    
    return batches


def send_service_tags_batches_in_parallel(
    ranger_url: str,
    service_tags: Dict[str, Any],
    batch_size: int = 10,
    max_workers: int = 5,
    session: Optional[requests.Session] = None,
    ranger_username: Optional[str] = None,
    ranger_password: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Split ServiceTags payload into batches and send them to Ranger in parallel.
    
    Args:
        ranger_url: Base URL of the Ranger Admin server
        service_tags: Full ServiceTags payload dictionary
        batch_size: Number of resources per batch (default: 10)
        max_workers: Maximum number of parallel threads (default: 5)
        session: Optional requests Session object for cookie handling
        ranger_username: Optional username for basic authentication
        ranger_password: Optional password for basic authentication
    
    Returns:
        List of response dictionaries from each batch
    
    Raises:
        requests.RequestException: If any API call fails
    """
    # Split into batches
    batches = split_service_tags_into_batches(service_tags, batch_size)
    total_batches = len(batches)
    total_resources = len(service_tags.get('serviceResources', []))
    
    logging.info(f"  Splitting {total_resources} resources into {total_batches} batches (max {batch_size} resources per batch)")
    
    responses = []
    errors = []
    
    # Send batches in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all batches
        future_to_batch = {
            executor.submit(
                send_service_tags_to_ranger,
                ranger_url,
                batch,
                session,
                ranger_username,
                ranger_password
            ): (i + 1, batch)
            for i, batch in enumerate(batches)
        }
        
        # Process completed batches
        for future in as_completed(future_to_batch):
            batch_num, batch = future_to_batch[future]
            batch_resources = len(batch.get('serviceResources', []))
            try:
                response = future.result()
                responses.append(response)
                logging.info(f"  ✅ Batch {batch_num}/{total_batches} ({batch_resources} resources) sent successfully")
            except Exception as e:
                errors.append((batch_num, batch_resources, str(e)))
                logging.error(f"  ❌ Batch {batch_num}/{total_batches} ({batch_resources} resources) failed: {e}")
    
    # If there were errors, raise an exception with details
    if errors:
        error_details = "\n".join([
            f"  Batch {num} ({resources} resources): {error}"
            for num, resources, error in errors
        ])
        raise requests.exceptions.RequestException(
            f"Failed to send {len(errors)} out of {total_batches} batches:\n{error_details}"
        )
    
    return responses


def send_service_tags_to_ranger(
    ranger_url: str,
    service_tags: Dict[str, Any],
    session: Optional[requests.Session] = None,
    ranger_username: Optional[str] = None,
    ranger_password: Optional[str] = None
) -> Dict[str, Any]:
    """
    Send ServiceTags payload to Ranger Admin API.
    
    Args:
        ranger_url: Base URL of the Ranger Admin server (e.g., http://localhost:6080)
        service_tags: ServiceTags payload dictionary
        session: Optional requests Session object for cookie handling
        ranger_username: Optional username for basic authentication
        ranger_password: Optional password for basic authentication
    
    Returns:
        JSON response as a dictionary
    
    Raises:
        requests.RequestException: If the API call fails
        ValueError: If the response is invalid
    """
    # Ensure ranger_url doesn't end with a slash
    ranger_url = ranger_url.rstrip('/')
    
    # Construct the API endpoint
    endpoint = f"{ranger_url}/service/tags/importservicetags"
    
    # Prepare request
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    # Prepare auth if credentials are provided
    auth = None
    if ranger_username and ranger_password:
        auth = (ranger_username, ranger_password)
        logging.debug(f"\n[RANGER API REQUEST]")
        logging.debug(f"  URL: {endpoint}")
        logging.debug(f"  Method: PUT")
        logging.debug(f"  Authentication: Basic Auth (username: {ranger_username})")
        logging.debug(f"  Headers: {json.dumps(headers, indent=2)}")
    else:
        logging.debug(f"\n[RANGER API REQUEST]")
        logging.debug(f"  URL: {endpoint}")
        logging.debug(f"  Method: PUT")
        logging.debug(f"  Authentication: None")
        logging.debug(f"  Headers: {json.dumps(headers, indent=2)}")
    
    # Payload logging removed to reduce log size
    payload_size = len(json.dumps(service_tags, default=str))
    logging.debug(f"  Payload Size: {payload_size} bytes")
    
    try:
        # Use session if provided, otherwise use requests directly
        if session is not None:
            response = session.put(
                endpoint,
                headers=headers,
                json=service_tags,
                auth=auth,
                timeout=60,
                allow_redirects=False
            )
        else:
            response = requests.put(
                endpoint,
                headers=headers,
                json=service_tags,
                auth=auth,
                timeout=60,
                allow_redirects=False
            )
        
        # Handle redirects
        if response.status_code in (301, 302, 303, 307, 308):
            location = response.headers.get('Location', '')
            raise ValueError(
                f"Server redirected to: {location}\n"
                f"This usually means authentication is required."
            )
        
        # Get response text and try to parse JSON
        response_text = response.text.strip()
        response_json = None
        try:
            response_json = response.json()
        except json.JSONDecodeError:
            response_json = None
        
        # Log response details (payload removed to reduce log size)
        logging.debug(f"\n[RANGER API RESPONSE]")
        logging.debug(f"  Status Code: {response.status_code}")
        logging.debug(f"  Headers: {json.dumps(dict(response.headers), indent=2)}")
        if response_json is not None:
            response_size = len(json.dumps(response_json, default=str))
            logging.debug(f"  Response Body (JSON): {response_size} bytes")
        else:
            response_size = len(response_text)
            logging.debug(f"  Response Body (Text): {response_size} bytes")
        
        # Check if response is successful
        # HTTP 204 (No Content) is a success status code indicating the request succeeded
        # but there's no content to return - this is common for PUT operations
        if response.status_code in (200, 201, 204):
            if response_json is not None:
                return response_json
            else:
                return {"status": "success", "statusCode": response.status_code}
        
        # For error responses, try to get JSON error details
        error_details = {}
        if response_json is not None:
            error_details = response_json
        else:
            error_details = {"message": response_text[:500] if len(response_text) > 500 else response_text}
        
        raise requests.exceptions.HTTPError(
            f"Failed to import service tags: HTTP {response.status_code}\n"
            f"Response: {error_details}"
        )
            
    except requests.exceptions.RequestException as e:
        error_msg = (
            f"Failed to send service tags to Ranger: {e}\n"
            f"URL: {endpoint}"
        )
        raise requests.exceptions.RequestException(error_msg)


def build_omni_url(api_key: str, base_url: str) -> str:
    """
    Build the full Omni Metadata Service URL from API key.
    
    Args:
        api_key: API key for authentication
        base_url: Base URL for Privacera Cloud API
    
    Returns:
        Full Omni Metadata Service URL
    """
    # Construct the full URL: {base_url}/{api_key}/omni-metadata
    omni_url = f"{base_url}/{api_key}/omni-metadata"
    
    return omni_url


def get_failed_batches_directory() -> str:
    """
    Get the directory path for storing failed batches.
    
    Returns:
        Path to the failed_batches directory
    """
    # Use a directory relative to the script location or current working directory
    script_dir = os.path.dirname(os.path.abspath(__file__)) if os.path.dirname(os.path.abspath(__file__)) else os.getcwd()
    failed_batches_dir = os.path.join(script_dir, 'failed_batches')
    
    # Create directory if it doesn't exist
    if not os.path.exists(failed_batches_dir):
        try:
            os.makedirs(failed_batches_dir, exist_ok=True)
        except Exception as e:
            # Use print instead of logging in case logging isn't initialized yet
            try:
                logging.warning(f"Could not create failed_batches directory: {e}")
            except:
                print(f"Warning: Could not create failed_batches directory: {e}")
            # Fallback to current directory
            failed_batches_dir = os.getcwd()
    
    return failed_batches_dir


def clear_failed_batches_directory() -> None:
    """
    Clear all files from the failed batches directory.
    
    This function removes all JSON files from the failed_batches directory,
    but preserves the directory itself.
    """
    failed_batches_dir = get_failed_batches_directory()
    
    if not os.path.exists(failed_batches_dir):
        return
    
    try:
        files_removed = 0
        for filename in os.listdir(failed_batches_dir):
            file_path = os.path.join(failed_batches_dir, filename)
            if os.path.isfile(file_path) and filename.endswith('.json'):
                try:
                    os.remove(file_path)
                    files_removed += 1
                    logging.debug(f"Removed failed batch file: {filename}")
                except Exception as e:
                    logging.warning(f"Could not remove file {filename}: {e}")
        
        if files_removed > 0:
            logging.info(f"Cleared {files_removed} file(s) from failed_batches directory")
    except Exception as e:
        logging.warning(f"Could not clear failed_batches directory: {e}")


def build_ranger_url(api_key: str, base_url: str) -> str:
    """
    Build the full Ranger Admin Service URL from API key.
    
    Args:
        api_key: API key for authentication
        base_url: Base URL for Privacera Cloud API
    
    Returns:
        Full Ranger Admin Service URL
    """
    # Construct the full URL: {base_url}/{api_key}
    ranger_url = f"{base_url}/{api_key}"
    
    return ranger_url


def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(
        description='Download tags and tag-resource mappings from Omni Metadata Service and push them to Ranger',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using API key (default base URL: https://api.privaceracloud.com/api):
  python3 omni_tag_to_ranger_migrate.py \\
    --api-key 620071085fa8135a90e23482d437ac0c83bd5a15716995fc6402c212cda9cbc8 \\
    --service-name privacera_snowflake \\
    --service-type SNOWFLAKE
  
  # Using custom base URL:
  python3 omni_tag_to_ranger_migrate.py \\
    --api-key 620071085fa8135a90e23482d437ac0c83bd5a15716995fc6402c212cda9cbc8 \\
    --base-url https://custom-api.example.com/api \\
    --service-name privacera_snowflake \\
    --service-type SNOWFLAKE
  
  # Using environment variables from .env file:
  python3 omni_tag_to_ranger_migrate.py \\
    --service-name privacera_snowflake \\
    --service-type SNOWFLAKE
  
  # Using full URLs (legacy support):
  python3 omni_tag_to_ranger_migrate.py \\
    --omni-url https://api.privaceracloud.com/api/620071085fa8135a90e23482d437ac0c83bd5a15716995fc6402c212cda9cbc8/omni-metadata \\
    --ranger-url https://api.privaceracloud.com/api/620071085fa8135a90e23482d437ac0c83bd5a15716995fc6402c212cda9cbc8/service \\
    --service-name privacera_snowflake \\
    --service-type SNOWFLAKE
  
  # Save payload to file:
  python3 omni_tag_to_ranger_migrate.py \\
    --api-key 620071085fa8135a90e23482d437ac0c83bd5a15716995fc6402c212cda9cbc8 \\
    --service-name privacera_snowflake \\
    --service-type SNOWFLAKE \\
    --save-payload payload.json
  
  # Specify custom log file and log level:
  python3 omni_tag_to_ranger_migrate.py \\
    --api-key 620071085fa8135a90e23482d437ac0c83bd5a15716995fc6402c212cda9cbc8 \\
    --service-name privacera_snowflake \\
    --service-type SNOWFLAKE \\
    --log-file /path/to/migration.log \\
    --log-level DEBUG
  
  # Retry only failed batches (skip main migration):
  python3 omni_tag_to_ranger_migrate.py \\
    --retry-failed-only \\
    --api-key 620071085fa8135a90e23482d437ac0c83bd5a15716995fc6402c212cda9cbc8 \\
    --ranger-username admin \\
    --ranger-password admin

Environment Variables (in .env file):
  API_KEY=620071085fa8135a90e23482d437ac0c83bd5a15716995fc6402c212cda9cbc8
  BASE_URL=https://api.privaceracloud.com/api (optional, defaults to https://api.privaceracloud.com/api)
  RANGER_USERNAME=admin (optional, for Ranger basic auth)
  RANGER_PASSWORD=admin (optional, for Ranger basic auth)
  LOG_FILE=/path/to/migration.log (optional, for custom log file path)
  LOG_LEVEL=DEBUG (optional, for logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL)
  
Note: BASE_URL can be set via --base-url argument, BASE_URL environment variable, or defaults to https://api.privaceracloud.com/api
      You only need to provide your API_KEY which will be appended to this base URL.
        """
    )
    
    parser.add_argument(
        '--omni-url',
        help='Full URL of the Omni Metadata Service (legacy option, use --api-key instead)'
    )
    
    parser.add_argument(
        '--api-key',
        help='API key for authentication. The base URL https://api.privaceracloud.com/api/ is hardcoded. Can also be set via API_KEY env var.'
    )
    
    parser.add_argument(
        '--base-url',
        help='Base URL for Privacera Cloud API (defaults to https://api.privaceracloud.com/api). Can also be set via BASE_URL env var.'
    )
    
    parser.add_argument(
        '--ranger-url',
        help='Base URL of the Ranger Admin server (optional, will be built from hardcoded base URL and --api-key if not provided)'
    )
    
    parser.add_argument(
        '--service-name',
        required=False,
        help='Service name (e.g., privacera_snowflake). Required unless --retry-failed-only is used.'
    )
    
    parser.add_argument(
        '--service-type',
        required=False,
        help='Service type (e.g., SNOWFLAKE, HIVE). Required unless --retry-failed-only is used.'
    )
    
    parser.add_argument(
        '--save-payload',
        help='Optional: Save the ServiceTags payload to a JSON file before sending to Ranger'
    )
    
    parser.add_argument(
        '--ranger-username',
        help='Ranger API username for basic authentication. Can also be set via RANGER_USERNAME env var.'
    )
    
    parser.add_argument(
        '--ranger-password',
        help='Ranger API password for basic authentication. Can also be set via RANGER_PASSWORD env var.'
    )
    
    parser.add_argument(
        '--log-file',
        help='Path to log file. If not specified, a timestamped log file will be created in the current directory. Can also be set via LOG_FILE env var.'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default=None,
        help='Logging level. Can also be set via LOG_LEVEL env var. Default: INFO'
    )
    
    parser.add_argument(
        '--retry-failed-only',
        action='store_true',
        help='Skip main migration and only retry failed batches from failed_batches/ directory. Can also be set via RETRY_FAILED_ONLY env var (true/false).'
    )
    
    args = parser.parse_args()
    
    # Check environment variable for retry-failed-only flag
    retry_failed_only_env = os.getenv('RETRY_FAILED_ONLY', '').lower()
    if retry_failed_only_env in ('true', '1', 'yes'):
        args.retry_failed_only = True
    
    # Get log file from command-line argument or environment variable
    log_file = args.log_file or os.getenv('LOG_FILE')
    
    # Get log level from command-line argument, environment variable, or default
    log_level = args.log_level or os.getenv('LOG_LEVEL', 'INFO')
    
    # Validate log level
    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    if log_level.upper() not in valid_levels:
        # Use print here since logging isn't initialized yet
        print(f"Warning: Invalid log level '{log_level}', defaulting to INFO", file=sys.stderr)
        log_level = 'INFO'
    else:
        log_level = log_level.upper()
    
    # Initialize logging first
    setup_logging(log_file=log_file, log_level=log_level)
    
    # Record start time for total execution time
    script_start_time = time.time()
    
    try:
        # Determine base URL (priority: --base-url arg > BASE_URL env var > default)
        base_url = args.base_url or os.getenv('BASE_URL') or DEFAULT_BASE_URL
        if base_url != DEFAULT_BASE_URL:
            logging.info(f"Using custom base URL: {base_url}")
        else:
            logging.info(f"Using default base URL: {base_url}")
        
        # Determine Omni URL from various sources
        omni_url = None
        api_key = None
        
        # Priority 1: Use --omni-url if provided (legacy support)
        if args.omni_url:
            omni_url = args.omni_url
            logging.info("Using --omni-url (legacy mode)")
        
        # Priority 2: Build from --api-key
        elif args.api_key:
            api_key = args.api_key
            omni_url = build_omni_url(api_key, base_url)
            logging.info(f"Using --api-key with base URL: {base_url}")
        
        # Priority 3: Build from environment variable
        else:
            api_key = args.api_key or os.getenv('API_KEY')
            
            if api_key:
                omni_url = build_omni_url(api_key, base_url)
                logging.info(f"Using API_KEY environment variable with base URL: {base_url}")
            else:
                raise ValueError(
                    f"Omni URL not specified. Please provide either:\n"
                    f"  1. --omni-url (full URL)\n"
                    f"  2. --api-key\n"
                    f"  3. API_KEY environment variable\n"
                    f"Missing: API_KEY (or --api-key)"
                )
        
        # Determine Ranger URL
        ranger_url = None
        
        # Priority 1: Use --ranger-url if provided
        if args.ranger_url:
            ranger_url = args.ranger_url
            logging.info("Using --ranger-url")
        
        # Priority 2: Build from --api-key (or env var)
        else:
            # Get API key if not already determined
            if not api_key:
                api_key = args.api_key or os.getenv('API_KEY')
            
            if api_key:
                ranger_url = build_ranger_url(api_key, base_url)
                logging.info(f"Building Ranger URL from base URL ({base_url}) and API key")
            else:
                raise ValueError(
                    f"Ranger URL not specified and cannot be built. Please provide either:\n"
                    f"  1. --ranger-url (full URL)\n"
                    f"  2. --api-key (to build Ranger URL)\n"
                    f"  3. API_KEY environment variable\n"
                )
        
        # Create a session to handle cookies
        session = requests.Session()
        
        # Get Ranger authentication credentials
        ranger_username = args.ranger_username or os.getenv('RANGER_USERNAME')
        ranger_password = args.ranger_password or os.getenv('RANGER_PASSWORD')
        
        # Check if we should only retry failed batches
        if args.retry_failed_only:
            logging.info("=" * 80)
            logging.info("RETRY FAILED BATCHES ONLY MODE")
            logging.info("=" * 80)
            logging.info("Skipping main migration. Only retrying failed batches from failed_batches/ directory...")
            logging.info("")
            
            # Determine Ranger URL if not already set
            if not ranger_url:
                # Try to get from environment or args
                if not api_key:
                    api_key = args.api_key or os.getenv('API_KEY')
                
                if api_key:
                    ranger_url = build_ranger_url(api_key, base_url)
                    logging.info(f"Ranger URL: {ranger_url}")
                else:
                    # Will try to get from JSON files
                    logging.info("Ranger URL not provided. Will use URL from failed batch files if available.")
            
            # Step: Retry failed batches from JSON files
            logging.info("Retrying failed batches from failed_batches/ directory...")
            
            # Find all failed_batch_*.json files in failed_batches directory
            failed_batch_files = []
            failed_batches_dir = get_failed_batches_directory()
            try:
                if os.path.exists(failed_batches_dir):
                    for filename in os.listdir(failed_batches_dir):
                        # Support both old format (failed_batches_) and new format (failed_batch_)
                        if (filename.startswith('failed_batch_') or filename.startswith('failed_batches_')) and filename.endswith('.json'):
                            # Skip files that were already successfully retried
                            if not filename.endswith('_retried_successfully.json'):
                                failed_batch_files.append(os.path.join(failed_batches_dir, filename))
            except Exception as e:
                logging.warning(f"  ⚠️  Could not list files in failed_batches directory: {e}")
            
            if not failed_batch_files:
                total_elapsed_time = time.time() - script_start_time
                logging.warning("  ⚠️  No failed batches files found in failed_batches/ directory")
                logging.info("")
                logging.info("=" * 80)
                logging.info(f"TOTAL EXECUTION TIME: {total_elapsed_time:.2f} seconds ({total_elapsed_time/60:.2f} minutes)")
                logging.info("=" * 80)
                return 0
            
            logging.info(f"  Found {len(failed_batch_files)} failed batches file(s) to retry")
            total_successful = 0
            total_still_failed = 0
            
            for failed_file in sorted(failed_batch_files):
                logging.info(f"\n  Processing {os.path.basename(failed_file)}...")
                # Try to get ranger_url from JSON file if not set
                file_ranger_url = ranger_url
                try:
                    with open(failed_file, 'r', encoding='utf-8') as f:
                        file_data = json.load(f)
                        if not file_ranger_url and file_data.get('ranger_url'):
                            file_ranger_url = file_data.get('ranger_url')
                            logging.info(f"  Using Ranger URL from file: {file_ranger_url}")
                except Exception as e:
                    logging.debug(f"  Could not read ranger_url from file: {e}")
                
                if not file_ranger_url:
                    logging.error(f"  ❌ Cannot retry batches: Ranger URL not available")
                    continue
                
                successful, still_failed = retry_failed_batches_from_json(
                    failed_file,
                    file_ranger_url,
                    max_retries=5,
                    session=session,
                    ranger_username=ranger_username,
                    ranger_password=ranger_password
                )
                total_successful += successful
                total_still_failed += still_failed
            
            if total_successful > 0:
                logging.info(f"\n  ✅ Successfully retried {total_successful} batch(es)")
            if total_still_failed > 0:
                logging.warning(f"\n  ⚠️  {total_still_failed} batch(es) still failed after retries")
            else:
                logging.info(f"\n  ✅ All failed batches were successfully retried!")
            
            # Calculate and display total execution time
            total_elapsed_time = time.time() - script_start_time
            logging.info("")
            logging.info("=" * 80)
            logging.info(f"TOTAL EXECUTION TIME: {total_elapsed_time:.2f} seconds ({total_elapsed_time/60:.2f} minutes)")
            logging.info("=" * 80)
            
            return 0
        
        # Validate required arguments for normal migration BEFORE clearing failed batches directory
        # This prevents loss of retry data if validation fails
        if not args.service_name:
            raise ValueError("--service-name is required for normal migration (use --retry-failed-only to skip)")
        if not args.service_type:
            raise ValueError("--service-type is required for normal migration (use --retry-failed-only to skip)")
        
        # Clear failed batches directory when starting a fresh migration (RETRY_FAILED_ONLY is false)
        # Only clear after validation passes to prevent data loss
        logging.info("Clearing failed_batches directory before starting fresh migration...")
        clear_failed_batches_directory()
        logging.info("")
        
        # Log input parameters
        logging.info(f"Omni URL: {omni_url}")
        logging.info(f"Ranger URL: {ranger_url}")
        logging.info(f"Service Name: {args.service_name}")
        logging.info(f"Service Type: {args.service_type}")
        logging.info("")
        
        # Step 1: Fetch tag definitions page by page and send each page to Ranger
        logging.info("Step 1: Fetching tag definitions from Omni and sending to Ranger page by page...")
        total_tags = fetch_and_send_tags_page_by_page(
            omni_url,
            ranger_url,
            args.service_name,
            page_size=20,
            session=session,
            ranger_username=ranger_username,
            ranger_password=ranger_password
        )
        logging.info(f"\n✅ Successfully processed and sent {total_tags} tag definitions to Ranger!")
        
        logging.info("")
        
        # Step 2: Fetch tag-resource mappings page by page and send each page to Ranger
        logging.info("Step 2: Fetching tag-resource mappings from Omni and sending to Ranger page by page...")
        total_mappings, total_resources = fetch_and_send_tag_resource_mappings_page_by_page(
            omni_url,
            ranger_url,
            args.service_name,
            args.service_type,
            session=session,
            ranger_username=ranger_username,
            ranger_password=ranger_password
        )
        logging.info(f"\n✅ Successfully processed and sent {total_mappings} tag-resource mappings to Ranger!")
        logging.info(f"✅ Total successful resources sent to Ranger: {total_resources}")
        
        # Step 3: Retry any failed batches from JSON files
        logging.info("")
        logging.info("Step 3: Retrying failed batches from JSON files...")
        
        # Find all failed_batch_*.json files in failed_batches directory
        failed_batch_files = []
        failed_batches_dir = get_failed_batches_directory()
        try:
            if os.path.exists(failed_batches_dir):
                for filename in os.listdir(failed_batches_dir):
                    # Support both old format (failed_batches_) and new format (failed_batch_)
                    if (filename.startswith('failed_batch_') or filename.startswith('failed_batches_')) and filename.endswith('.json'):
                        # Skip files that were already successfully retried
                        if not filename.endswith('_retried_successfully.json'):
                            failed_batch_files.append(os.path.join(failed_batches_dir, filename))
        except Exception as e:
            logging.warning(f"  ⚠️  Could not list files in failed_batches directory: {e}")
        
        if failed_batch_files:
            logging.info(f"  Found {len(failed_batch_files)} failed batches file(s) to retry")
            total_successful = 0
            total_still_failed = 0
            
            for failed_file in sorted(failed_batch_files):
                logging.info(f"\n  Processing {os.path.basename(failed_file)}...")
                successful, still_failed = retry_failed_batches_from_json(
                    failed_file,
                    ranger_url,
                    max_retries=5,
                    session=session,
                    ranger_username=ranger_username,
                    ranger_password=ranger_password
                )
                total_successful += successful
                total_still_failed += still_failed
            
            if total_successful > 0:
                logging.info(f"\n  ✅ Successfully retried {total_successful} batch(es)")
            if total_still_failed > 0:
                logging.warning(f"\n  ⚠️  {total_still_failed} batch(es) still failed after retries")
            else:
                logging.info(f"\n  ✅ All failed batches were successfully retried!")
        else:
            logging.info("  No failed batches files found to retry")
        
        # Calculate and display total execution time
        total_elapsed_time = time.time() - script_start_time
        logging.info("")
        logging.info("=" * 80)
        logging.info(f"TOTAL EXECUTION TIME: {total_elapsed_time:.2f} seconds ({total_elapsed_time/60:.2f} minutes)")
        logging.info("=" * 80)
        
        return 0
        
    except KeyboardInterrupt:
        total_elapsed_time = time.time() - script_start_time
        logging.warning("\n\nInterrupted by user")
        logging.info("")
        logging.info("=" * 80)
        logging.info(f"TOTAL EXECUTION TIME (before interruption): {total_elapsed_time:.2f} seconds ({total_elapsed_time/60:.2f} minutes)")
        logging.info("=" * 80)
        return 130
    except Exception as e:
        total_elapsed_time = time.time() - script_start_time
        logging.error(f"\n❌ Error: {e}", exc_info=True)
        logging.info("")
        logging.info("=" * 80)
        logging.info(f"TOTAL EXECUTION TIME (before error): {total_elapsed_time:.2f} seconds ({total_elapsed_time/60:.2f} minutes)")
        logging.info("=" * 80)
        return 1


if __name__ == '__main__':
    sys.exit(main())

