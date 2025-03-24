import requests
import json
import os
import re
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# Configuration
API_URL = "https://app.anabix.cz/api"

# List IDs
INDIVIDUAL_DONORS_LIST_ID = 57  # "Individuální dárci"

# Custom field IDs for activities
ACTIVITY_CUSTOM_FIELDS = {
    "amount_gross": 33,  # "Částka Kč brutto"
    "amount_net": 35,    # "Částka Kč netto"
    "support_type": 36,  # "Forma podpory"
    "branch": 37,        # "Pobočka"
    "donor_type": 38     # "Typ dárce"
}

# Custom field values
SUPPORT_TYPE_FINANCIAL = "2"  # "Finanční dary"
BRANCH_TYK = "5"            # "TYK"
DONOR_TYPE_INDIVIDUAL = "2"  # "Fyzická osoba"

# Load environment variables from .env file if it exists
load_dotenv()

# Retrieve username and token from environment variables
username = os.getenv("ANABIX_USERNAME")
token = os.getenv("ANABIX_API_TOKEN")

def get_list_id_from_title(url: str, username: str, token: str, list_title: str) -> Optional[int]:
  """
  Gets the ID of a list based on its title.
  
  Args:
    url (str): The API endpoint URL
    username (str): Username for authentication
    token (str): Authentication token
    list_title (str): The title of the list to find
  
  Returns:
    Optional[int]: The list ID if found, None otherwise
  
  Raises:
    requests.exceptions.RequestException: If the API request fails
  """
  # Prepare request data
  data = {
    "username": username,
    "token": token,
    "requestType": "lists",
    "requestMethod": "getAll",
    "data": {
      "limit": 200,
      "offset": 0,
      "includeMetadata": 1,
      "criteria": {
        "title": list_title
      }
    }
  }
  
  # Make the POST request
  response = requests.post(url, json=data)
  
  # Check if request was successful
  if response.status_code != 200:
    raise requests.exceptions.RequestException(
      f"API request failed with status code {response.status_code}: {response.text}"
    )
  
  # Parse response
  response_data = response.json()
  
  # Check status
  if response_data.get("status") != "SUCCESS":
    return None
  
  # Extract lists data
  lists_data = response_data.get("data", {})
  
  # Find the list with exact title match (case sensitive)
  for list_id, list_info in lists_data.items():
    if list_info.get("title") == list_title:
      return int(list_id)
  
  # Return None if no matching list found
  return None

def download_all_contacts(url: str, username: str, token: str) -> List[Dict]:
  """
  Downloads all contacts from the API using pagination.
  
  Args:
    url (str): The API endpoint URL
    username (str): Username for authentication
    token (str): Authentication token
  
  Returns:
    List[Dict]: List of all contacts
  
  Raises:
    requests.exceptions.RequestException: If any API request fails
  """
  all_contacts = []
  offset = 0
  limit = 200  # Maximum limit per request
  
  while True:
    # Prepare request data
    data = {
      "username": username,
      "token": token,
      "requestType": "contacts",
      "requestMethod": "getAll",
      "data": {
        "limit": limit,
        "offset": offset,
        "includeMetadata": 1
      }
    }
    
    # Make the POST request
    response = requests.post(url, json=data)
    
    # Check if request was successful
    if response.status_code != 200:
      raise requests.exceptions.RequestException(
        f"API request failed with status code {response.status_code}: {response.text}"
      )
    
    # Parse response
    response_data = response.json()
    
    # Extract contacts and metadata
    contacts = response_data.get('data', [])
    metadata = response_data.get('metadata', {})
    
    # Add contacts to the main list
    all_contacts.extend(contacts)
    
    # Check if we've retrieved all contacts
    total_records = metadata.get('totalRecords', 0)
    
    # Calculate next offset
    offset += limit
    
    # Print progress
    print(f"Downloaded {len(all_contacts)} of {total_records} contacts")
    
    # Break if we've retrieved all records
    if offset >= total_records:
      break
  
  return all_contacts

def find_contact_by_email(url: str, username: str, token: str, email: str) -> Optional[int]:
  """
  Searches for a contact with the given email address.
  
  Args:
    url (str): The API endpoint URL
    username (str): Username for authentication
    token (str): Authentication token
    email (str): The email address to search for
    
  Returns:
    Optional[int]: The contact ID if found, None if not found
    
  Raises:
    requests.exceptions.RequestException: If the API request fails
  """
  # Prepare request data
  data = {
    "username": username,
    "token": token,
    "requestType": "contacts",
    "requestMethod": "getAll",
    "data": {
      "limit": 200,
      "offset": 0,
      "includeMetadata": 1,
      "criteria": {
        "email": email
      }
    }
  }
  
  # Make the POST request
  response = requests.post(url, json=data)
  
  # Check if request was successful
  if response.status_code != 200:
    raise requests.exceptions.RequestException(
      f"API request failed with status code {response.status_code}: {response.text}"
    )
  
  # Parse response
  response_data = response.json()
  
  # Check status
  if response_data.get("status") != "SUCCESS":
    return None
  
  # Extract contacts data
  contacts_data = response_data.get("data", {})
  
  # Debug the structure (optional)
  # print(f"Data type: {type(contacts_data)}")
  # print(f"Data content: {json.dumps(contacts_data, indent=2)}")
  
  # Process based on data structure
  if isinstance(contacts_data, dict):
    # If data is a dictionary (key-value pairs of contact_id: contact_info)
    for contact_id, contact_info in contacts_data.items():
      if contact_info.get("email", "").lower() == email.lower():
        return int(contact_id)
  elif isinstance(contacts_data, list):
    # If data is a list of contact objects
    for contact in contacts_data:
      if contact.get("email", "").lower() == email.lower():
        # Try to get ID using different possible field names
        for id_field in ["idContact", "id", "contactId"]:
          if id_field in contact:
            return int(contact[id_field])
  
  # Return None if no matching contact found
  return None

def manage_contact_lists(
  url: str,
  username: str,
  token: str,
  contact_id: Optional[int] = None,
  email: Optional[str] = None,
  add_to_lists: List[int] = None,
  remove_from_lists: List[int] = None
) -> bool:
  """
  Manages the lists a contact belongs to (adds to or removes from lists).
  
  Args:
    url (str): The API endpoint URL
    username (str): Username for authentication
    token (str): Authentication token
    contact_id (Optional[int]): ID of the contact (either this or email must be provided)
    email (Optional[str]): Email address of the contact (either this or contact_id must be provided)
    add_to_lists (List[int], optional): List IDs to add the contact to
    remove_from_lists (List[int], optional): List IDs to remove the contact from
    
  Returns:
    bool: True if successful, False otherwise
    
  Raises:
    ValueError: If neither contact_id nor email is provided
    requests.exceptions.RequestException: If the API request fails
  """
  # Ensure at least one identifier is provided
  if contact_id is None and (email is None or email == ""):
    raise ValueError("Either contact_id or email must be provided")
  
  # Initialize the data structure
  contact_data = {}
  
  # Add the identifier (either ID or email)
  if contact_id is not None:
    contact_data["idContact"] = contact_id
  else:
    contact_data["email"] = email
  
  # Add lists to add/remove (if provided)
  if add_to_lists:
    contact_data["addTo"] = add_to_lists
  
  if remove_from_lists:
    contact_data["removeFrom"] = remove_from_lists
  
  # Prepare request data
  data = {
    "username": username,
    "token": token,
    "requestType": "contacts",
    "requestMethod": "manageLists",
    "data": contact_data
  }
  
  # Make the POST request
  response = requests.post(url, json=data)
  
  # Check if request was successful
  if response.status_code != 200:
    raise requests.exceptions.RequestException(
      f"API request failed with status code {response.status_code}: {response.text}"
    )
  
  # Parse response
  response_data = response.json()
  
  # Check status
  if response_data.get("status") != "SUCCESS":
    error_message = response_data.get("errorMessage", "Unknown error")
    print(f"Failed to manage contact lists: {error_message}")
    return False
  
  return True

# Helper function to add a contact to a single list
def add_contact_to_list(
  url: str,
  username: str,
  token: str,
  list_id: int,
  contact_id: Optional[int] = None,
  email: Optional[str] = None
) -> bool:
  """
  Adds a contact to a specific list.
  
  Args:
    url (str): The API endpoint URL
    username (str): Username for authentication
    token (str): Authentication token
    list_id (int): ID of the list to add the contact to
    contact_id (Optional[int]): ID of the contact (either this or email must be provided)
    email (Optional[str]): Email address of the contact (either this or contact_id must be provided)
    
  Returns:
    bool: True if successful, False otherwise
  """
  return manage_contact_lists(
    url=url,
    username=username,
    token=token,
    contact_id=contact_id,
    email=email,
    add_to_lists=[list_id]
  )

def read_darujme_data(filepath: str = "./temp/darujme_data.json") -> List[Dict]:
  """
  Reads and parses the Darujme data file
  
  Args:
    filepath: Path to the darujme_data.json file
  Returns:
    List of pledges with donor information
  """
  try:
    with open(filepath, 'r', encoding='utf-8') as f:
      data = json.load(f)
      return data.get('pledges', [])
  except (IOError, json.JSONDecodeError) as e:
    print(f"Error reading Darujme data: {e}")
    return []

def categorize_darujme_donors(
  url: str,
  username: str, 
  token: str,
  pledges: List[Dict]
) -> Dict[str, List]:
  """
  Categorizes Darujme donors into new and existing based on email presence in Anabix
  
  Args:
    url: Anabix API URL
    username: Anabix username
    token: Anabix API token
    pledges: List of Darujme pledges
  Returns:
    Dictionary with 'new' and 'existing' donor lists
  """
  result = {
    'new': [],
    'existing': []
  }
  
  # Extract unique donors (by email) from pledges
  seen_emails = set()
  unique_donors = []
  
  for pledge in pledges:
    donor = pledge.get('donor', {})
    email = donor.get('email')
    if email and email not in seen_emails:
      seen_emails.add(email)
      unique_donors.append(donor)
  
  # Categorize donors
  for donor in unique_donors:
    email = donor.get('email')
    contact_id = find_contact_by_email(url, username, token, email)
    
    if contact_id:
      result['existing'].append({
        'donor': donor,
        'contact_id': contact_id
      })
    else:
      result['new'].append(donor)
  
  return result

def create_contact(
  url: str,
  username: str,
  token: str,
  email: str = "",
  first_name: str = "",
  last_name: str = "",
  phone_number: str = "",
  cell_number: str = "",
  company_name: str = "",
  shipping_address: Dict = None,
  gdpr_reason: int = 2,  # Default to "Oprávněný zájem správce"
  gdpr_acceptance_date: str = None  # Format YYYY-MM-DD
) -> Optional[int]:
  """
  Creates a new contact in Anabix
  
  Args:
    url: Anabix API URL
    username: Anabix username
    token: Anabix API token
    email: Contact's primary email
    first_name: Contact's first name
    last_name: Contact's last name
    phone_number: Contact's phone number
    cell_number: Contact's mobile number
    company_name: Contact's company name
    shipping_address: Dict containing address details (shippingStreet, shippingCity, shippingCode, shippingCountry)
    gdpr_reason: GDPR legal basis (1-7)
    gdpr_acceptance_date: Date of GDPR consent (YYYY-MM-DD)
    
  Returns:
    Optional[int]: The ID of created contact if successful, None if failed
  """
  # Validate that at least one required field is present
  if not any([first_name, last_name, email]):
    print("Error: At least one of firstName, lastName, or email must be provided")
    return None

  contact_data = {
    "email": email,
    "firstName": first_name,
    "lastName": last_name,
    "phoneNumber": phone_number,
    "cellNumber": cell_number,
    "organization": company_name,
    "gdprReason": gdpr_reason
  }
  
  # Add GDPR acceptance date if provided
  if gdpr_acceptance_date:
    contact_data["gdprAcceptanceDate"] = gdpr_acceptance_date
  
  # Add shipping address if provided
  if shipping_address:
    contact_data.update({
      "shippingStreet": shipping_address.get("street", ""),
      "shippingCity": shipping_address.get("city", ""),
      "shippingCode": shipping_address.get("postCode", ""),
      "shippingCountry": shipping_address.get("country", "")
    })
  
  # Prepare request data
  data = {
    "username": username,
    "token": token,
    "requestType": "contacts",
    "requestMethod": "create",
    "data": contact_data
  }
  
  try:
    response = requests.post(url, json=data)
    response.raise_for_status()
    
    response_data = response.json()
    
    if response_data.get("status") == "SUCCESS":
      return response_data.get("data", {}).get("idContact")
    else:
      print(f"Failed to create contact: {response_data.get('errorMessage', 'Unknown error')}")
      return None
      
  except requests.exceptions.RequestException as e:
    print(f"Error creating contact: {e}")
    return None

def add_new_donors_to_list(
  url: str,
  username: str,
  token: str,
  list_id: int,
  new_donors: List[Dict]
) -> Dict[str, List]:
  """
  Creates new contacts and adds them to specified Anabix list
  
  Args:
    url: Anabix API URL
    username: Anabix username
    token: Anabix API token
    list_id: ID of the target list
    new_donors: List of new donors to add
  Returns:
    Dictionary with successful and failed additions
  """
  results = {
    'success': [],
    'failed': []
  }
  
  for donor in new_donors:
    email = donor.get('email')
    if not email:
      results['failed'].append({'donor': donor, 'reason': 'No email provided'})
      continue
        
    try:
      # Create new contact
      contact_id = create_contact(
        url=url,
        username=username,
        token=token,
        email=email,
        first_name=donor.get('firstName', ''),
        last_name=donor.get('lastName', ''),
        phone_number=donor.get('phone', ''),
        cell_number=donor.get('phone', ''),  # Using phone as cell_number since Darujme provides only one
        company_name=donor.get('companyName', ''),
        shipping_address={
          "street": donor.get('address', {}).get('street', ''),
          "city": donor.get('address', {}).get('city', ''),
          "postCode": donor.get('address', {}).get('postCode', ''),
          "country": donor.get('address', {}).get('country', '')
        } if donor.get('address') else None
      )
      
      if not contact_id:
        results['failed'].append({'donor': donor, 'reason': 'Failed to create contact'})
        continue
      
      # Add to list
      success = add_contact_to_list(
        url=url,
        username=username,
        token=token,
        list_id=list_id,
        contact_id=contact_id
      )
      
      if success:
        results['success'].append(donor)
      else:
        results['failed'].append({'donor': donor, 'reason': 'Failed to add to list'})
            
    except Exception as e:
      results['failed'].append({'donor': donor, 'reason': str(e)})
  
  return results

def update_existing_donors_list(
  url: str,
  username: str,
  token: str,
  list_id: int,
  existing_donors: List[Dict]
) -> Dict[str, List]:
  """
  Updates list membership for existing donors
  
  Args:
    url: Anabix API URL
    username: Anabix username
    token: Anabix API token
    list_id: ID of the target list
    existing_donors: List of existing donors with their contact IDs
  Returns:
    Dictionary with successful and failed updates
  """
  results = {
    'success': [],
    'failed': []
  }
  
  for donor_info in existing_donors:
    contact_id = donor_info.get('contact_id')
    
    try:
      success = add_contact_to_list(
        url=url,
        username=username,
        token=token,
        list_id=list_id,
        contact_id=contact_id
      )
      
      if success:
        results['success'].append(donor_info)
      else:
        results['failed'].append({
          'donor_info': donor_info,
          'reason': 'API call failed'
        })
            
    except Exception as e:
      results['failed'].append({
        'donor_info': donor_info,
        'reason': str(e)
      })
  
  return results

def read_darujme_projects(filepath: str = "./temp/darujme_projects.json") -> Dict[str, str]:
    """
    Reads and parses the Darujme projects file
    
    Args:
        filepath: Path to the darujme_projects.json file
    Returns:
        Dictionary mapping project IDs to their titles
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Create a dictionary from projects array
            return {
                str(project['projectId']): project['title']['cs']
                for project in data.get('projects', [])
            }
    except (IOError, json.JSONDecodeError) as e:
        print(f"Error reading Darujme projects: {e}")
        return {}

def process_darujme_donors(
  url: str,
  username: str,
  token: str,
  list_id: int,
  darujme_file: str = "./temp/darujme_data.json",
  projects_file: str = "./temp/darujme_projects.json"
) -> Dict[str, Any]:
  """
  Main function to process Darujme donors and update Anabix list
  
  Args:
    url: Anabix API URL
    username: Anabix username
    token: Anabix API token
    list_id: ID of the target list
    darujme_file: Path to the Darujme data file
    projects_file: Path to the Darujme projects file
  Returns:
    Processing results summary
  """
  # Read data
  pledges = read_darujme_data(darujme_file)
  if not pledges:
    return {'error': 'No pledges found'}
    
  projects = read_darujme_projects(projects_file)
  if not projects:
    return {'error': 'No projects found'}
  
  # Process donors
  results = {
    'processed_pledges': 0,
    'new_donors': 0,
    'existing_donors': 0,
    'activities_created': 0,
    'errors': []
  }
  
  # Get custom field IDs for activities
  custom_field_ids = get_activity_custom_fields(url, username, token)
  
  # Process each pledge
  for pledge in pledges:
    try:
      # Get donor email and project
      donor_email = pledge.get('donor', {}).get('email')
      project_id = pledge.get('projectId')
      project_title = projects.get(project_id)
      
      if not all([donor_email, project_title]):
        results['errors'].append(f"Missing data for pledge {pledge.get('pledgeId')}")
        continue
      
      # Get or create contact
      contact_id = find_contact_by_email(url, username, token, donor_email)
      if not contact_id:
        # Create new contact
        contact_id = create_contact(
          url=url,
          username=username,
          token=token,
          email=donor_email,
          first_name=pledge.get('donor', {}).get('firstName', ''),
          last_name=pledge.get('donor', {}).get('lastName', ''),
          phone_number=pledge.get('donor', {}).get('phone', '')
        )
        if contact_id:
          results['new_donors'] += 1
      else:
        results['existing_donors'] += 1
      
      if not contact_id:
        results['errors'].append(f"Failed to create/find contact for {donor_email}")
        continue
      
      # Add to list
      add_contact_to_list(url, username, token, list_id, contact_id=contact_id)
      
      # Get or create deal
      deal_id = get_or_create_deal(url, username, token, project_title, contact_id)
      if not deal_id:
        results['errors'].append(f"Failed to get/create deal for {project_title}")
        continue
      
      # Create activity
      activity_id = create_darujme_pledge_activity(
        url=url,
        username=username,
        token=token,
        contact_id=contact_id,
        deal_id=deal_id,
        pledge=pledge,
        custom_field_ids=custom_field_ids,
        check_duplicates=True
      )
      
      if activity_id:
        results['activities_created'] += 1
      
      results['processed_pledges'] += 1
      
    except Exception as e:
      results['errors'].append(str(e))
  
  return results

def get_deals_by_title(
  url: str,
  username: str,
  token: str,
  title: str
) -> List[Dict]:
  """
  Get business cases (deals) by title
  
  Args:
    url: Anabix API URL
    username: Anabix username
    token: Anabix API token
    title: Title of the business case to search for
    
  Returns:
    List of matching deals
  """
  data = {
    "username": username,
    "token": token,
    "requestType": "deals",
    "requestMethod": "getAll",
    "data": {
      "limit": 200,
      "offset": 0,
      "includeMetadata": 1,
      "criteria": {
        "title": title
      }
    }
  }
  
  try:
    response = requests.post(url, json=data)
    response.raise_for_status()
    
    response_data = response.json()
    
    if response_data.get("status") != "SUCCESS":
      print(f"Failed to get deals: {response_data.get('errorMessage', 'Unknown error')}")
      return []
    
    # Handle both list and dictionary response formats
    deals_data = response_data.get("data", [])
    if isinstance(deals_data, dict):
      return list(deals_data.values())
    return deals_data
    
  except requests.exceptions.RequestException as e:
    print(f"Error getting deals: {e}")
    return []

def create_deal(
  url: str,
  username: str,
  token: str,
  title: str,
  contact_id: int,
  body: str = "",
  status: str = "open",
  owner_id: Optional[int] = None,
  amount: Optional[float] = None,
  deadline: Optional[str] = None,  # YYYY-MM-DD
  completed_date: Optional[str] = None  # YYYY-MM-DD
) -> Optional[int]:
  """
  Create a new business case (deal)
  
  Args:
    url: Anabix API URL
    username: Anabix username
    token: Anabix API token
    title: Deal title
    contact_id: ID of the related contact
    body: Deal description
    status: Deal status (open, postponed, won, closed)
    owner_id: ID of the deal owner
    amount: Deal amount
    deadline: Deal deadline (YYYY-MM-DD)
    completed_date: Deal completion date (YYYY-MM-DD)
    
  Returns:
    ID of created deal if successful, None if failed
  """
  deal_data = {
    "title": title,
    "idContact": contact_id,
    "body": body,
    "status": status
  }
  
  # Add optional fields if provided
  if owner_id:
    deal_data["idOwner"] = owner_id
  if amount is not None:
    deal_data["amount"] = amount
  if deadline:
    deal_data["deadline"] = deadline
  if completed_date:
    deal_data["completedDate"] = completed_date
  
  data = {
    "username": username,
    "token": token,
    "requestType": "deals",
    "requestMethod": "create",
    "data": deal_data
  }
  
  try:
    response = requests.post(url, json=data)
    response.raise_for_status()
    
    response_data = response.json()
    
    if response_data.get("status") == "SUCCESS":
      return response_data.get("data", {}).get("idDeal")
    else:
      print(f"Failed to create deal: {response_data.get('errorMessage', 'Unknown error')}")
      return None
      
  except requests.exceptions.RequestException as e:
    print(f"Error creating deal: {e}")
    return None

def get_or_create_deal(
  url: str,
  username: str,
  token: str,
  title: str,
  contact_id: int,
  **deal_params
) -> Optional[int]:
  """
  Get existing deal by title or create new one if it doesn't exist
  
  Args:
    url: Anabix API URL
    username: Anabix username
    token: Anabix API token
    title: Deal title
    contact_id: ID of the related contact
    **deal_params: Additional parameters for deal creation
    
  Returns:
    Deal ID if found or created successfully, None if failed
  """
  # Try to find existing deal
  existing_deals = get_deals_by_title(url, username, token, title)
  
  if existing_deals:
    # Return ID of the first matching deal
    return existing_deals[0].get("idDeal")
  
  # Create new deal if none exists
  return create_deal(
    url=url,
    username=username,
    token=token,
    title=title,
    contact_id=contact_id,
    **deal_params
  )
  
def get_activities_by_contact_and_deal(
  url: str,
  username: str,
  token: str,
  contact_id: int,
  deal_id: int,
  since_date: Optional[str] = None  # YYYY-MM-DD
) -> List[Dict]:
  """
  Get activities for a specific contact within a deal
  """
  data = {
    "username": username,
    "token": token,
    "requestType": "activities",
    "requestMethod": "getAll",
    "data": {
      "limit": 200,
      "offset": 0,
      "includeMetadata": 1,
      "criteria": {
        "idContact": contact_id,
        "idDeal": deal_id
      }
    }
  }
  
  try:
    response = requests.post(url, json=data)
    response.raise_for_status()
    
    response_data = response.json()
    
    if response_data.get("status") != "SUCCESS":
      print(f"Failed to get activities: {response_data.get('errorMessage', 'Unknown error')}")
      return []
    
    activities = response_data.get("data", [])
    if isinstance(activities, dict):
      activities = list(activities.values())
    
    # Filter by date if provided
    if since_date:
      from datetime import datetime
      since_timestamp = datetime.strptime(since_date, '%Y-%m-%d').timestamp()
      activities = [
        activity for activity in activities 
        if activity.get('timestamp', 0) >= since_timestamp
      ]
    
    return activities
    
  except requests.exceptions.RequestException as e:
    print(f"Error getting activities: {e}")
    return []

def create_activity(
  url: str,
  username: str,
  token: str,
  contact_id: int,
  deal_id: int,
  body: str,
  timestamp: int,  # Unix timestamp
  custom_fields: Dict[str, Any] = None,
  activity_type: str = "note",
  title: str = None
) -> Optional[int]:
  """
  Create a new activity
  """
  activity_data = {
    "idContact": contact_id,
    "idDeal": deal_id,
    "body": body,
    "type": activity_type,
    "timestamp": timestamp
  }
  
  if title:
    activity_data["title"] = title
    
  if custom_fields:
    # Format custom fields as simple id => value pairs
    formatted_fields = {}
    for field_id, field_data in custom_fields.items():
      formatted_fields[field_id] = field_data["value"]
    activity_data["customFields"] = formatted_fields
  
  data = {
    "username": username,
    "token": token,
    "requestType": "activities",
    "requestMethod": "create",
    "data": activity_data
  }
  
  # Debug print
  print("\nActivity creation request data:")
  print(json.dumps(data, indent=2, ensure_ascii=False))
  
  try:
    response = requests.post(url, json=data)
    
    # Debug print
    print("\nResponse status code:", response.status_code)
    print("Response body:")
    print(json.dumps(response.json(), indent=2, ensure_ascii=False))
    
    response.raise_for_status()
    
    response_data = response.json()
    
    if response_data.get("status") == "SUCCESS":
      return response_data.get("data", {}).get("idActivity")
    else:
      print(f"Failed to create activity: {response_data.get('errorMessage', 'Unknown error')}")
      return None
      
  except requests.exceptions.RequestException as e:
    print(f"Error creating activity: {e}")
    return None

def create_darujme_pledge_activity(
  url: str,
  username: str,
  token: str,
  contact_id: int,
  deal_id: int,
  pledge: Dict,
  custom_field_ids: Dict[str, int],
  check_duplicates: bool = True
) -> Optional[int]:
  """
  Create an activity for a Darujme pledge
  """
  # Get successful transaction
  successful_states = {"success", "success_money_on_account", "sent_to_organization"}
  transaction = next(
    (t for t in pledge.get('transactions', [])
    if t.get('state') in successful_states),
    None
  )
  
  if not transaction:
    return None
  
  # Format amounts
  sent_amount = int(round(transaction.get('sentAmount', {}).get('cents', 0) / 100))
  outgoing_amount = int(round(transaction.get('outgoingAmount', {}).get('cents', 0) / 100))
  
  # Create activity text
  body = f"Dar přes Darujme.cz - {sent_amount} Kč"
  
  # Convert timestamp to Unix timestamp
  from datetime import datetime
  timestamp_str = re.sub(r'([+-]\d{2}:\d{2})$', '', pledge.get('pledgedAt', ''))
  dt = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S')
  timestamp = int(dt.timestamp())  # Convert to Unix timestamp
  
  # Check for duplicates if requested
  if check_duplicates:
    since_date = dt.strftime('%Y-%m-%d')
    existing_activities = get_activities_by_contact_and_deal(
      url, username, token, contact_id, deal_id, since_date
    )
    
    # Check if similar activity exists
    for activity in existing_activities:
      if body in str(activity.get('body', '')):
        print(f"Duplicate activity found for pledge {pledge.get('pledgeId')}")
        return None
  
  # Custom fields with actual IDs and correct value formats
  custom_fields = {}
  
  # Add custom fields exactly as they appear in the example
  custom_fields[str(custom_field_ids['amount_gross'])] = {
    "idCustomField": custom_field_ids['amount_gross'],
    "value": str(sent_amount)
  }
  custom_fields[str(custom_field_ids['amount_net'])] = {
    "idCustomField": custom_field_ids['amount_net'],
    "value": str(outgoing_amount)
  }
  custom_fields[str(custom_field_ids['support_type'])] = {
    "idCustomField": custom_field_ids['support_type'],
    "value": SUPPORT_TYPE_FINANCIAL
  }
  custom_fields[str(custom_field_ids['branch'])] = {
    "idCustomField": custom_field_ids['branch'],
    "value": BRANCH_TYK
  }
  custom_fields[str(custom_field_ids['donor_type'])] = {
    "idCustomField": custom_field_ids['donor_type'],
    "value": DONOR_TYPE_INDIVIDUAL
  }
  
  # Debug print
  print("\nPreparing to create activity with:")
  print(f"Contact ID: {contact_id}")
  print(f"Deal ID: {deal_id}")
  print(f"Body: {body}")
  print(f"Timestamp: {timestamp}")
  print("Custom fields:")
  print(json.dumps(custom_fields, indent=2, ensure_ascii=False))
  
  return create_activity(
    url=url,
    username=username,
    token=token,
    contact_id=contact_id,
    deal_id=deal_id,
    body=body,
    timestamp=timestamp,
    custom_fields=custom_fields
  )

def get_activity_custom_fields(
  url: str,
  username: str,
  token: str
) -> Dict[str, int]:
  """
  Get custom field IDs for activity fields
  """
  return ACTIVITY_CUSTOM_FIELDS

def test_single_pledge():
  """
  Test function to process a single pledge step by step
  """
  print("\n=== Testing Single Pledge Processing ===\n")
  
  # Read test data
  pledges = read_darujme_data("./temp/darujme_data.json")
  if not pledges:
    print("No pledges found")
    return
  
  # Get first pledge
  first_pledge = pledges[0]
  print(f"Testing with pledge ID: {first_pledge.get('pledgeId')}")
  
  # Get project title
  projects = read_darujme_projects("./temp/darujme_projects.json")
  project_id = first_pledge.get('projectId')
  project_title = projects.get(str(project_id))
  if not project_title:
    print(f"Project {project_id} not found")
    return
  
  print(f"Project title: {project_title}")
  
  # Get contact ID
  donor_email = first_pledge.get('donor', {}).get('email')
  contact_id = find_contact_by_email(url=API_URL, username=username, token=token, email=donor_email)
  if not contact_id:
    print(f"Contact not found for email: {donor_email}")
    return
  
  print(f"Contact ID: {contact_id}")
  
  # Get or create deal
  deal_id = get_or_create_deal(url=API_URL, username=username, token=token, title=project_title, contact_id=contact_id)
  if not deal_id:
    print("Failed to get or create deal")
    return
  
  print(f"Deal ID: {deal_id}")
  
  # Create activity
  activity_id = create_darujme_pledge_activity(
    url=API_URL,
    username=username,
    token=token,
    contact_id=contact_id,
    deal_id=deal_id,
    pledge=first_pledge,
    custom_field_ids=ACTIVITY_CUSTOM_FIELDS,
    check_duplicates=True
  )
  
  if activity_id:
    print(f"\nSuccess! Activity created with ID: {activity_id}")
  else:
    print("\nNo activity created (possibly duplicate)")

if __name__ == "__main__":
  # Load environment variables
  load_dotenv()
  
  # Get credentials
  username = os.getenv("ANABIX_USERNAME")
  token = os.getenv("ANABIX_API_TOKEN")
  
  if not all([username, token]):
    print("Error: Missing required environment variables")
    print("Please set ANABIX_USERNAME and ANABIX_API_TOKEN")
    exit(1)
  
  # Run test function if TEST environment variable is set
  if os.getenv("TEST"):
    test_single_pledge()
  else:
    # Process all donors (original functionality)
    results = process_darujme_donors(
      url=API_URL,
      username=username,
      token=token,
      list_id=INDIVIDUAL_DONORS_LIST_ID
    )
    
    # Print results
    print("\nProcessing Results:")
    print(f"Pledges processed: {results.get('processed_pledges', 0)}")
    print(f"New donors: {results.get('new_donors', 0)}")
    print(f"Existing donors: {results.get('existing_donors', 0)}")
    print(f"Activities created: {results.get('activities_created', 0)}")
    
    if results.get('errors'):
      print("\nErrors:")
      for error in results['errors']:
        print(f"- {error}")


