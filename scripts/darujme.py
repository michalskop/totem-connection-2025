import requests
from datetime import datetime, timedelta
import json
import os
from typing import Optional, Dict, Any
import logging
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Configure logging
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DarujmeDownloader:
  def __init__(self, org_id: str, api_id: str, api_secret: str):
    """
    Initialize the downloader with API credentials
    
    Args:
      org_id: Organization ID from Darujme.cz
      api_id: API ID from Darujme.cz
      api_secret: API secret from Darujme.cz
    """
    self.org_id = org_id  # Changed from constructing base_url directly
    self.base_url = f"https://www.darujme.cz/api/v1/organization/{org_id}/pledges-by-filter"
    self.api_id = api_id
    self.api_secret = api_secret

  def get_date_range(self, timeframe: str = "week") -> str:
    """
    Calculate the start date based on timeframe.
    Args:
      timeframe: Either 'week' or 'year' or a number of days
    Returns:
      ISO formatted date string
    """
    today = datetime.now()
    if timeframe.lower() == "week":
      start_date = today - timedelta(days=7)
    elif timeframe.lower() == "year":
      start_date = today - timedelta(days=365)
    elif timeframe.isdigit():
      start_date = today - timedelta(days=int(timeframe))
    else:
      raise ValueError("Timeframe must be either 'week' or 'year' or integer")
    
    return start_date.strftime("%Y-%m-%d")

  def download_data(self, timeframe: str = "week") -> Optional[Dict[str, Any]]:
    """
    Download data from Darujme.cz API
    Args:
      timeframe: Time period to fetch data for ('week' or 'year')
    Returns:
      JSON response data or None if request fails
    """
    try:
      params = {
        "apiId": self.api_id,
        "apiSecret": self.api_secret,
        "fromPledgedDate": self.get_date_range(timeframe)
      }
      
      logger.info(f"Fetching data from {self.get_date_range(timeframe)}")
      
      response = requests.get(
        self.base_url,
        params=params,
        timeout=30
      )
      
      response.raise_for_status()
      return response.json()
      
    except requests.RequestException as e:
      logger.error(f"Error downloading data: {str(e)}")
      return None

  def filter_successful_pledges(self, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Filter pledges to only include those with successful transactions.
    Successful states are: success, success_money_on_account, sent_to_organization
    
    Args:
      data: Raw API response data
    Returns:
      Filtered data containing only successful pledges
    """
    successful_states = {"success", "success_money_on_account", "sent_to_organization"}
    
    if not data or "pledges" not in data:
      return {"pledges": []}
    
    successful_pledges = []
    for pledge in data["pledges"]:
      # Check if any transaction in the pledge is successful
      if any(
        transaction["state"] in successful_states 
        for transaction in pledge.get("transactions", [])
      ):
        successful_pledges.append(pledge)
    
    return {"pledges": successful_pledges}

  def save_data(self, data: Dict[str, Any], output_file: str) -> bool:
    """
    Save downloaded data to JSON file with secure permissions
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Set secure file permissions (only owner can read/write)
    old_umask = os.umask(0o077)
    """
    Save downloaded data to JSON file
    Args:
      data: API response data
      output_file: Path to save the JSON file
    Returns:
      True if save successful, False otherwise
    """
    try:
      with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
      logger.info(f"Data saved to {output_file}")
      return True
      
    except IOError as e:
      logger.error(f"Error saving data: {str(e)}")
      return False

  def get_projects(self) -> Optional[Dict[str, Any]]:
    """
    Get list of projects from Darujme.cz API
    
    Returns:
      Dictionary containing projects data or None if request fails
    """
    try:
      params = {
        "apiId": self.api_id,
        "apiSecret": self.api_secret
      }
      
      logger.info("Fetching projects data")
      
      response = requests.get(
        f"https://www.darujme.cz/api/v1/organization/{self.org_id}/projects",
        params=params,
        timeout=30
      )
      
      response.raise_for_status()
      projects_data = response.json()
      
      # Log the number of projects found
      if projects_data and "projects" in projects_data:
        logger.info(f"Found {len(projects_data['projects'])} projects")
      
      return projects_data
      
    except requests.RequestException as e:
      logger.error(f"Error downloading projects: {str(e)}")
      return None

  def save_projects(self, data: Dict[str, Any], output_file: str = "./temp/darujme_projects.json") -> bool:
    """
    Save projects data to JSON file
    
    Args:
      data: Projects data from API
      output_file: Path to save the JSON file
    Returns:
      True if save successful, False otherwise
    """
    return self.save_data(data, output_file)

def main():
  # Get credentials from environment variables
  org_id = os.getenv("DARUJME_ORG_ID")
  api_id = os.getenv("DARUJME_API_ID")
  api_secret = os.getenv("DARUJME_API_SECRET")
  timeframe = os.getenv("DARUJME_TIMEFRAME", "year")  # Default to week if not specified
  output_file = os.getenv("DARUJME_OUTPUT_FILE", "./temp/darujme_data.json")
  fetch_projects = True
  projects_file = os.getenv("DARUJME_PROJECTS_FILE", "./temp/darujme_projects.json")

  # Validate environment variables
  if not all([org_id, api_id, api_secret]):
    logger.error("Missing required environment variables. Please check your .env file or environment variables.")
    logger.error("Required variables: DARUJME_ORG_ID, DARUJME_API_ID, DARUJME_API_SECRET")
    exit(1)

  # Initialize downloader
  downloader = DarujmeDownloader(org_id, api_id, api_secret)

  # Download and save projects if requested
  if fetch_projects:
    logger.info("Fetching projects data...")
    projects_data = downloader.get_projects()
    if projects_data:
      if downloader.save_projects(projects_data, projects_file):
        logger.info(f"Projects data saved to {projects_file}")
      else:
        logger.error("Failed to save projects data")
        exit(1)

  # Download and save pledges data
  data = downloader.download_data(timeframe)
  if data:
    # Filter successful pledges
    filtered_data = downloader.filter_successful_pledges(data)
    if downloader.save_data(filtered_data, output_file):
      logger.info("Process completed successfully")
      # exit(0)
  
  logger.error("Process failed")
  # exit(1)

if __name__ == "__main__":
  main()