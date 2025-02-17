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

def main():
  # Get credentials from environment variables
  org_id = os.getenv("DARUJME_ORG_ID")
  api_id = os.getenv("DARUJME_API_ID")
  api_secret = os.getenv("DARUJME_API_SECRET")
  timeframe = os.getenv("DARUJME_TIMEFRAME", "year")  # Default to week if not specified
  output_file = os.getenv("DARUJME_OUTPUT_FILE", "./temp/darujme_data.json")

  # Validate environment variables
  if not all([org_id, api_id, api_secret]):
    logger.error("Missing required environment variables. Please check your .env file or environment variables.")
    logger.error("Required variables: DARUJME_ORG_ID, DARUJME_API_ID, DARUJME_API_SECRET")
    exit(1)

  # Initialize downloader
  downloader = DarujmeDownloader(org_id, api_id, api_secret)

  # Download and save data
  data = downloader.download_data(timeframe)
  if data:
    if downloader.save_data(data, output_file):
      logger.info("Process completed successfully")
      exit(0)
  
  logger.error("Process failed")
  exit(1)

if __name__ == "__main__":
  main()