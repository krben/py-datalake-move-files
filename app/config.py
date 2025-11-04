import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

# Replace with your Dynamics details
D365_CLIENT_ID = os.getenv("D365_CLIENT_ID")  # Power BI App Client ID
D365_CLIENT_SECRET = os.getenv("D365_CLIENT_SECRET")  # Power BI App Secret
D365_TENANT_ID = os.getenv("D365_TENANT_ID")  # Microsoft Tenant ID
D365_RESOURCE_URL = os.getenv("D365_RESOURCE_URL")  # Dynamics 365 Resource URL
D365_API_URL = os.getenv("D365_API_URL")  # Dynamics 365 API URL
D365_ENDPOINTS_FILE = os.getenv("D365_ENDPOINTS_FILE")  # File to store last extracted timestamps

# Replace with your Azure details
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
AZURE_ACCOUNT_NAME = os.getenv("AZURE_ACCOUNT_NAME")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
AZURE_REMOTE_PATH = os.getenv("AZURE_REMOTE_PATH")

# Replace with your Graph details
GRAPH_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID")
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET")
GRAPH_TENANT_ID = os.getenv("GRAPH_TENANT_ID")
GRAPH_URL = os.getenv("GRAPH_URL")
GRAPH_SCOPES = os.getenv("GRAPH_SCOPES")
GRAPH_ENDPOINTS_FILE = os.getenv("GRAPH_ENDPOINTS_FILE")


# Replace with your Papertrail details
PAPERTRAIL_HOST = os.getenv("LOGGGER_HOST")
PAPERTRAIL_PORT = os.getenv("LOGGER_PORT")
LOGGER_NAME = os.getenv("LOGGER_NAME")
LOGGER_LEVEL = os.getenv("LOGGER_LEVEL")


# Azure Insights
AZURE_INSIGHTS_INSTRUMENTATION_KEY = os.getenv("AZURE_INSIGHTS_INSTRUMENTATION_KEY")
AZURE_INGESTS_ENDPOINT = os.getenv("AZURE_INGESTS_ENDPOINT")
AZURE_LIVE_ENDPOINT = os.getenv("AZURE_LIVE_ENDPOINT")
AZURE_APPLICATION_ID = os.getenv("AZURE_APPLICATION_ID")
AZURE_INSIGHTS_LOGGER_NAME = os.getenv("AZURE_INSIGHTS_LOGGER_NAME")
AZURE_INSIGHTS_LOGGER_LEVEL = os.getenv("AZURE_INSIGHTS_LOGGER_LEVEL")