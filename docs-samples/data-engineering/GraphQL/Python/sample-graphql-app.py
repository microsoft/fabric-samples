import asyncio
from msal import PublicClientApplication
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

# Constants
CLIENT_ID = "1e1126bf-17a1-4c9c-bcbe-8e41700991d6"  # Your Application ID
TENANT_ID = "cb4ed31a-dc29-496c-979a-ef47810cfeae"  # Your Tenant ID
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
GRAPHQL_URI = "https://api.fabric.microsoft.com/v1/workspaces/[WORKSPACE_ID]/graphqlapis/[API_ID]/graphql"  # GraphQL API Endpoint
SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]  # Scopes for the resource you're accessing

async def main():
    # Setup MSAL Public Client
    app = PublicClientApplication(CLIENT_ID, authority=AUTHORITY)
    
    # Acquire token interactively
    result = None
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
    if not result:
        result = app.acquire_token_interactive(SCOPES)
    
    if "access_token" in result:
        print("Authentication Success")
        
        # Setup GraphQL client
        transport = AIOHTTPTransport(url=GRAPHQL_URI, headers={"Authorization": f"Bearer {result['access_token']}"})
        client = Client(transport=transport, fetch_schema_from_transport=True)
        
        # GraphQL query
        query = gql("""
            query {
                addresses {
                    items {
                        City
                    }
                }
            }
        """)
        
        # Execute query
        response = await client.execute_async(query)
        print(response)
    else:
        print(f"Authentication Error: {result.get('error_description')}")

if __name__ == "__main__":
    asyncio.run(main())