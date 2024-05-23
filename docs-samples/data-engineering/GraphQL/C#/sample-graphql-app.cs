using System;
using Microsoft.Identity.Client;
using GraphQL.Client.Http;
using GraphQL.Client.Serializer.Newtonsoft;

namespace AzureADAuthApp
{
    class Program
    {
        private const string ClientId = "1e1126bf-17a1-4c9c-bcbe-8e41700991d6";  // This is your Application ID from Entra App Registration
        private const string TenantId = "cb4ed31a-dc29-496c-979a-ef47810cfeae";  // This is your Tenant ID from Entra App Registration
        private const string Authority = "https://login.microsoftonline.com/" + TenantId;
        private const string GraphQLUri = "https://api.fabric.microsoft.com/v1/workspaces/[WORKSPACE_ID]/graphqlapis/[API_ID]/graphql"; // This is your GraphQL API endpoint

        static async System.Threading.Tasks.Task Main(string[] args)
        {
            var app = PublicClientApplicationBuilder.Create(ClientId)
                .WithAuthority(Authority)
                .WithRedirectUri("http://localhost:1234")
                .Build();

            var scopes = new[] { "https://analysis.windows.net/powerbi/api/.default" };

            try
            {
                var result = await app.AcquireTokenInteractive(scopes).ExecuteAsync();
                if (result != null)
                {
                    Console.WriteLine("Authentication Success");

                    var graphQLClient = new GraphQLHttpClient(GraphQLUri, new NewtonsoftJsonSerializer());
                    
                    graphQLClient.HttpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", result.AccessToken);

                    var query = new GraphQLHttpRequest
                      {
                          Query = @"query {
                                        addresses {
                                            items {
                                                City
                                            }
                                        }
                                    }" 
                      };
                    
                    var graphQLResponse = await graphQLClient.SendQueryAsync<dynamic>(query);

                    Console.WriteLine(graphQLResponse.Data.ToString());
                }
            }
            catch (MsalServiceException ex)
            {
                Console.WriteLine($"Authentication Error: {ex.Message}");
            }
        }
    }
}
