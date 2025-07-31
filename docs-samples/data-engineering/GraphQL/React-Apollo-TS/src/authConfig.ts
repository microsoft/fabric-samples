import {
  InteractiveBrowserCredential,
  ClientSecretCredential,
  TokenCredential,
} from '@azure/identity';

interface AuthOptions {
  useInteractive?: boolean; // Change to true to use browser-based login, for development
}

export const AUTH_CONFIG = {
    clientId: "<Enter_the_Application_Id_Here>",
    tenantId: "<Enter_the_Tenant_Id_Here>",
    clientSecret: "<Enter_the_Client_Secret_Here>",
}

export const GRAPHQL_ENDPOINT = '<Enter_the_GraphQL_Endpoint_Here>';

/**
 * Scopes you add here will be prompted for user consent during sign-in.
 * By default, MSAL.js will add OIDC scopes (openid, profile, email) to any login request.
 * For more information about OIDC scopes, visit: 
 * https://docs.microsoft.com/en-us/azure/active-directory/develop/v2-permissions-and-consent#openid-connect-scopes
 */
export const DEFAULT_SCOPE = "https://analysis.windows.net/powerbi/api/.default";

export function getCredential(options: AuthOptions): TokenCredential | undefined {
  const { useInteractive } = options;

  if (useInteractive || !AUTH_CONFIG.clientSecret) {
    // Development: Use browser-based login
    return new InteractiveBrowserCredential({
      tenantId: AUTH_CONFIG.tenantId,
      clientId: AUTH_CONFIG.clientId,
    });
  }

  // Production: Use App Registration with client secret
  if (AUTH_CONFIG.tenantId && AUTH_CONFIG.clientId) {
    return new ClientSecretCredential(AUTH_CONFIG.tenantId, AUTH_CONFIG.clientId, AUTH_CONFIG.clientSecret);
  }
}

export async function getToken(credential: TokenCredential | undefined, scope: string) {
  const token = await credential?.getToken(scope);
  return token?.token;
}
