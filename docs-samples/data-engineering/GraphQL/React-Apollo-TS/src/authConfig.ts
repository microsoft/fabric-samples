import {
  InteractiveBrowserCredential,
  ClientSecretCredential,
  TokenCredential,
} from "@azure/identity";

interface AuthOptions {
  useInteractive?: boolean; // Change to true to use browser-based login, for development
}

export const AUTH_CONFIG = {
  clientId: "<Enter_the_Application_Id_Here>",
  tenantId: "<Enter_the_Tenant_Id_Here>",
  clientSecret: "<Enter_the_Client_Secret_Here>", \\optional
};

export const GRAPHQL_ENDPOINT =
  "https://7f8c98f205134c0ba7fa35ffcea479d1.z7f.dailygraphql.fabric.microsoft.com/v1/workspaces/7f8c98f2-0513-4c0b-a7fa-35ffcea479d1/graphqlapis/a703d700-18c6-4b50-84ba-a20f1ab1b179/graphql";

/**
 * Scopes you add here will be prompted for user consent during sign-in.
 * By default, MSAL.js will add OIDC scopes (openid, profile, email) to any login request.
 * For more information about OIDC scopes, visit:
 * https://docs.microsoft.com/en-us/azure/active-directory/develop/v2-permissions-and-consent#openid-connect-scopes
 */
export const DEFAULT_SCOPE =
  "https://analysis.windows.net/powerbi/api/.default";

export function getCredential(
  options: AuthOptions
): TokenCredential | undefined {
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
    return new ClientSecretCredential(
      AUTH_CONFIG.tenantId,
      AUTH_CONFIG.clientId,
      AUTH_CONFIG.clientSecret
    );
  }
}

export async function getToken(
  credential: TokenCredential | undefined,
  scope: string
) {
  const token = await credential?.getToken(scope);
  return token?.token;
}
