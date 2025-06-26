// MCP Server for Microsoft Fabric GraphQL API using Streamable HTTP
import express from 'express';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { request, gql } from 'graphql-request';
import { z } from 'zod';
import dotenv from 'dotenv';
import fetch from 'node-fetch';

// Load environment variables
dotenv.config();

const MICROSOFT_FABRIC_API_URL = process.env.MICROSOFT_FABRIC_API_URL;
const MICROSOFT_FABRIC_TENANT_ID = process.env.MICROSOFT_FABRIC_TENANT_ID;
const MICROSOFT_FABRIC_CLIENT_ID = process.env.MICROSOFT_FABRIC_CLIENT_ID;
const MICROSOFT_FABRIC_CLIENT_SECRET = process.env.MICROSOFT_FABRIC_CLIENT_SECRET;
const SCOPE = process.env.SCOPE;

// Token management
let accessToken = null;
let tokenExpiry = null;

async function getAccessToken() {
  // Check if we have a valid token
  if (accessToken && tokenExpiry && Date.now() < tokenExpiry) {
    return accessToken;
  }

  try {
    const tokenUrl = `https://login.microsoftonline.com/${MICROSOFT_FABRIC_TENANT_ID}/oauth2/v2.0/token`;
    
    const response = await fetch(tokenUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        client_id: MICROSOFT_FABRIC_CLIENT_ID,
        client_secret: MICROSOFT_FABRIC_CLIENT_SECRET,
        scope: SCOPE,
        grant_type: 'client_credentials',
      }),
    });

    if (!response.ok) {
      throw new Error(`Token request failed: ${response.status} ${response.statusText}`);
    }

    const tokenData = await response.json();
    accessToken = tokenData.access_token;
    // Set expiry to 5 minutes before actual expiry to be safe
    tokenExpiry = Date.now() + (tokenData.expires_in - 300) * 1000;
    
    console.log('Successfully obtained access token');
    return accessToken;
  } catch (error) {
    console.error('Error obtaining access token:', error);
    throw error;
  }
}

// Standard GraphQL introspection query
const INTROSPECTION_QUERY = gql`
  query IntrospectionQuery {
    __schema {
      queryType { name }
      mutationType { name }
      subscriptionType { name }
      types {
        ...FullType
      }
      directives {
        name
        description
        locations
        args {
          ...InputValue
        }
      }
    }
  }
  fragment FullType on __Type {
    kind
    name
    description
    fields(includeDeprecated: true) {
      name
      description
      args {
        ...InputValue
      }
      type {
        ...TypeRef
      }
      isDeprecated
      deprecationReason
    }
    inputFields {
      ...InputValue
    }
    interfaces {
      ...TypeRef
    }
    enumValues(includeDeprecated: true) {
      name
      description
      isDeprecated
      deprecationReason
    }
    possibleTypes {
      ...TypeRef
    }
  }
  fragment InputValue on __InputValue {
    name
    description
    type { ...TypeRef }
    defaultValue
  }
  fragment TypeRef on __Type {
    kind
    name
    ofType {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
              }
            }
          }
        }
      }
    }
  }
`;

// In-memory schema cache (for this process only)
let cachedSchema = null;

// Session management
const sessions = new Map();
let sessionCounter = 0;

function generateSessionId() {
  return `session-${++sessionCounter}`;
}

// Create a single server instance
const server = new McpServer({
  name: 'microsoft-fabric-graphql-mcp-server',
  version: '1.0.0',
}, { capabilities: {} });

// introspect-schema tool
server.tool(
  'introspect-schema',
  'Retrieves the GraphQL schema from the Microsoft Fabric endpoint.',
  z.object({}),
  async () => {
    try {
      const token = await getAccessToken();
      const response = await fetch(MICROSOFT_FABRIC_API_URL, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify({
          query: INTROSPECTION_QUERY,
          variables: {}
        })
      });
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP ${response.status}: ${errorText}`);
      }
      const schema = await response.json();
      cachedSchema = schema;
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(schema)
          }
        ]
      };
    } catch (err) {
      console.error('Error during schema introspection:', err);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({ error: 'Failed to introspect schema', details: err.message })
          }
        ]
      };
    }
  }
);

// query-graphql tool
server.tool(
  'query-graphql',
  'Executes a GraphQL query against the Microsoft Fabric endpoint.',
  {
    query: z.string().describe('GraphQL query string'),
    variables: z.record(z.any()).optional().describe('GraphQL variables'),
  },
  async ({ query, variables }) => {
    if (!cachedSchema) {
      const msg = 'Schema not available. Please run introspect-schema first.';
      console.error(msg);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({ error: msg })
          }
        ]
      };
    }
    try {
      const token = await getAccessToken();
      const response = await fetch(MICROSOFT_FABRIC_API_URL, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify({
          query,
          variables: variables || {}
        })
      });
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP ${response.status}: ${errorText}`);
      }
      const data = await response.json();
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({ data })
          }
        ]
      };
    } catch (err) {
      console.error('Error during GraphQL query:', err);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({ error: 'Failed to execute GraphQL query', details: err.message })
          }
        ]
      };
    }
  }
);

// Express server setup
const app = express();
app.use(express.json());

// Force all responses to be JSON
app.use((req, res, next) => {
  res.type('application/json');
  next();
});

// MCP endpoint
app.post('/mcp', (req, res, next) => {
  console.log('RAW BODY:', req.body);
  next();
});

app.post('/mcp', async (req, res) => {
  try {
    // Create session manager
    const sessionManager = {
      sessionIdGenerator: generateSessionId,
      getSession: (sessionId) => sessions.get(sessionId),
      createSession: (sessionId) => {
        const session = { id: sessionId, created: Date.now() };
        sessions.set(sessionId, session);
        return session;
      },
      deleteSession: (sessionId) => {
        sessions.delete(sessionId);
      }
    };

    const transport = new StreamableHTTPServerTransport({
      sessionManager
    });
    
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
    res.on('close', () => {
      transport.close();
    });
  } catch (err) {
    console.error('Error handling MCP request:', err);
    if (!res.headersSent) {
      res.status(500).json({ error: 'Internal server error', details: err.message });
    }
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    server: 'Microsoft Fabric GraphQL MCP Server',
    hasToken: !!accessToken,
    tokenExpiry: tokenExpiry ? new Date(tokenExpiry).toISOString() : null
  });
});

app.listen(3000, () => {
  console.log('Microsoft Fabric GraphQL MCP server listening on port 3000');
  console.log('API URL:', MICROSOFT_FABRIC_API_URL);
  console.log('Scope:', SCOPE);
}); 