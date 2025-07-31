import React, { useEffect, useState } from 'react';
import { useQuery, gql, ApolloClient, ApolloProvider } from '@apollo/client';
import './App.css';
import { DEFAULT_SCOPE, getCredential, getToken, GRAPHQL_ENDPOINT } from './authConfig';
import { getApolloClient } from './Client';

const ExampleComponent: React.FC = () => {

// define the query - This is a simple query to get all customer details
const GQL_GET_CUSTOMERS= gql`
  query GQL_GET_CUSTOMERS{
    customers {
      items {
        CustomerID
        FirstName
        LastName
      }
    }
  }
`;

const { loading, error, data } = useQuery(GQL_GET_CUSTOMERS);

  if (loading) return <div>Loading...</div>;
  if (error) return <div>Error: {error.message}</div>;

  return (
    <div>
      <main>
        <h2>Customers</h2>
        {data?.customers.items ? (
          <table>
            <thead>
              <tr>
                <th>Customer ID</th>
                <th>First Name</th>
                <th>Last Name</th>
              </tr>
            </thead>
            <tbody>
              {data.customers.items.map((customer: any) => (
                <tr key={customer.CustomerID}>
                  <td>{customer.CustomerID}</td>
                  <td>{customer.FirstName}</td>
                  <td>{customer.LastName}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p>No customers found. Make sure to configure your GraphQL endpoint.</p>
        )}
      </main>
    </div>
  );
};

export const App: React.FC = () => {
  const [client, setClient] = useState<ApolloClient<any> | null>(null);

  useEffect(() => {
    const initClient = async () => {
      const credential = getCredential({ useInteractive: true });
      const token = await getToken(credential, DEFAULT_SCOPE);
      const apolloClient = getApolloClient(GRAPHQL_ENDPOINT, token!);
      setClient(apolloClient);
    };

    initClient();
  }, []);

  if (!client) return <p>Initializing client...</p>;

  return (
    <ApolloProvider client={client}>
      <h1>GraphQL + React (TypeScript)</h1>
      <ExampleComponent />
    </ApolloProvider>
  );
};

export default App;
