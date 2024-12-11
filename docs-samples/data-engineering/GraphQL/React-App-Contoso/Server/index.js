// server/index.js
import { InteractiveBrowserCredential } from "@azure/identity";
import express from 'express';

//import fetch from 'node-fetch';
//const cred=require("@azure/identity");
//import pkg from "express"
//const {express} = pkg;

const PORT = process.env.PORT || 3001;

const app = express();


// Acquire a token
// DO NOT USE IN PRODUCTION.
// Below code to acquire token is for development purpose only to test the GraphQL endpoint
// For production, always register an application in a Microsoft Entra ID tenant and use the appropriate client_id and scopes
// https://learn.microsoft.com/en-us/fabric/data-engineering/connect-apps-api-graphql#create-a-microsoft-entra-app

let auth = new InteractiveBrowserCredential({});
let tokenPromise = auth.getToken('https://analysis.windows.net/powerbi/api/user_impersonation');
let accessToken = await tokenPromise;

//Uddate the GraphQL endpoint below with your own GraphQL endpoint created in Fabric by selecting "Copy endpoint" from the GraphQL API ribbon.
const endpoint = 'https://622e5d1a-5d57-41d4-b515-fda2d6a9993e.z62.msitgraphql.fabric.microsoft.com/v1/workspaces/622e5d1a-5d57-41d4-b515-fda2d6a9993e/graphqlapis/149ce51a-18f7-40c5-9f75-60b0582e9543/graphql';

const query = `
query {
  customers (first: 5){
    items {
        CustomerID
        FirstName
        LastName
        EmailAddress
        Phone
        CompanyName
        SalesPerson
      }
    }
        
  addresses {
     items {
        StateProvince
        
      }
    }

  products (first:10, orderBy: {
    ListPrice: DESC}) {
      items {
         ListPrice
         Name
      }
    }
}
  
`;

const headers = {
	'Content-Type': 'application/json',
	'Authorization': `Bearer ${accessToken.token}`
};

async function fetchData() 	{

    return fetch(endpoint, {
        method: 'POST',
        headers: headers,
        body: JSON.stringify({ query }),
    })
        .then(response => response.json())
        .catch(error => console.log(error));

}

app.get("/graphql", (req, res) => {
    (async()=>await fetchData().then(response => {
        res.json(response);
    }))();
  });

app.listen(PORT, () => {
  console.log(`Server listening on ${PORT}`);
});
