# Getting Started with Create React App

This sample project was bootstrapped with [Create React App](https://github.com/facebook/create-react-app), and it demonstrates how to connect your React application to API for GraphQL and SQL Database in Microsoft Fabric.

## Prerequisites

- You need to have [Node.js](https://nodejs.org/) installed on your local machine.
- You need an existing Fabric capacity. If you don't, [start a Fabric trial](https://learn.microsoft.com/fabric/get-started/fabric-trial).
- Make sure that you [enable SQL database in Fabric using tenant settings in the Admin Portal](https://learn.microsoft.com/fabric/database/sql/enable).
- [Create a new workspace](https://learn.microsoft.com/fabric/get-started/workspaces) or use an existing Fabric workspace.
- [Create a new SQL database](https://learn.microsoft.com/fabric/database/sql/create) and [load AdventureWorks sample data](https://learn.microsoft.com/fabric/database/sql/load-adventureworks-sample-data). (**Note:** Make sure you have selected and loaded all the tables from AdventureWorks sample data.)
- You must have an API for GraphQL in Fabric that is connected to the Fabric SQL database populated with AdventureWorks sample data. For more information, see [Create GraphQL API from your SQL database in the Fabric](https://learn.microsoft.com/fabric/database/sql/graphql-api).

## Setup instructions

- Clone the repository in your targeted folder:

```sh
cd <targeted-repository-folder>
git clone https://github.com/microsoft/fabric-samples.git

```

- Navigate to the root directory to install **server** dependencies:

```sh
npm install
```

- Navigate to the client directory to install **client** dependencies:

```sh
cd  client
npm install
```

- Update the GraphQL endpoint in **index.js** file for server, which is the entry point for the server application:

    1) Obtain your GraphQL API endpoint by selecting **Copy endpoint** from the ribbon on the GraphQL API homepage.

    2) Open the **index.js** file in the server directory.

    3) Locate the line referencing the GraphQL endpoint and replace it with your copied endpoint.

## Run the application

- **Start the server:**

    From the root directory, start the node.js server first which will be in development mode. You will need to provide log in credentials that have access to the GraphQL API to log in:

```sh
npm start
```

- **Start the client:**

    Navigate to `client` directory and start the client to run the application in development mode:

```sh
cd client
npm start
```

Open [http://localhost:3000](http://localhost:3000) to view it in your browser.\
The page will reload when you make changes.\
You may also see any lint errors in the console.

## Other available scripts

### `npm run build`

`npm run build`: Builds the application for production to the `build` folder. It correctly bundles React in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes.\
Your application is ready to be deployed!

See the section about [deployment](https://facebook.github.io/create-react-app/docs/deployment) for more information.

### `npm run eject`

`npm run eject`: Removes the single build dependency from your project.

If you aren't satisfied with the build tool and configuration choices, you can `eject` at any time. This command will remove the single build dependency from your project.

This will copy all the configuration files and the transitive dependencies (webpack, Babel, ESLint, etc) right into your project so you have full control over them. All of the commands except `eject` will still work, but they will point to the copied scripts so you can tweak them. At this point you're on your own.

You don't have to ever use `eject`. The curated feature set is suitable for small and middle deployments, and you shouldn't feel obligated to use this feature. However, we understand that this tool wouldn't be useful if you couldn't customize it when you are ready for it.
> [!NOTE]
> **`npm run eject` is a one-way operation. Once you `eject`, you can't go back!**

## Learn more

- Learn more about the Create React App by checking out [this documentation](https://facebook.github.io/create-react-app/docs/getting-started).

- Explore the [React documentation](https://reactjs.org/) to dive deeper into React.

- Connect a production React App to a Fabric API for GraphQL by checking out the [connect applications to Fabric API for GraphQL](https://learn.microsoft.com/en-us/fabric/data-engineering/connect-apps-api-graphql).

- Troubleshoot issues if `npm run build` fails to minify by referring to the [troubleshooting guide](https://facebook.github.io/create-react-app/docs/troubleshooting#npm-run-build-fails-to-minify)

- Understand how to create [a progressive web app](https://facebook.github.io/create-react-app/docs/making-a-progressive-web-app)

- Customize your application with [advanced configuration](https://facebook.github.io/create-react-app/docs/advanced-configuration)

- Deploy your application by checking out the [deployment guide](https://facebook.github.io/create-react-app/docs/deployment)