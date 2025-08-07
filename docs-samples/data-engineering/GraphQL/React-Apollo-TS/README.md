# React Apollo TypeScript App

A minimal React TypeScript application with Apollo GraphQL Client.

## Features

- ⚛️ **React 18** with TypeScript
- ⚡ **Vite** for fast development and building
- 🚀 **Apollo Client** for GraphQL operations
- 🎨 **CSS** styling with modular approach
- 🔧 **TypeScript** for type safety

## Quick Start

### Prerequisites

- Node.js (version 16 or higher)
- npm or yarn

### Installation

1. Install dependencies:
```bash
npm install
```

2. Create Fabric GraphQL Endpoint:
   - Step 1:
      - We get started by creating a SQL database in Fabric:
         - In your Fabric workspace, select New Item then SQL database (preview).
         - Give your database a name, then select Sample data to quickly create all the required tables and data in your database.
   - Step 2:
      - Create a GraphQL API:
         - Creating an API from your SQL table is fast, easy, and straightforward. You just need to click the New API for GraphQL button in the ribbon and give your API a name.
         - Next select all the `SalesLT` tables in your database, then click Load

3. Configure your GraphQL API:
   - Open `src/authConfig.ts`
   - Replace `'https://your-graphql-endpoint.com/graphql'` with your actual GraphQL endpoint
   - Add authentication related config in the `AUTH_CONFIG` constant following the steps in the "Create a Microsoft Entra app" section in the [documentation](https://learn.microsoft.com/en-us/fabric/data-engineering/connect-apps-api-graphql#create-a-microsoft-entra-app).

4. Update the GraphQL schema:
   - Replace `schema.graphql` with your actual GraphQL schema
   - Update the queries in `src/App.tsx` to match your schema
   - for codegen, Update the `src/operations/queries.graphql` to match your schema

### Development

### Build

```bash
npm run build
```

### Start the development server:
```bash
npm run dev
```

The app will open at [http://localhost:3000](http://localhost:3000)

### Build

```bash
npm run build
```

The build artifacts will be stored in the `build/` directory.

### Preview production build:
```bash
npm run preview
```

## Project Structure

```
 src/
    ├── index.tsx          # App entry point
    ├── App.tsx            # Main app component
    ├── App.css            # App styles
    ├── components/        # React components (create as needed)
    ├── operations/        # GraphQL queries and mutations (create as needed)
  graphqlrc.yml            # Intellisense and auto completion config (Update if needed)
  codegen.yml              # Codegen config file (update if needed)
```

## GraphQL client Setup

### Apollo Client Configuration

The Apollo Client is configured in `src/Client.ts` with:
- GraphQL endpoint URL
- In-memory cache
- Authentication headers (commented out by default)


## Technologies Used

- [React](https://reactjs.org/) - UI library
- [Vite](https://vitejs.dev/) - Build tool and dev server
- [TypeScript](https://www.typescriptlang.org/) - Type safety
- [Apollo Client](https://www.apollographql.com/docs/react/) - GraphQL client
- [GraphQL](https://graphql.org/) - Query language for APIs


