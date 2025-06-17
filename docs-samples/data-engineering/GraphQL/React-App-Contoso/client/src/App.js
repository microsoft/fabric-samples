import './Styles//App.css';
import './Styles/AppStyles.css';
import React, { useState } from "react";
import PieChartComponent from "./Components/GraphQLPieChartComponent";
import BarChart from "./Components/GraphQLBarChartComponent";
import MyCustomTable from "./Components/GraphQLTableGridComponent";
import { PageLayout } from "./PageLayout";

const GraphQLContent = () => {
    
  const [loading, setLoading] = React.useState(true);
  const [data, setData] = React.useState(null);

  React.useEffect(() => {
      // Fetch data from an API endpoint (e.g., JSON Placeholder API)
           fetch("/graphql")
            .then((res) => res.json())
              .then((response) => setData(response.data))
          .catch((error) => {
              setData(error.message);
          })
          .finally(() => {
              setLoading(false);
          });
      });
  return (
      <>
          <h5 className="card-title">Welcome to Contoso Outdoors Portal</h5>
              <div className="horizontal-container">
                  <BarChart responseData={data} loadStatus={loading} />
                  <PieChartComponent responseData={data} loadStatus={loading} />
              </div>
              <div className="horizontal-container">
                  <MyCustomTable responseData={data} loadStatus={loading} />
              </div>
          
      </>
  );
};

export default function App() {
  return (
    <PageLayout>
    <div className="App">     
            <GraphQLContent />
    </div>
    </PageLayout>
  );
}