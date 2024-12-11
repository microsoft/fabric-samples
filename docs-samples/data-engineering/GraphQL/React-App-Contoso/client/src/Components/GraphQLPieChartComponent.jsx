import React from "react";
import { Pie } from "react-chartjs-2";
import '../Styles/PieStyles.css';
import { Chart, ArcElement } from 'chart.js'; // Import ArcElement and Chart

Chart.register(ArcElement); // Register ArcElement

const PieChartComponent = ({ responseData, loadStatus }) => {
  const [addresses, setAddresses] = React.useState([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState(false);

  React.useEffect(() => {
    if (!loadStatus) {
      setLoading(false);
      if (typeof responseData === 'object' && responseData !== null && responseData.addresses && responseData.addresses.items) {
        setAddresses(responseData.addresses.items);
      } else if (typeof responseData === 'string' && responseData !== null) {
        setError(responseData);
      }
    }
  }, [loadStatus, responseData]);

  if (loading) {
    return <p>Loading...</p>;
  }

  if (error) {
    return <p>Error while loading.</p>;
  }

  // Extract relevant data for the chart (e.g., state provinces)
  const stateProvinces = addresses.map(address => address.StateProvince);

  // Count occurrences of each state province
  const stateCounts = stateProvinces.reduce((acc, state) => {
    acc[state] = (acc[state] || 0) + 1;
    return acc;
  }, {});

  const chartData = {
    labels: Object.keys(stateCounts),
    datasets: [
      {
        label: 'State Provinces',
        data: Object.values(stateCounts),
        backgroundColor: ['#6f42c1', '#73ed8f', '#3a96f9', '#636567', '#ce69e7', '#ff6384', '#36a2eb', '#ffce56', '#4bc0c0', '#9966ff'], // Customize the colors
      },
    ],
  };

  return (
    <div className="custom-my-pie-chart">
      <Pie data={chartData} />
      <h6>Number of Sales by State Provinces</h6>
    </div>
  );
};

export default PieChartComponent;