import React from "react";
import { Bar } from "react-chartjs-2";
import '../Styles/ChartStyles.css';
import { Chart, CategoryScale, LinearScale, BarElement, Tooltip, PointElement, LineElement } from 'chart.js';

Chart.register(
    CategoryScale,
    LinearScale,
    BarElement,
    PointElement,
    LineElement,
    Tooltip,
);

const BarChart = ({ responseData, loadStatus }) => {
    const [products, setProducts] = React.useState([]);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState(false);

    React.useEffect(() => {
        if (!loadStatus) {
            setLoading(false);
            if (typeof responseData === 'object' && responseData !== null && responseData.products) {
                setProducts(responseData.products.items);
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

    const listPrices = products.map(product => product.ListPrice);
    const productLabels = products.map(product => product.Name);

    const chartData = {
        labels: productLabels,
        datasets: [
            {
                label: 'List Price',
                data: listPrices,
                backgroundColor: ['#6f42c1', '#73ed8f', '#3a96f9', '#636567', '#ce69e7'], // Customize the bar color
            },
        ],
    };

    const chartOptions = {
        scales: {
            y: {
                beginAtZero: true, // Start the scale at zero
                ticks: {
                    callback: function (value) {
                        return '$' + value; // Add a dollar sign to the tick labels
                    }
                }
            }
        }
    };

    return (
        <div className="custom-pie-chart">
            <Bar data={chartData} options={chartOptions} />
            <h6>Top 10 Products by List Price</h6>
        </div>
    );
};

export default BarChart;