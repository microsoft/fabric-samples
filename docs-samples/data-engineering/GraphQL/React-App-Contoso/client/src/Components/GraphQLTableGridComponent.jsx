import React, { useState, useEffect } from 'react';
import { Table } from 'react-bootstrap';

const MyCustomTable = ({ responseData, loadStatus }) => {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  function parseCustomerSearchData(data) {
    let dataArray = [];
    if (data.customers && data.customers.items) {
      data.customers.items.forEach(item => {
        dataArray.push({
          CustomerID: item.CustomerID.toString(),
          FirstName: item.FirstName,
          LastName: item.LastName,
          EmailAddress: item.EmailAddress,
          Phone: item.Phone,
          CompanyName: item.CompanyName,
          SalesPerson: item.SalesPerson,
        });
      });
    }
    return dataArray;
  }

  useEffect(() => {
    if (!loadStatus) {
      setLoading(false);
      if (typeof responseData === 'object' && responseData !== null) {
        console.log('Response Data:', responseData); // Log responseData
        const parsedData = parseCustomerSearchData(responseData);
        console.log('Parsed Data:', parsedData); // Log parsed data
        setData(parsedData);
      } else if (typeof responseData === 'string' && responseData !== null) {
        setError(responseData);
      }
    }
  }, [loadStatus, responseData]);

  if (loading) {
    return <p>Loading...</p>;
  }

  if (error) {
    return <p>{error}</p>;
  }

  return (
    <div>
      <h5>Customer Order Details</h5>
      <div className='table-form'></div>
      <Table striped bordered hover style={{ borderColor: '#006D5B' }}>
        <thead>
          <tr>
            <th>CustomerID</th>
            <th>FirstName</th>
            <th>LastName</th>
            <th>EmailAddress</th>
            <th>Phone</th>
            <th>CompanyName</th>
            <th>SalesPerson</th>
          </tr>
        </thead>
        <tbody>
          {data.map((row) => (
            <tr key={row.CustomerID}>
              <td>{row.CustomerID}</td>
              <td>{row.FirstName}</td>
              <td>{row.LastName}</td>
              <td>{row.EmailAddress}</td>
              <td>{row.Phone}</td>
              <td>{row.CompanyName}</td>
              <td>{row.SalesPerson}</td>
            </tr>
          ))}
        </tbody>
      </Table>
    </div>
  );
};

export default MyCustomTable;