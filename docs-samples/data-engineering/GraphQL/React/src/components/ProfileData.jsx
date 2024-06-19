import React from "react";
import ListGroup from 'react-bootstrap/ListGroup'; 
import Table from 'react-bootstrap/Table';
/**
 * Renders information about the user obtained from MS Graph 
 * @param props
 */
export const ProfileData = (props) => {
  const holidays = props.graphqlData.data.publicholidays.items;
  return (
    <Table striped bordered hover responsive>
    <thead>
      <tr>
        <th>Country</th>
        <th>Holiday</th>
        <th>Date</th>
      </tr>
    </thead>
    <tbody>
      {holidays.map((item,i) => (
      <tr key={i}>
        <td>{item.countryOrRegion}</td>
        <td>{item.holidayName}</td>
        <td>{item.date}</td>
      </tr>
      ))}
      </tbody>
    </Table>
)};