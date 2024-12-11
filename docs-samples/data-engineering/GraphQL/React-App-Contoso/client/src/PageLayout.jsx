/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 */

import React from "react";
import Navbar from "react-bootstrap/Navbar";
import logo from "./Images/contoso.png"

/**
 * Renders the navbar component 
 * @param props 
 */
export const PageLayout = (props) => {
    return (
        <>
            <Navbar variant="dark" style={{ backgroundColor: '#006D5B' }}>
                <a className="navbar-brand" href="/">Contoso Outdoors</a>
            </Navbar>
            <img src={logo} alt="contoso_logo"></img>
            <br />
            <br />
            {props.children}
        </>
    );
};
