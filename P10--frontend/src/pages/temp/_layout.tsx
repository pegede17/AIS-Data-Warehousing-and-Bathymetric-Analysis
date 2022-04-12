import React from 'react';
import Container from "react-bootstrap/Container";
import styled from "styled-components";
import MapExample from "./MapExample";
import Sidebar from "../../components/Sidebar";
import {SidebarContext} from "../../contexts/sidebarContext";
import SidebarFloatingButton from "../../components/SidebarFloatingButton";

const MainLayout = () => {

    return (
        <OnePageContainer className={'position-relative'}>
            <Container fluid className={'h-100 p-0'}>

                <Sidebar/>

                <div className={'p-0 m-0 w-100 h-100'}>
                    <MapExample/>
                </div>
            </Container>

            <SidebarFloatingButton/>

        </OnePageContainer>
    );
};

const OnePageContainer = styled.section`
  width: 100vw;
  height: 100vh;
`

export default MainLayout;
