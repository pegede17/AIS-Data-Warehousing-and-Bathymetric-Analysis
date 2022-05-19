import React from 'react';
import Container from "react-bootstrap/Container";
import styled from "styled-components";
import Sidebar from "../components/Sidebar";
import SidebarFloatingButton from "../components/SidebarFloatingButton";
import MapGeojson from "../components/maps/MapGeojson";
import CellDetailPanel from "../components/CellDetailPanel";
import MapFetchIndicator from "../components/MapFetchIndicator";

const MainLayout = () => {

    return (
        <OnePageContainer className={'position-relative'}>
            <Container fluid className={'h-100 p-0'}>

                <MapFetchIndicator />

                <CellDetailPanel />

                <Sidebar/>

                <div className={'p-0 m-0 w-100 h-100'}>
                    <MapGeojson/>
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
