import React from 'react';
import Container from "react-bootstrap/Container";
import styled from "styled-components";
import MapExample from "./MapExample";
import Sidebar from "../../components/Sidebar";
import {SidebarContext} from "../../contexts/sidebarContext";
import DoubleArrowIcon from '@mui/icons-material/DoubleArrow';
import { IconButton } from '@mui/material';

const MainLayout = () => {
    const {isShown, handleSidebar} = React.useContext(SidebarContext);

    return (
        <OnePageContainer className={'position-relative'}>
            <Container fluid className={'h-100'}>

                <div className={'row h-100'}>
                    <div className={(isShown ? 'col-2' : 'd-none') + ' p-0'}>
                        <Sidebar/>
                    </div>

                    <div className={(isShown ? 'col-10' : 'col-12') + ' p-0 m-0'}>
                        <MapExample/>
                    </div>
                </div>
            </Container>

            {!isShown &&
                <IconButton className={'p-0 position-absolute top-0'} style={{zIndex: '9999'}}
                onClick={() => handleSidebar()}>
                    <DoubleArrowIcon sx={{color: '#201513'}}/>
                </IconButton>
            }
        </OnePageContainer>
    );
};

const OnePageContainer = styled.section`
  width: 100vw;
  height: 100vh;
`

export default MainLayout;