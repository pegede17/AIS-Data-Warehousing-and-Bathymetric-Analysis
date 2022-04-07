import React from 'react';
import { Button } from 'react-bootstrap';
import styled from "styled-components";
import {SidebarContext} from "../contexts/sidebarContext";
import '../styles/chartType.scss';
import ListButton from './ListButton';

const Sidebar: React.FC = () => {
    const {isShown, handleSidebar} = React.useContext(SidebarContext);
    const shipListName = "Ship Types";
    const shipList = ["Sailing", "Pleasure", "Cargo", "Passenger", "Military"];
    
    const aisListName = "AIS Transponder Type";
    const aisTypes = ["Type A", "Type B"];

    const gridListName = "Grid Size";
    const gridList = ["50 Meters", "100 Meters", "500 Meters", "1000 Meters"];

    // Old classname 'bg-black text-white sidebar
    return (
        <SidebarContainer className={'bg-white text-black sidebar'}>
            <button className={'chart-type'} onClick={() => handleSidebar()}>Hide sidebar</button>
            <Button style={{background: "#FF8C21", color: "black"}}>Chart Type</Button>

            <p>Starting date</p>
            <p>End date</p>
            <ListButton listItems={shipList} listName={shipListName}/>
            <ListButton listItems={aisTypes} listName={aisListName}/>
            <ListButton listItems={gridList} listName={gridListName}/>

            <Button style={{background: "#FF8C21", color: "black",}}>Update Map</Button>
        </SidebarContainer>
    );
};

const SidebarContainer = styled.div`
  height: 100%;
  width: 100%;
`

export default Sidebar;