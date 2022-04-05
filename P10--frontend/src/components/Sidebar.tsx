import React from 'react';
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
    const gridList = [50, 100, 500, 1000];

    return (
        <SidebarContainer className={'bg-dark text-white sidebar'}>
            <button className={'chart-type'} onClick={() => handleSidebar()}>Hide sidebar</button>

            <p>Round choose list with trajectorioes or Depth charts</p>

            <p>Starting date</p>
            <p>End date</p>
            <ListButton listItems={shipList} listName={shipListName}/>
            <ListButton listItems={aisTypes} listName={aisListName}/>
            <ListButton listItems={gridList} listName={gridListName}/>

            <p>Fetch button</p>

        </SidebarContainer>
    );
};

const SidebarContainer = styled.div`
  height: 100%;
  width: 100%;
`

export default Sidebar;