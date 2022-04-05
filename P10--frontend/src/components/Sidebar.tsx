import React from 'react';
import styled from "styled-components";
import {SidebarContext} from "../contexts/sidebarContext";
import '../styles/chartType.scss';
import ListButton from './ListButton';

const Sidebar: React.FC = () => {
    const {isShown, handleSidebar} = React.useContext(SidebarContext);
    const shipList = ["Sailing", "Pleasure", "Cargo", "Passenger", "Military"]
    const shipListName = "Ship Types"

    return (
        <SidebarContainer className={'bg-dark text-white sidebar'}>
            <button className={'chart_type'} onClick={() => handleSidebar()}>Hide sidebar</button>

            <p>Sidebar content</p>
            <p>Round choose list with trajectorioes or Depth charts</p>

            <p>Choose starting and end date</p>
            <p>Ship types - expandable list</p>
            <ListButton listItems={shipList} listName={shipListName}/>
            <p>AIS type - expandable list</p>
            <p>Grid Size - expandable list. 50, 100, 500, 1000</p>
            <p>Checkbox include null values</p>

            <p>Fetch button</p>

        </SidebarContainer>
    );
};

const SidebarContainer = styled.div`
  height: 100%;
  width: 100%;
`

export default Sidebar;