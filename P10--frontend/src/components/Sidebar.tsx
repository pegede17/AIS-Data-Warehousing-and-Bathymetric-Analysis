import React from 'react';
import styled from "styled-components";
import {SidebarContext} from "../contexts/sidebarContext";

const Sidebar: React.FC = () => {
    const {isShown, handleSidebar} = React.useContext(SidebarContext);

    return (
        <SidebarContainer className={'bg-dark text-white sidebar'}>
            <p>Sidebar content</p>

            <button onClick={() => handleSidebar()}>Hide sidebar</button>
        </SidebarContainer>
    );
};

const SidebarContainer = styled.div`
  height: 100%;
  width: 100%;
`

export default Sidebar;