import React from 'react';
import {IconButton} from "@mui/material";
import MenuIcon from '@mui/icons-material/Menu';
import {SidebarContext} from "../contexts/sidebarContext";

const SidebarFloatingButton: React.FC = () => {
    const {isShown, handleSidebar} = React.useContext(SidebarContext);

    if (!isShown) {
        return (
            <div className={'position-absolute top-0'}>
                <IconButton size={"large"}
                            sx={{ml: 4, mt: 3, color: '#4f7ffe', border: 1, borderColor: '#4f7ffe', boxShadow: 1}}
                            style={{zIndex: '9999'}}
                            onClick={() => handleSidebar()}>
                    <MenuIcon/>
                </IconButton>
            </div>
        )
    }

    return null;
};

export default SidebarFloatingButton;
