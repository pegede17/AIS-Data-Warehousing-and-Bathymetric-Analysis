import React from 'react';
import {SidebarContextState, useSidebar} from "../hooks/useSidebar";

let SidebarContext: React.Context<SidebarContextState>;
const {Provider} = (SidebarContext = React.createContext(
    {} as SidebarContextState,
));

const SidebarProvider: React.FC = ({children}) => {
    const {
        isShown,
        handleSidebar
    } = useSidebar();

    return (
        <Provider
            value={{isShown, handleSidebar}}
        >
            {children}
        </Provider>
    );
};

export {SidebarContext, SidebarProvider};
