import React from 'react';

export interface SidebarContextState {
    isShown: boolean;
    handleSidebar: (bool?: boolean) => void;
}

export const useSidebar = () => {
    const [isShown, setShown] = React.useState<boolean>(true);

    const handleSidebar = (bool?: boolean) => {
        bool ? setShown(bool) : setShown(!isShown);
    };

    return { isShown, handleSidebar };
};
