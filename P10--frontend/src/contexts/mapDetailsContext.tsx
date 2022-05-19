import React from 'react';
import {MapDetails, useMapDetails} from "../hooks/useMapDetails";

const MapDetailsContext = React.createContext({} as MapDetails);

const MapDetailsProvider: React.FC = ({children}) => {
    return (
        <MapDetailsContext.Provider value={useMapDetails()}>
            {children}
        </MapDetailsContext.Provider>
    );
};

export {MapDetailsProvider, MapDetailsContext};
