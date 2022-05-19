import React from 'react';
import {Histogram, useHistogram} from "../hooks/useHistogram";

const HistogramContext = React.createContext({} as Histogram);

const HistogramProvider: React.FC = ({children}) => {
    return (
        <HistogramContext.Provider value={useHistogram()}>
            {children}
        </HistogramContext.Provider>
    );
};

export {HistogramProvider, HistogramContext};
