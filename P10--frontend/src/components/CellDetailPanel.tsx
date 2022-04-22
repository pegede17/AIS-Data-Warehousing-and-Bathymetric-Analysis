import React from 'react';
import {MapDetailsContext} from "../contexts/mapDetailsContext";

const CellDetailPanel: React.FC = () => {
    const {setSelectedProperty, selectedProperties} = React.useContext(MapDetailsContext);

    if (selectedProperties) {
        return (
            <div style={{zIndex: 99999, position: 'absolute', right: '50px', top: '50px', backgroundColor: '#fff'}}
                 className={'p-4 rounded'}>
                <h3>Cell details</h3>
                <p>Max draught: {selectedProperties.properties?.max ? selectedProperties.properties?.max : 'None'}</p>
                <p>Trajectory count: {selectedProperties.properties?.count}</p>
                <button className={'btn btn-secondary'} onClick={() => setSelectedProperty(undefined)}>Hide</button>
            </div>
        )
    }

    return null;
};

export default CellDetailPanel;