import React from 'react';
import {MapDetailsContext} from "../contexts/mapDetailsContext";
import CellDetails from './CellDetails';

const CellDetailPanel: React.FC = () => {
    const {setSelectedProperty, selectedProperties} = React.useContext(MapDetailsContext);

    // TODO: make recorded draughts component
    // TODO: HOOK with api call to get draught details 

    if (selectedProperties) {
        console.log(selectedProperties)
        return (
            <div style={{zIndex: 99999, position: 'absolute', right: '50px', top: '50px', backgroundColor: '#fff'}}
                 className={'p-4 rounded'}>
                <h3 style={{textAlign: 'center'}}>Cell Details</h3>
                {/* <h6>Recorded draughts</h6> */}
                {/* <p>Max draught: {selectedProperties.properties?.draught ? selectedProperties.properties?.draught : 'None'}</p> */}
                <CellDetails minimumDraught={selectedProperties.properties.mindraught ?? 0} averageDraught={6.6} maximumDraught={selectedProperties.properties.maxdraught ?? 0} shipsRecorded={selectedProperties.properties.count!}>
                </CellDetails>
                <button className={'btn btn-secondary'} onClick={() => setSelectedProperty(undefined)}>Hide</button>
            </div>
        )
    }

    return null;
};

export default CellDetailPanel;
