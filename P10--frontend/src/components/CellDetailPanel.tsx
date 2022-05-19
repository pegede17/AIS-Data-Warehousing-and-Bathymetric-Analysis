import React from 'react';
import {MapDetailsContext} from "../contexts/mapDetailsContext";
import CellDetails from './CellDetails';
import {Box, Typography} from "@mui/material";

const CellDetailPanel: React.FC = () => {
    const {setSelectedProperty, selectedProperties} = React.useContext(MapDetailsContext);

    // TODO: make recorded draughts component
    // TODO: HOOK with api call to get draught details

    if (selectedProperties) {
        console.log(selectedProperties);

        return (
            <Box style={{zIndex: 1200, position: 'absolute', right: '50px', top: '50px', width: '30vh'}}
                 sx={{
                     backdropFilter: 'blur(6px)',
                     backgroundColor: 'rgba(255, 255, 255, 0.8)',
                     boxShadow: '5px 0px 50px rgba(0, 0, 0, 0.15)',
                     borderRadius: '0px 15px 15px 0px',
                 }}
                 className={'p-4 rounded'}>

                <Typography variant={'h6'} sx={{pb: 2, fontWeight: 'bold', color: '#4f7ffe', textAlign: 'center'}}>
                    Cell Details
                </Typography>

                {/* <h6>Recorded draughts</h6> */}
                {/* <p>Max draught: {selectedProperties.properties?.draught ? selectedProperties.properties?.draught : 'None'}</p> */}
                <CellDetails
                    minimumDraught={selectedProperties.properties.mindraught ?? 0}
                    averageDraught={Math.round(selectedProperties.properties.avgdraught * 100) / 100 ?? 0}
                    maximumDraught={selectedProperties.properties.maxdraught ?? 0}
                    shipsRecorded={selectedProperties.properties.count!}
                    cellId={selectedProperties.properties.cellid!}>
                </CellDetails>
                <button className={'btn btn-secondary'} onClick={() => setSelectedProperty(undefined)}>Hide</button>
            </Box>
        )
    }

    return null;
};

export default CellDetailPanel;
