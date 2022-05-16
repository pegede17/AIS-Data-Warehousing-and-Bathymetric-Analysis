import React from 'react';
import {MapDetailsContext} from "../contexts/mapDetailsContext";
import {ScaleLoader} from "react-spinners";
import Button from "@mui/material/Button";
import * as muiSidebarStyling from "../styles/muiSidebarStyling";

const MapFetchIndicator = () => {
    const {mapLoading, setMapLoading, updateMapData, viewportChanged, filtersChanged} = React.useContext(MapDetailsContext);

    return (
        <div className={'position-absolute top-0 start-50 translate-middle'} style={{zIndex: '9999'}}>
            <div className={'d-flex align-items-top'} style={{marginTop: '4rem'}}>
                {(!mapLoading && (viewportChanged || filtersChanged)) &&
                <>
                    <Button sx={{py: 1, px: 3, ...muiSidebarStyling.buttonRevertStyle}} onClick={() => {
                        updateMapData();
                    }}>
                        Refetch map data
                    </Button>
                </>
                }

                {mapLoading &&
                <>
                    <ScaleLoader color={"#4f7ffe"} height={15} width={5}/>
                    <span className={'mx-2'}>Fetching map data</span>
                </>
                }
            </div>
        </div>
    )
};

export default MapFetchIndicator;
