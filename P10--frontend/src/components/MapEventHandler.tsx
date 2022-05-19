import React from 'react';
import {useMapEvents} from "react-leaflet";
import {MapDetailsContext} from "../contexts/mapDetailsContext";

const MapEventHandler: React.FC = () => {
    const {
        zoomLevel,
        bounds,
        setZoom,
        setBounds,
        setViewportChanged,
        viewportChanged
    } = React.useContext(MapDetailsContext);

    const mapEvents = useMapEvents({
        zoomend: (event) => {
            setZoom(event.target.getZoom());
        },
        moveend: (event) => {
            setBounds(event.target.getBounds());
        },
        move: () => {
            if (!viewportChanged) {
                setViewportChanged(true);
            }
        }
    })

    /*
    console.log('map center:', map.getZoom())
    console.log('map center:', map.getBounds())
    console.log('map center:', map.getPixelBounds())
    */

    // TODO: This is only temporary.
    //  In theory this implementation is stupid because we use context instead of directly ref. :D
    return (
        <div style={{zIndex: 99999, position: 'absolute', left: '350px', bottom: '100px'}}>
            <h2>Zoom: {zoomLevel}</h2>
            <label>Viewport details (lat, long)</label>
            <p>NE: ({bounds?.getNorthEast().lat} , {bounds?.getNorthEast().lng}) <br/>
                SW: ({bounds?.getSouthWest().lat} , {bounds?.getSouthWest().lng}) </p>
        </div>
    );
};

export default MapEventHandler;
