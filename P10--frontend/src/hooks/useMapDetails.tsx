import React from 'react';
import {LatLngBounds} from "leaflet";

export interface MapDetails {
    zoomLevel?: number;
    bounds?: LatLngBounds;

    // Dispatch methods
    setZoom: (zoom: number) => void;
    setBounds: (bounds: LatLngBounds) => void;
}

export const useMapDetails = () => {
    const [zoomLevel, setZoomLevel] = React.useState<number | undefined>(undefined);
    const [bounds, setBoundsState] = React.useState<LatLngBounds | undefined>(undefined);

    const setZoom = (zoom: number) => setZoomLevel(zoom);
    const setBounds = (bounds: LatLngBounds) => setBoundsState(bounds);

    return {
        zoomLevel,
        bounds,
        setZoom,
        setBounds
    };
};
