import React from 'react';
import {LatLngBounds} from "leaflet";
import {Feature} from "geojson";

export type CustomFeature = Feature & { properties: { max?: number; count?: number; } }

export interface MapDetails {
    zoomLevel?: number;
    bounds?: LatLngBounds;
    selectedProperties?: CustomFeature | undefined;

    // Dispatch methods
    setZoom: (zoom: number) => void;
    setBounds: (bounds: LatLngBounds) => void;
    setSelectedProperty: (ft: CustomFeature | undefined) => void;
}

export const useMapDetails = () => {
    const [zoomLevel, setZoomLevel] = React.useState<number | undefined>(undefined);
    const [bounds, setBoundsState] = React.useState<LatLngBounds | undefined>(undefined);
    const [selectedProperties, setSelectedProperties] = React.useState<CustomFeature | undefined>(undefined);

    const setZoom = (zoom: number) => setZoomLevel(zoom);
    const setBounds = (bounds: LatLngBounds) => setBoundsState(bounds);
    const setSelectedProperty = (ft: CustomFeature | undefined) => setSelectedProperties(ft);

    return {
        zoomLevel,
        bounds,
        setZoom,
        setBounds,
        selectedProperties,
        setSelectedProperty
    };
};
