import React from 'react';
import {LatLngBounds} from "leaflet";
import {Feature, FeatureCollection} from "geojson";
import API from "../utils/API";
import {AxiosError} from "axios";
import {useSnackbar} from "notistack";
import data from "../data/some_real_data.json";
import {RasterRequestParameters} from "../models/Requests";

export type CustomFeature = Feature & { properties: { draught?: number; count?: number; } }

export interface MapDetails {
    zoomLevel?: number;
    bounds?: LatLngBounds;
    selectedProperties?: CustomFeature | undefined;
    mapLoading: boolean;
    mapData: FeatureCollection;
    viewportChanged: boolean;

    // Dispatch methods
    setZoom: (zoom?: number) => void;
    setBounds: (bounds?: LatLngBounds) => void;
    setSelectedProperty: (feature?: CustomFeature) => void;
    setMapLoading: (state: boolean) => void;
    updateMapData: () => void;
    setViewportChanged: (state: boolean) => void;
}

export const useMapDetails = () => {
    const {enqueueSnackbar} = useSnackbar();
    const [zoomLevel, setZoom] = React.useState<number | undefined>(undefined);
    const [bounds, setBounds] = React.useState<LatLngBounds | undefined>(undefined);
    const [selectedProperties, setSelectedProperty] = React.useState<CustomFeature | undefined>(undefined);
    const [mapLoading, setMapLoading] = React.useState<boolean>(false);
    const [mapData, setMapData] = React.useState<FeatureCollection>(data as FeatureCollection); // TODO: Remove static data as default
    const [viewportChanged, setViewportChanged] = React.useState<boolean>(false);

    const updateMapData = () => {
        setMapLoading(true);
        setViewportChanged(false);

        if (bounds) {
            const params: RasterRequestParameters = {
                "northEastLat": bounds.getNorthEast().lat,
                "northEastLong": bounds.getNorthEast().lng,
                "southWestLat": bounds.getSouthWest().lat,
                "southWestLong": bounds.getSouthWest().lng
            }

            API.map.getBoxesTesting(params)
                .then(response => {
                    setMapLoading(false);
                    setMapData(response.data);
                })
                .catch((error: AxiosError) => {
                    setMapLoading(false);
                    enqueueSnackbar(`Error occurred: ${error.message} (${error.code})`, {variant: 'error'});
                });
        } else {
            // TODO: Error handle here if something is wrong with bounds
            enqueueSnackbar(`Error occurred: Bounds cannot be undefined`, {variant: 'error'});
            setMapLoading(false);
        }
    }

    return {
        zoomLevel,
        bounds,
        setZoom,
        setBounds,
        selectedProperties,
        setSelectedProperty,
        mapLoading,
        setMapLoading,
        mapData,
        updateMapData,
        viewportChanged,
        setViewportChanged
    };
};
