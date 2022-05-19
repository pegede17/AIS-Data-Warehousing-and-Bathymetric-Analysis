import React from 'react';
import {LatLngBounds} from "leaflet";
import {Feature, FeatureCollection} from "geojson";
import API from "../utils/API";
import {AxiosError} from "axios";
import {useSnackbar} from "notistack";
import {QueryFilters, RasterRequestParameters} from "../models/Requests";
import {aisTypes, defaultGradientColors, shipList} from "../models/FiltersDefaults";

export type CustomFeature =
    Feature
    & { properties: { maxdraught?: number; count?: number; mindraught?: number; cellid?: number; } }

export enum ViewType { "DRAUGHT", "HEATMAP"}

export type GradientColors = {
    colorOne: string;
    colorTwo: string;
}

export interface MapDetails {
    zoomLevel?: number;
    bounds?: LatLngBounds;
    selectedProperties?: CustomFeature | undefined;
    mapLoading: boolean;
    mapData: FeatureCollection | undefined;
    viewportChanged: boolean;
    filtersChanged: boolean;
    viewType: ViewType;
    gradientColors: GradientColors;

    // Dispatch methods
    setZoom: (zoom?: number) => void;
    setBounds: (bounds?: LatLngBounds) => void;
    setSelectedProperty: (feature?: CustomFeature) => void;
    setMapLoading: (state: boolean) => void;
    updateMapData: () => void;
    setViewportChanged: (state: boolean) => void;
    setFiltersChanged: (state: boolean) => void;
    setViewType: (type: ViewType) => void;
    setGradientColors: (color: GradientColors) => void;

    // Filters
    filters: QueryFilters;
    setFilters: (filter: QueryFilters) => void;
}

export const useMapDetails = () => {
    const {enqueueSnackbar} = useSnackbar();
    const [zoomLevel, setZoom] = React.useState<number | undefined>(undefined);
    const [bounds, setBounds] = React.useState<LatLngBounds | undefined>(undefined);
    const [selectedProperties, setSelectedProperty] = React.useState<CustomFeature | undefined>(undefined);
    const [mapLoading, setMapLoading] = React.useState<boolean>(false);
    const [mapData, setMapData] = React.useState<FeatureCollection | undefined>(); // TODO: Remove static data as default
    const [viewportChanged, setViewportChanged] = React.useState<boolean>(false);
    const [filtersChanged, setFiltersChanged] = React.useState<boolean>(false);
    const [viewType, setViewType] = React.useState<ViewType>(ViewType.DRAUGHT);
    const [gradientColors, setGradientColors] = React.useState<GradientColors>(defaultGradientColors);

    const [filters, setFilters] = React.useState<QueryFilters>({
        fromDate: "20210501",
        toDate: "20210507",
        shipTypes: shipList,
        mobileTypes: aisTypes,
        onlyTrustedDraught: true
    });

    const updateMapData = () => {
        setMapLoading(true);
        setViewportChanged(false);
        setFiltersChanged(false);

        if (bounds) {
            const params: RasterRequestParameters = {
                ...filters,
                "northEastLat": bounds.getNorthEast().lat,
                "northEastLong": bounds.getNorthEast().lng,
                "southWestLat": bounds.getSouthWest().lat,
                "southWestLong": bounds.getSouthWest().lng,
                "zoomLevel": zoomLevel!
            }
            console.log(params)
            API.map.getBoxes(params)
                .then(response => {
                    setMapLoading(false);
                    setMapData(response.data);
                    if (!response.data.features) {
                        enqueueSnackbar('No data found with the parameters')
                    }
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
        setViewportChanged,
        filters,
        setFilters,
        filtersChanged,
        setFiltersChanged,
        viewType,
        setViewType,
        gradientColors,
        setGradientColors
    };
};
