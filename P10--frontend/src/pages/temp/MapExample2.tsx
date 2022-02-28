import React from 'react';
import {MapContainer, Marker, Popup, TileLayer, Rectangle, FeatureGroup} from 'react-leaflet';
import data from '../../data/constraints.json';
import {LatLng, LatLngBounds} from "leaflet";

import L from "leaflet";
import {
    createTileLayerComponent,
    updateGridLayer,
    withPane,
} from "@react-leaflet/core";

// @ts-ignore
import geojsonvt from 'geojson-vt';
// @ts-ignore
window.geojsonvt = geojsonvt;
// @ts-ignore
import {} from "leaflet-geojson-vt";

const MapExample2: React.FC = () => {
    // @ts-ignore
    const GeoJSONVtLayer = createTileLayerComponent(function createGridLayer(
        { geoJSON, ...options },
        context
        ) {
            return {
                instance: L.gridLayer.geoJson(geoJSON, withPane(options, context)),
                context
            };
        },
        updateGridLayer);


    return (
        <MapContainer
            center={[57.01228, 9.9917]}
            zoom={13}
        >
            <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />



        </MapContainer>
    );
};

export default MapExample2;
