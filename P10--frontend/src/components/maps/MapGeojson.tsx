import React from 'react';
import {GeoJSON, MapContainer, TileLayer, ZoomControl} from 'react-leaflet';
import {Layer, LeafletMouseEvent} from "leaflet";
import * as geojson from "geojson";
import MapEventHandler from "../MapEventHandler";
import {MapDetailsContext} from "../../contexts/mapDetailsContext";
import {v4 as uuidv4} from 'uuid';
import {useSnackbar} from "notistack";
import {ViewType} from "../../hooks/useMapDetails";

const MapGeojson: React.FC = () => {
    const {enqueueSnackbar} = useSnackbar();
    const {
        setSelectedProperty,
        setMapLoading,
        mapLoading,
        mapData,
        updateMapData,
        viewType
    } = React.useContext(MapDetailsContext);


    const defaultFeatureStyle = (feature?: geojson.Feature) => {
        // TODO: Skift mellem dybde og varmekort (Draught/count)
        let draught;
        if (viewType === ViewType.DRAUGHT) {
            draught = feature?.properties?.maxdraught;
        } else {
            draught = feature?.properties?.count;
        }
        const bgColor = draught ? '#000' : '#e440ea';
        const opacity = draught ? 1 - (1 / draught) : 1;

        return ({
            fillColor: bgColor,
            fillOpacity: opacity,
            opacity: opacity,
            color: '#526579',
            stroke: false
        });
    }

    const selectRegion = (e: LeafletMouseEvent) => {
        setSelectedProperty(e.target.feature);
    }

    const setFeatureHighlight = (e: LeafletMouseEvent) => {
        e.target.setStyle({
            fillColor: "#4f7ffe",
            weight: 1,
            opacity: 0.5,
            color: '#526579',
            dashArray: '5',
            fillOpacity: 0.5,
            stroke: true
        });
    }

    const onEachAction = (feature: any, layer: Layer) => {
        layer.on({
            mousedown: selectRegion,
            click: setFeatureHighlight,
        })
    }

    return (
        <MapContainer
            center={[56.00, 11.08]}
            zoom={9}
            preferCanvas={true}
            svg={false}
            zoomControl={false}
        >
            <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                url="https://cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png"
            />
            <ZoomControl position={"bottomright"}/>

            <MapEventHandler/>

            {mapData?.features &&
            <GeoJSON key={uuidv4()} data={mapData}
                     onEachFeature={onEachAction}
                     style={(ft) => defaultFeatureStyle(ft)}
            />
            }

        </MapContainer>
    );
};

export default MapGeojson;
