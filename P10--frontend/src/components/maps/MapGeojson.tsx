import React, {useEffect} from 'react';
import {GeoJSON, MapContainer, TileLayer, ZoomControl} from 'react-leaflet';
import {Layer, LeafletMouseEvent} from "leaflet";
import * as geojson from "geojson";
import MapEventHandler from "../MapEventHandler";
import {MapDetailsContext} from "../../contexts/mapDetailsContext";
import {v4 as uuidv4} from 'uuid';
import {useSnackbar} from "notistack";
import {CustomFeature, ViewType} from "../../hooks/useMapDetails";
import Gradient from "javascript-color-gradient";

const MapGeojson: React.FC = () => {
    const {enqueueSnackbar} = useSnackbar();
    const {
        setSelectedProperty,
        setMapLoading,
        mapLoading,
        mapData,
        updateMapData,
        viewType,
        gradientColors
    } = React.useContext(MapDetailsContext);

    const GradientGenerator = new Gradient()
        .setColorGradient(gradientColors.colorOne, gradientColors.colorTwo)
        .setMidpoint(50); // 50 = 100 colors to pick from (useful for normalization 0-100)

    const [MAX_DATA_VALUE, setMaxRecordedValue] = React.useState<number | null | undefined>(undefined);

    useEffect(() => {
        if (mapData?.features) {
            if (viewType === ViewType.DRAUGHT) {
                setMaxRecordedValue(Math.max(...mapData.features.map(ft => ft.properties?.maxdraught)));
            } else {
                setMaxRecordedValue(Math.max(...mapData.features.map(ft => ft.properties?.count)));
            }
        }
    }, [mapData, viewType]);


    const defaultFeatureStyle = (feature?: geojson.Feature) => {
        const draught = viewType === ViewType.DRAUGHT ? feature?.properties?.maxdraught : feature?.properties?.count;
        let cellColor = "#e440ea"; // Default value (pink) in case draught is missing or max data

        if (MAX_DATA_VALUE) {
            const colorIndex = Math.floor(((draught - 0.1) / (MAX_DATA_VALUE - 0.1) * 100))
            cellColor = colorIndex > 0 ? GradientGenerator.getColor(colorIndex) : GradientGenerator.getColor(1);
        }

        return ({
            fillColor: cellColor,
            fillOpacity: 1,
            opacity: 1,
            stroke: false,
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
