import React from 'react';
import {GeoJSON, MapContainer, TileLayer, ZoomControl} from 'react-leaflet';
import {Layer, LeafletMouseEvent} from "leaflet";
import * as geojson from "geojson";
import MapEventHandler from "../MapEventHandler";
import {MapDetailsContext} from "../../contexts/mapDetailsContext";
import {v4 as uuidv4} from 'uuid';
import {useSnackbar} from "notistack";

const MapGeojson: React.FC = () => {
    const {enqueueSnackbar} = useSnackbar();
    const {
        setSelectedProperty,
        setMapLoading,
        mapLoading,
        mapData,
        updateMapData
    } = React.useContext(MapDetailsContext);


    /*
    React.useEffect(() => {
        // setMapLoading(true);

        API.map.getBoxesTesting()
            .then(response => {
                console.log("Test1")
                // setMapLoading(false);
                console.log(mapLoading);
                setMapData(response.data);
            })
            .catch((error: AxiosError) => {
                console.log("Test")
                console.log(mapLoading);
                // setMapLoading(false);
                enqueueSnackbar(`Error occurred: ${error.message} (${error.code})`, {variant: 'error'});
            });
    }, []);

     */

    const defaultFeatureStyle = (feature?: geojson.Feature) => {
        const draught = feature?.properties?.draught;
        const bgColor = draught ? '#000' : '#e440ea';
        const opacity = draught ? 1 - (1 / draught) : 1;

        return ({
            fillColor: bgColor,
            weight: 1,
            opacity: opacity,
            color: '#526579',
            dashArray: '2',
            fillOpacity: opacity,
            stroke: false
        });
    }

    const selectRegion = (e: LeafletMouseEvent) => {
        setSelectedProperty(e.target.feature);

        e.target.setStyle({
            weight: 1,
            fillColor: '#ff0000',
            color: "#ff0000",
            fillOpacity: 1,
            opacity: 1,
        });
    }

    const resetSelectedRegion = (e: LeafletMouseEvent) => {
        setSelectedProperty(undefined);
        e.target.setStyle(defaultFeatureStyle());
    }

    const onEachAction = (feature: any, layer: Layer) => {
        layer.on({
            mousedown: selectRegion,
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

            <GeoJSON key={uuidv4()} data={mapData}
                     onEachFeature={onEachAction}
                     style={(ft) => defaultFeatureStyle(ft)}
            />

        </MapContainer>
    );
};

export default MapGeojson;
