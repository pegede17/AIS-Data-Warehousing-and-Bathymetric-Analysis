import React from 'react';
import {GeoJSON, MapContainer, Marker, Popup, TileLayer, ZoomControl} from 'react-leaflet';
import {Layer, LeafletMouseEvent, Map} from "leaflet";
import data from '../../../data/more_real_data.json';
import {FeatureCollection} from "geojson";
import MapEventHandler from "../../../components/MapEventHandler";

const MapGeojson: React.FC = () => {
    // const [selectedProperties, setSelectedProperties] = React.useState(undefined);

    const defaultFeatureStyle = (e: LeafletMouseEvent) => {

        return ({
            fillColor: '#000',
            weight: 1,
            opacity: e?.target?.feature ? e.target.feature.properties?.max ? 1 / e.target.feature.properties?.max : 1 : 1,
            color: '#526579',
            dashArray: '2',
            fillOpacity: 0.5
        });
    }

    const selectRegion = (e: LeafletMouseEvent) => {
        // setSelectedProperties(e.target.feature.properties);
        console.log("test");
        console.log(e.target.feature.properties?.max);

        e.target.setStyle({
            weight: 1,
            color: "#526579",
            fillOpacity: 1
        });
    }

    const resetSelectedRegion = (e: LeafletMouseEvent) => {
        // setSelectedProperties(undefined);
        e.target.setStyle(defaultFeatureStyle(e));
    }

    const onEachAction = (feature: any, layer: Layer) => {
        layer.on({
            mouseover: selectRegion,
            mouseout: resetSelectedRegion
        })
    }

    const MAX_ZOOM = 16;
    const TILE_SIZE = 512;

    // https://github.com/microsoft/TypeScript/issues/26552
    const mapData = data as FeatureCollection;

// const extent = Math.sqrt(2) * 6371007.2;
    /*
    const resolutions = Array(MAX_ZOOM + 1)
        .fill()
        .map((_, i) => 1000000 / TILE_SIZE / Math.pow(2, i - 1));

    const ARCTIC_LAEA = new Proj.CRS(
        "EPSG:3034",
        "+proj=lcc +lat_1=35 +lat_2=65 +lat_0=52 +lon_0=10 +x_0=4000000 +y_0=2800000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs",
        {
            origin: [3505631.57, 4115863.55],
            bounds: L.bounds(L.point(1150546.94, 1584884.54), L.point(6678398.53, 4442721.99)),
            resolutions: resolutions
        }
    );


    const TEST_3034 = new Proj.CRS(
        "EPSG:3575",
        "+proj=lcc +lat_1=35 +lat_2=65 +lat_0=52 +lon_0=10 +x_0=4000000 +y_0=2800000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs",
        {
            origin: [4115863.55, 3505631.57],
            bounds: L.bounds([1584884.54, 1150546.94], [4442721.99, 6678398.53]),
            resolutions: [
                8192, 4096, 2048, 1024, 512, 256, 128,
                64, 32, 16, 8, 4, 2, 1, 0.5
            ],
        }
    );
     */

    // crs={ARCTIC_LAEA}
    return (
        <MapContainer
            center={[57.007539, 9.973894]}
            zoom={13}
            preferCanvas={true}
            svg={false}
            zoomControl={false}
        >
            <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                url="https://cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png"
            />
            <ZoomControl position={"bottomright"}/>
            <MapEventHandler />
            <Marker position={[57.01228, 9.9917]}>
                <Popup>
                    Aalborg Universitet, Cassiopeia - House of Computer Science <br/>
                    Telefon: 99 40 99 40
                </Popup>
            </Marker>

            <GeoJSON data={mapData}
                     onEachFeature={onEachAction}
                     style={defaultFeatureStyle}
            />
        </MapContainer>
    );
};

export default MapGeojson;
