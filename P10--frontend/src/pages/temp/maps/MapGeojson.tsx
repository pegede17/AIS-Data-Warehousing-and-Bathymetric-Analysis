import React from 'react';
import {GeoJSON, MapContainer, Marker, Popup, TileLayer} from 'react-leaflet';
import * as L from "leaflet";
import {Layer, LeafletMouseEvent} from "leaflet";
import data from '../../../data/15000boxes_geojson.json';
// @ts-ignore
import Proj from "proj4leaflet";
import proj4 from 'proj4';

const MapGeojson: React.FC = () => {
    const [selectedProperties, setSelectedProperties] = React.useState(undefined);

    const defaultFeatureStyle = () => {
        return ({
            fillColor: '#fb6a4a',
            weight: 1,
            opacity: 1,
            color: 'white',
            dashArray: '2',
            fillOpacity: 0.5
        });
    }

    const selectRegion = (e: LeafletMouseEvent) => {
        // setSelectedProperties(e.target.feature.properties);

        console.log(e.target.feature.properties);

        e.target.setStyle({
            weight: 1,
            color: "black",
            fillOpacity: 1
        });
    }

    const resetSelectedRegion = (e: LeafletMouseEvent) => {
        // setSelectedProperties(undefined);
        e.target.setStyle(defaultFeatureStyle());
    }

    const onEachAction = (feature: any, layer: Layer) => {
        layer.on({
            mouseover: selectRegion,
            mouseout: resetSelectedRegion
        })
    }

    const MAX_ZOOM = 16;
    const TILE_SIZE = 512;

// const extent = Math.sqrt(2) * 6371007.2;
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

    return (
        <MapContainer
            center={[57.007539, 9.973894]}
            zoom={13}
            preferCanvas={true}
            svg={false}
            crs={ARCTIC_LAEA}
        >
            <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                url="https://a.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
            <Marker position={[57.01228, 9.9917]}>
                <Popup>
                    Aalborg Universitet, Cassiopeia - House of Computer Science <br/>
                    Telefon: 99 40 99 40
                </Popup>
            </Marker>

            <GeoJSON data={data}
                     onEachFeature={onEachAction}
                     style={defaultFeatureStyle}
            />
        </MapContainer>
    );
};

export default MapGeojson;
