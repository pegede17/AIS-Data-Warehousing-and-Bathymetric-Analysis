import React from 'react';
import {GeoJSON, MapContainer, Marker, Popup, TileLayer, ZoomControl} from 'react-leaflet';
import {Layer, LeafletMouseEvent} from "leaflet";
import data from '../../../data/some_real_data.json';
import * as geojson from "geojson";
import {FeatureCollection} from "geojson";
import MapEventHandler from "../../../components/MapEventHandler";
import {MapDetailsContext} from "../../../contexts/mapDetailsContext";
import { Button, Container } from '@mui/material';



const MapGeojson: React.FC = () => {
    const {setSelectedProperty, selectedProperties} = React.useContext(MapDetailsContext);
    const [jsonData, setJsonData] = React.useState<FeatureCollection>(data as FeatureCollection)
    const [test, setTest] = React.useState<number>(0)
    React.useEffect(() => {
        console.log(selectedProperties);
        console.log("JsonData")
        fetch('http://localhost:5000/boxes')
        .then(response => response.json())
        .then(data => setJsonData(data));
        console.log(jsonData)
        setTest(test + 1)
    }, [selectedProperties])

    const defaultFeatureStyle = (feature?: geojson.Feature) => {
        const draught = feature?.properties?.draught;
        const bgColor = draught ? '#000' : '#e440ea';
        const opacity = draught ? 1 - (1 / draught) : 1;

        /*
        console.log(feature);
        console.log("Opacity: " + opacity);
        console.log("Draught: " + draught);

         */

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
            fillColor: '#e7e7e7',
            color: "#526579",
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

    const MAX_ZOOM = 16;
    const TILE_SIZE = 512;

    // https://github.com/microsoft/TypeScript/issues/26552
    // const mapData = data as FeatureCollection;

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
        // <Container>
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
            <Marker position={[57.01228, 9.9917]}>
                <Popup>
                    Aalborg Universitet, Cassiopeia - House of Computer Science <br/>
                    Telefon: 99 40 99 40
                </Popup>
            </Marker>

            <GeoJSON key={test} data={jsonData}
                     onEachFeature={onEachAction}
                     style={(ft) => defaultFeatureStyle(ft)}
                     />
            
        </MapContainer>
        // {/* <Button onClick={() => {
        //     console.log("I was clicked")
        //     fetch('http://localhost:5000/boxes')
        // .then(response => response.json())
        // .then(data => setJsonData(data));
        // console.log(jsonData)}}>Tester</Button> */}
                    //  </Container>
    );
};

export default MapGeojson;
