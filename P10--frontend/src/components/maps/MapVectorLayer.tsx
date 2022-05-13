import React from 'react';
import {
    Circle,
    CircleMarker,
    MapContainer,
    Polyline,
    Polygon,
    Popup,
    Rectangle,
    TileLayer,
} from 'react-leaflet'

const fillBlueOptions = { fillColor: 'blue' }
const blackOptions = { color: 'black' }
const limeOptions = { color: 'lime' }
const purpleOptions = { color: 'purple' }
const redOptions = { color: 'red' }

// https://react-leaflet.js.org/docs/example-vector-layers/
function MapVectorLayer() {
    return (
        <MapContainer center={[51.505, -0.09]} zoom={13}>
            <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
            <Circle center={[51.505, -0.09]} pathOptions={fillBlueOptions} radius={200} />
            <CircleMarker
                center={[51.51, -0.12]}
                pathOptions={redOptions}
                radius={20}>
                <Popup>Popup in CircleMarker</Popup>
            </CircleMarker>
            <Polyline pathOptions={limeOptions} positions={[
                [51.505, -0.09],
                [51.51, -0.1],
                [51.51, -0.12],
            ]} />
            <Polyline pathOptions={limeOptions} positions={[
                [
                    [51.5, -0.1],
                    [51.5, -0.12],
                    [51.52, -0.12],
                ],
                [
                    [51.5, -0.05],
                    [51.5, -0.06],
                    [51.52, -0.06],
                ],
            ]} />
            <Polygon pathOptions={purpleOptions} positions={[
                [51.515, -0.09],
                [51.52, -0.1],
                [51.52, -0.12],
            ]} />
            <Polygon pathOptions={purpleOptions} positions={[
                [
                    [51.51, -0.12],
                    [51.51, -0.13],
                    [51.53, -0.13],
                ],
                [
                    [51.51, -0.05],
                    [51.51, -0.07],
                    [51.53, -0.07],
                ],
            ]} />
            <Rectangle bounds={[
                [51.49, -0.08],
                [51.5, -0.06],
            ]} pathOptions={blackOptions} />
        </MapContainer>
    )
}

export default MapVectorLayer;