import React from 'react';
import {MapContainer, Marker, Popup, TileLayer, Rectangle, FeatureGroup} from 'react-leaflet';
import data from '../../../data/constraints.json';
import {LatLng, LatLngBounds} from "leaflet";

const MapExample: React.FC = () => {
  return (
    <MapContainer
      center={[57.01228, 9.9917]}
      zoom={13}
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      <Marker position={[57.01228, 9.9917]}>
        <Popup>
          Aalborg Universitet, Cassiopeia - House of Computer Science <br />
          Telefon: 99 40 99 40
        </Popup>
      </Marker>
      <FeatureGroup pathOptions={ { color: 'purple' } }>
         {data.map((point) => {
          console.log(point.polygon.coordinates.toString())
          return <Rectangle key={point.polygon.coordinates.toString()} bounds={new LatLngBounds(
            new LatLng(point.polygon.coordinates[0][0][1] ?? 0, point.polygon.coordinates[0][0][0] ?? 0),
            new LatLng(point.polygon.coordinates[0][2][1] ?? 0, point.polygon.coordinates[0][2][0] ?? 0))}><Popup>Max: {point.max} Count: {point.count}</Popup></Rectangle>
        })}
       </FeatureGroup>
    </MapContainer>
  );
};

export default MapExample;
