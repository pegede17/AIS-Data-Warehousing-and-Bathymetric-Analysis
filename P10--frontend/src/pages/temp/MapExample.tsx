import React from 'react';
import { MapContainer, Marker, Popup, TileLayer } from 'react-leaflet';

const MapExample: React.FC = () => {
  return (
    <MapContainer
      center={[57.01228, 9.9917]}
      zoom={13}
      style={{ width: '100vw', height: '100vh' }}
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
    </MapContainer>
  );
};

export default MapExample;
