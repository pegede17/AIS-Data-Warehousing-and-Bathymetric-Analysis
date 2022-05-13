import axios from "axios";
import {FeatureCollection} from "geojson";
import React from "react";
import {LatLngBounds} from "leaflet";
import {RasterRequestParameters} from "../models/Requests";

export default {
    map: {
        getBoxesTesting: async (params: RasterRequestParameters) => {
            return await axios.get<FeatureCollection>('http://localhost:5000/boxes', {params});
        }
    }
}
