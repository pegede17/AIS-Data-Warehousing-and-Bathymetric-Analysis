import axios from "axios";
import {FeatureCollection} from "geojson";
import React from "react";
import {LatLngBounds} from "leaflet";
import {RasterRequestParameters} from "../models/Requests";
import qs from "qs";

export default {
    map: {
        getBoxesTesting: async (params: RasterRequestParameters) => {
            return await axios.get<FeatureCollection>('http://127.0.0.1:5000/boxes', {params,
            paramsSerializer: params => {
              return qs.stringify(params, {arrayFormat: 'comma'})
            }});
        }
    }
}
