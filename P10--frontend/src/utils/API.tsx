import axios from "axios";
import {FeatureCollection} from "geojson";
import React from "react";
import {LatLngBounds} from "leaflet";
import {HistogramParameters, RasterRequestParameters} from "../models/Requests";
import qs from "qs";

export default {
    map: {
        getBoxes: async (params: RasterRequestParameters) => {
            return await axios.get<FeatureCollection>('http://127.0.0.1:5000/boxes', {params,
            paramsSerializer: params => {
              return qs.stringify(params, {arrayFormat: 'comma'})
            }});
        },
        getHistogram: async (params: HistogramParameters) => {
            return await axios.get<number[]>('http://127.0.0.1:5000/histogram', {params,
            paramsSerializer: params => {
              return qs.stringify(params, {arrayFormat: 'comma'})
            }});
        }
    }
}
