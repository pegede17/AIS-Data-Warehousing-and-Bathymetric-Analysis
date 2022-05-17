import { useMapDetails } from "./useMapDetails";
import {MapDetailsContext} from "../contexts/mapDetailsContext";
import React from "react";
import { HistogramParameters } from "../models/Requests";
import API from "../utils/API";
import { AxiosError } from "axios";
import { useSnackbar } from "notistack";

export interface Histogram {
    histogramData: number[];

    // Dispatch methods
    setHistogramData: (state: number[]) => void;
    updateHistogramData: (cellId: number) => void;
}

export const useHistogram = () => {
    const {filters, zoomLevel} = React.useContext(MapDetailsContext);
    const [histogramData, setHistogramData] = React.useState<number[]>([]);
    const {enqueueSnackbar} = useSnackbar();

    const updateHistogramData = (cellId : number) => {
        const params: HistogramParameters = {
            ...filters,
            "cellId": cellId,
            "zoomLevel": zoomLevel!
        }
        console.log(params);
        API.map.getHistogram(params)
            .then(response => {
                setHistogramData(response.data);
            })
            .catch((error: AxiosError) => {
                enqueueSnackbar(`Error occurred: ${error.message} (${error.code})`, {variant: 'error'});
            });
    }

    return {
        updateHistogramData,
        histogramData,
        setHistogramData
    }
}