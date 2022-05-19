import {MapDetailsContext} from "../contexts/mapDetailsContext";
import React from "react";
import {HistogramParameters} from "../models/Requests";
import API from "../utils/API";
import {AxiosError} from "axios";
import {useSnackbar} from "notistack";

export interface Histogram {
    histogramData: number[];
    loading: boolean;

    // Dispatch methods
    setHistogramData: (state: number[]) => void;
    updateHistogramData: (cellId: number) => void;
}

export const useHistogram = () => {
    const {filters, zoomLevel} = React.useContext(MapDetailsContext);
    const [histogramData, setHistogramData] = React.useState<number[]>([]);
    const [histogramCellId, setHistogramCellId] = React.useState<number>(0)
    const [loading, setLoading] = React.useState<boolean>(false);
    const {enqueueSnackbar} = useSnackbar();

    const updateHistogramData = (cellId: number) => {
        if (histogramCellId == cellId) {
            return;
        }
        setLoading(true);
        setHistogramCellId(cellId)
        setHistogramData([])
        const params: HistogramParameters = {
            ...filters,
            "cellId": cellId,
            "zoomLevel": zoomLevel!
        }
        console.log(params);
        API.map.getHistogram(params)
            .then(response => {
                setLoading(false);
                setHistogramData(response.data);
                console.log(response.data);
            })
            .catch((error: AxiosError) => {
                setLoading(false);
                enqueueSnackbar(`Error occurred: ${error.message} (${error.code})`, {variant: 'error'});
            });
    }

    return {
        updateHistogramData,
        histogramData,
        setHistogramData,
        loading
    }
}
