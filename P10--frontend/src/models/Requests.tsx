

export interface RasterRequestParameters extends QueryFilters {
    // Viewport
    northEastLat: string | number,
    northEastLong: string | number,
    southWestLat: string | number,
    southWestLong: string | number,
    zoomLevel: number,
}

export interface QueryFilters {
    fromDate: string;
    toDate: string;

    shipTypes: string[];
    mobileTypes: string[];
    onlyTrustedDraught: boolean;
}

export interface HistogramParameters extends QueryFilters {
    zoomLevel: number;
    cellId: number;
}