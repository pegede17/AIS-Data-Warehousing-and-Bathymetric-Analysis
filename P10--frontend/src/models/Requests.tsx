export interface RasterRequestParameters {
    // Viewport
    northEastLat: string | number,
    northEastLong: string | number,
    southWestLat: string | number,
    southWestLong: string | number,

    // Filters
    filters?: ShipFilters;
}

export interface ShipFilters {
    temp: string;
}
