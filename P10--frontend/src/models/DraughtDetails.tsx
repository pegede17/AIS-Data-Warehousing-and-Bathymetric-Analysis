

export interface DraughtDetails {
    maximumDraught: number;
    averageDraught: number;
    minimumDraught: number;
    shipsRecorded: number; //trajectory count
    histogram?: number[];
}
