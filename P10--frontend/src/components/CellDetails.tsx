import React from "react";
import { DraughtDetails } from "../models/DraughtDetails";
import {Button, Container, Grid} from '@mui/material';
import { HistogramContext } from "../contexts/histogramContext";

// Recorded Draughts style
// font-family: 'Inter';
// font-style: normal;
// font-weight: 700;
// font-size: 18px;
// line-height: 22px;
// letter-spacing: 0.05em;
// color: #526579;

const CellDetails: React.FC<DraughtDetails> = ({maximumDraught, minimumDraught, averageDraught, shipsRecorded, cellId}) => {
    const {histogramData, updateHistogramData } = React.useContext(HistogramContext);

    return (
        <div>
            <h5>Recorded Draughts</h5>
            <Container>
                <Grid container spacing={2}>
                    <Grid item xs={10}>
                        <p>Maximum</p>
                        <p>Average</p>
                        <p>Minimum</p>
                        <p>Count</p>
                    </Grid>
                    <Grid item xs={2} >
                        <p>{maximumDraught}m</p>
                        <p>{averageDraught}m</p>
                        <p>{minimumDraught}m</p>
                        <p>{shipsRecorded} ships</p>
                    </Grid>
                </Grid>
                <Button onClick={() => {
                    updateHistogramData(cellId);
                    console.log(histogramData)
                }}>Get Histogram</Button>
                {histogramData?.length > 1 ? <div>{histogramData}</div> : null}
            </Container>
        </div>
    )
};

export default CellDetails;
