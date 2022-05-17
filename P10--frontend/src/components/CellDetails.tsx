import React from "react";
import { DraughtDetails } from "../models/DraughtDetails";
import {Button, Container, Grid, Modal, Box} from '@mui/material';
import { HistogramContext } from "../contexts/histogramContext";
import HistogramChart from "./HistogramChart";

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
    const [open, setOpen] = React.useState(false);
    const handleOpen = () => setOpen(true);
    const handleClose = () => setOpen(false);
    
    const style = {
        position: "absolute",
        top: "50%",
        left: "50%",
        transform: "translate(-50%, -50%)",
        width: 400,
        bgcolor: "background.paper",
        border: "2px solid #000",
        boxShadow: 24,
        p: 4
      };

    return (
        <div>
            <h5>Recorded Draughts</h5>
            <Container>
                <Grid container spacing={2}>
                    <Grid item xs={10}>
                        <p>Maximum</p>
                        <p>Average</p>
                        <p>Minimum</p>
                        <p>Ship Count</p>
                    </Grid>
                    <Grid item xs={2} >
                        <p>{maximumDraught}m</p>
                        <p>{averageDraught}m</p>
                        <p>{minimumDraught}m</p>
                        <p>{shipsRecorded}</p>
                    </Grid>
                </Grid>
                <Button onClick={() => {
                    updateHistogramData(cellId);
                    handleOpen()
                    console.log(histogramData)
                }}>Get Histogram
                </Button>
                <Modal
                    open={open}
                    onClose={handleClose}
                    aria-labelledby="modal-modal-title"
                    aria-describedby="modal-modal-description"
                    >
                    <Box sx={style}>
                        <HistogramChart/>
                    </Box>
                </Modal>
            </Container>
        </div>
    )
};

export default CellDetails;
