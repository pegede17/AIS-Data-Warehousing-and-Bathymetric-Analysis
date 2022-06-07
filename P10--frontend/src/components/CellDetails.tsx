import React from "react";
import {DraughtDetails} from "../models/DraughtDetails";
import {Box, Button, Modal} from '@mui/material';
import {HistogramContext} from "../contexts/histogramContext";
import HistogramChart from "./HistogramChart";

// Recorded Draughts style
// font-family: 'Inter';
// font-style: normal;
// font-weight: 700;
// font-size: 18px;
// line-height: 22px;
// letter-spacing: 0.05em;
// color: #526579;


const CellDetails: React.FC<DraughtDetails> = ({
                                                   maximumDraught,
                                                   minimumDraught,
                                                   averageDraught,
                                                   shipsRecorded,
                                                   cellId
                                               }) => {
    const {histogramData, updateHistogramData, loading} = React.useContext(HistogramContext);
    const [open, setOpen] = React.useState(false);
    const handleOpen = () => setOpen(true);
    const handleClose = () => setOpen(false);

    const style = {
        position: "absolute",
        top: "50%",
        left: "50%",
        transform: "translate(-50%, -50%)",
        width: "70%",
        bgcolor: "background.paper",
        border: "2px solid #000",
        boxShadow: 24,
        p: 4
    };

    const renderRow = (name: string, value: string | number) => {
        return (
            <div className="row">
                <div className="col-sm-auto">{name}</div>
                <div className="col text-end">{value}</div>
            </div>
        )
    }

    return (
        <div>
            <p className={'h6'}><strong>Recorded Draughts</strong></p>

            <div className={'d-grid gap-2'}>
                {renderRow("Maximum", maximumDraught + " m")}
                {renderRow("Average", averageDraught + " m")}
                {renderRow("Minimum", minimumDraught + " m")}
                {renderRow("Trajectory Count", shipsRecorded)}
            </div>

            <div className={'d-flex justify-content-center py-2'}>
                <Button onClick={() => {
                    updateHistogramData(cellId);
                    handleOpen()
                }}>
                    Fetch Histogram
                </Button>
            </div>

            <Modal
                open={open}
                onClose={handleClose}
                aria-labelledby="modal-modal-title"
                aria-describedby="modal-modal-description"
            >
                <Box sx={style}>
                    <HistogramChart histogramData={histogramData} isLoading={loading}/>
                </Box>
            </Modal>
        </div>
    )
};

export default CellDetails;
