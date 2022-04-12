import React from 'react';
import Button from '@mui/material/Button';
import styled from "styled-components";
import {SidebarContext} from "../contexts/sidebarContext";
import '../styles/hideOrShowSidebar.scss';
import '../styles/datePicker.scss';
import ListButton from './ListButton';
import * as muiSidebarStyling from '../styles/muiSidebarStyling';
import {AdapterMoment} from '@mui/x-date-pickers/AdapterMoment';
import {LocalizationProvider} from '@mui/x-date-pickers/LocalizationProvider';
import {DesktopDatePicker} from '@mui/x-date-pickers/DesktopDatePicker';
import TextField from '@mui/material/TextField';
import DoubleArrowIcon from '@mui/icons-material/DoubleArrow';
import {Container, Grid, IconButton, Typography} from '@mui/material';

const Sidebar: React.FC = () => {
    const {isShown, handleSidebar} = React.useContext(SidebarContext);
    const [fromDate, setFromDate] = React.useState<Date | null>(
        new Date('2021-01-01T12:00:00'),
    );
    const [toDate, setToDate] = React.useState<Date | null>(
        new Date('2021-01-07T12:00:00'),
    );

    const shipListName = "Ship Types";
    const shipList = ["Sailing", "Pleasure", "Cargo", "Passenger", "Military"];

    const aisListName = "AIS Transponder Type";
    const aisTypes = ["Type A", "Type B"];

    const gridListName = "Grid Size";
    const gridList = ["50 Meters", "100 Meters", "500 Meters", "1000 Meters"];

    const handleFromDate = (newValue: Date | null) => {
        setFromDate(newValue);
    };
    const handleToDate = (newValue: Date | null) => {
        setToDate(newValue);
    };

    // Old classname 'bg-black text-white sidebar
    return (
        <SidebarContainer className={'bg-white text-black sidebar'}>

            <Container>
                <Grid container spacing={2}>
                    <Grid item xs={10}>
                        <Typography variant={'h6'} sx={{py: 4, fontWeight: 'bold', color: '#4f7ffe'}}>Draught
                            Overview</Typography>
                    </Grid>
                    <Grid item xs={2} sx={{mt: 1}}>
                        <IconButton sx={muiSidebarStyling.ExpandButtonStyle}>
                            <DoubleArrowIcon onClick={() => handleSidebar()}/>
                        </IconButton>
                    </Grid>
                </Grid>
            </Container>

            <Container>
                <div className='date-picker'>
                    <LocalizationProvider dateAdapter={AdapterMoment}>
                        <DesktopDatePicker
                            label="From:"
                            inputFormat="DD/MM/yyyy"
                            value={fromDate}
                            onChange={handleFromDate}
                            renderInput={(params) => <TextField {...params} />}
                        />
                        <DesktopDatePicker
                            label="To:"
                            inputFormat="DD/MM/yyyy"
                            value={toDate}
                            onChange={handleToDate}
                            renderInput={(params) => <TextField {...params} />}
                        />
                    </LocalizationProvider>
                </div>

                <ListButton listItems={shipList} listName={shipListName}/>
                <ListButton listItems={aisTypes} listName={aisListName}/>
                <ListButton listItems={gridList} listName={gridListName}/>

                <div style={{display: "flex", justifyContent: "space-evenly"}}>
                    <Button sx={{py: 1.5, px: 3, ...muiSidebarStyling.buttonRevertStyle}}>Revert</Button>
                    <Button sx={{py: 1.5, px: 3, ...muiSidebarStyling.buttonApplyStyle}}>Apply</Button>
                </div>
            </Container>
        </SidebarContainer>
    );
};

const SidebarContainer = styled.div`
  height: 100%;
  width: 100%;
`

export default Sidebar;
