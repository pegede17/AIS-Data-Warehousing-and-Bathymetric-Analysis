import React, {useEffect} from 'react';
import Button from '@mui/material/Button';
import {SidebarContext} from "../contexts/sidebarContext";
import '../styles/hideOrShowSidebar.scss';
import '../styles/datePicker.scss';
import ListButton from './ListButton';
import * as muiSidebarStyling from '../styles/muiSidebarStyling';
import {LocalizationProvider} from '@mui/x-date-pickers/LocalizationProvider';
import {DesktopDatePicker} from '@mui/x-date-pickers/DesktopDatePicker';
import TextField from '@mui/material/TextField';
import DoubleArrowIcon from '@mui/icons-material/DoubleArrow';
import {Box, Container, Drawer, Grid, IconButton, InputLabel, Typography} from '@mui/material';
import daLocale from 'date-fns/locale/da';
import {AdapterDateFns} from '@mui/x-date-pickers/AdapterDateFns';
import {useSnackbar} from "notistack";
import {MapDetailsContext} from "../contexts/mapDetailsContext";
import {ConvertJSDateToSmartKey} from "../utils/Conversions";
import {aisTypes, shipList, trustedDraughts} from "../models/FiltersDefaults";

const DRAWER_WIDTH = 325;

const Sidebar: React.FC = () => {
    const {filters, setFilters, filtersChanged, setFiltersChanged} = React.useContext(MapDetailsContext);

    const {isShown, handleSidebar} = React.useContext(SidebarContext);
    const {enqueueSnackbar} = useSnackbar();
    const [fromDate, setFromDate] = React.useState<Date>(
        new Date('2021-05-01T12:00:00'),
    );

    const [toDate, setToDate] = React.useState<Date>(
        new Date('2021-05-07T12:00:00'),
    );
    const [shipTypes, setShipTypes] = React.useState<string[]>(shipList);
    const [mobileTypes, setMobileTypes] = React.useState<string[]>(aisTypes);
    const [onlyTrusted, setOnlyTrusted] = React.useState<string[]>(trustedDraughts);
    const shipListName = "Ship Types";

    const aisListName = "AIS Transponder Type";
    const trustedListName = "Trustworthiness";

    useEffect(() => {
        console.log(filters);
    }, [filters])

    const onApply = () => {
        setFiltersChanged(true);
        console.log(filtersChanged);
        setFilters({
            ...filters,
            shipTypes,
            mobileTypes,
            fromDate: ConvertJSDateToSmartKey(fromDate),
            toDate: ConvertJSDateToSmartKey(toDate),
            onlyTrustedDraught: onlyTrusted.length > 0
        });

        // TODO: Temp
        enqueueSnackbar('This is a toast example!', {variant: 'success'});
    }

    const handleFromDate = (newValue: Date | null) => {
        if (newValue) {
            setFromDate(newValue);
        }
    };
    const handleToDate = (newValue: Date | null) => {
        if (newValue) {
            setToDate(newValue);
        }
    };

    return (
        <Drawer
            sx={{
                width: DRAWER_WIDTH,
                flexShrink: 0,
                '& .MuiDrawer-paper': {
                    width: DRAWER_WIDTH,
                    boxSizing: 'border-box',

                    backdropFilter: 'blur(6px)',
                    backgroundColor: 'rgba(255, 255, 255, 0.8)',
                    boxShadow: '5px 0px 50px rgba(0, 0, 0, 0.15)',
                    borderRadius: '0px 15px 15px 0px',
                },
            }}
            variant="persistent"
            anchor="left"
            open={isShown}
        >

            <Container>
                <Grid container spacing={2}>
                    <Grid item xs={10}>
                        <Typography variant={'h6'} sx={{py: 4, fontWeight: 'bold', color: '#4f7ffe'}}>Draught
                            Overview</Typography>
                    </Grid>
                    <Grid item xs={2} sx={{mt: 1}}>
                        <IconButton sx={muiSidebarStyling.ExpandButtonStyle} onClick={() => handleSidebar()}>
                            <DoubleArrowIcon/>
                        </IconButton>
                    </Grid>
                </Grid>
            </Container>

            <Container>
                <div className='date-picker'>
                    <LocalizationProvider dateAdapter={AdapterDateFns} locale={daLocale}>
                        <DesktopDatePicker
                            label="From:"
                            mask="__-__-____"
                            inputFormat="dd-MM-yyyy"
                            value={fromDate}
                            onChange={handleFromDate}
                            renderInput={(params) => <TextField {...params} />}
                        />
                        <DesktopDatePicker
                            label="To:"
                            mask="__-__-____"
                            inputFormat="dd-MM-yyyy"
                            value={toDate}
                            onChange={handleToDate}
                            renderInput={(params) => <TextField {...params} />}
                        />
                    </LocalizationProvider>
                </div>

                <Box sx={{py: 4}}>
                    <InputLabel sx={{mb: 1, fontWeight: 'bold'}}>Filters</InputLabel>
                    <ListButton listItems={shipList} checkedList={shipTypes} listName={shipListName}
                                setChecked={setShipTypes}/>
                    <ListButton listItems={aisTypes} checkedList={mobileTypes} listName={aisListName}
                                setChecked={setMobileTypes}/>
                    <ListButton listItems={trustedDraughts} checkedList={onlyTrusted} listName={trustedListName} 
                                setChecked={setOnlyTrusted}/>
                </Box>

                <Box style={{display: "flex", justifyContent: "space-evenly"}} sx={{mb: 3}}>
                    <Button sx={{py: 1, px: 3, ...muiSidebarStyling.buttonRevertStyle}}>Revert</Button>
                    <Button sx={{py: 1, px: 3, ...muiSidebarStyling.buttonApplyStyle}} onClick={onApply}>Apply</Button>
                </Box>
            </Container>
        </Drawer>
    );
};

export default Sidebar;
