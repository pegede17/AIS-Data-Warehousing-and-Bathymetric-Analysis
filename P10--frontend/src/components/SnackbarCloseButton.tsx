import {SnackbarKey, useSnackbar} from 'notistack';
import * as React from 'react';
import {IconButton} from "@mui/material";
import Close from '@mui/icons-material/Close';

const SnackbarCloseButton: React.FC<{ key: SnackbarKey }> = ({key}) => {
    const {closeSnackbar} = useSnackbar();

    return (
        <IconButton onClick={() => closeSnackbar(key)}>
            <Close htmlColor={'#fff'}/>
        </IconButton>
    );
}

export default SnackbarCloseButton;
