import React from 'react';
import {MapDetailsContext} from "../contexts/mapDetailsContext";
import {Stack, Typography} from "@mui/material";

const SidebarColorPicker = () => {
    const {
        gradientColors,
        setGradientColors
    } = React.useContext(MapDetailsContext);

    return (
        <>
            <Stack direction="row" spacing={2} sx={{justifyContent: 'center', alignItems: 'center'}}>
                <Typography variant="overline" display="block" sx={{margin: 0}} gutterBottom>
                    From
                </Typography>

                <input type="color" defaultValue={gradientColors.colorOne}
                       onBlur={e => setGradientColors({...gradientColors, colorOne: e.target.value})}/>

                <Typography variant="overline" display="block" sx={{margin: 0}} gutterBottom>
                    To
                </Typography>

                <input type="color" defaultValue={gradientColors.colorTwo}
                       onBlur={e => setGradientColors({...gradientColors, colorTwo: e.target.value})}/>

            </Stack>

        </>
    );
};

export default SidebarColorPicker;
