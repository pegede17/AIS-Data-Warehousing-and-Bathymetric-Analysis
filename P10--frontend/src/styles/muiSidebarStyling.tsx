import { checkboxClasses } from "@mui/material/Checkbox";
const defaultBlue = '#4f7ffe';
const backgroundWhite = '#fefefe';
const innerListBackground = '#fafbff'
const hoverBlue = '#0075e3';
const textGray = '#526579';
const checkmarkGray = '#D3D8E3';
const applyGreen = '#3ad3a7';
const applyHover = '#1ed38c';
const revertHover = '#52616c';
const ExpandCompressIcon = '#d3d8e3';
const textWhite = '#FFFFFF';



export const outerListStyle = {
    display: 'flex',
    backgroundColor: defaultBlue,
    padding: 1,
    border: '1px solid black',
    '&:hover': { 
        bgcolor: hoverBlue 
    },
    color: textWhite,
}

export const innerListStyle = {
    display: 'flex-row',
    background: innerListBackground,
    color: textGray,
    border: '1px solid white',
    py: 0,
}

export const itemStyle = {
    background: backgroundWhite,
    margin: 0,
    padding: 0,
    '&:hover': { 
        bgcolor: 'transparent' // no on-hover color
    },
}

export const textStyle = {
    'span': {
        fontWeight: 'bold'
    }
}

export const checkboxStyle = {
    paddingTop: 1,
    // Changes checkbox color
    [`&, &.${checkboxClasses.checked}`]: {
        color: checkmarkGray,
    },
    // Checkmark icon size
    '& .MuiSvgIcon-root': { fontSize: 30,  }
}

export const buttonRevertStyle = {
    color: textWhite,
    backgroundColor: textGray,
    '&:hover': { 
        bgcolor: revertHover 
    },
}

export const buttonApplyStyle = {
    color: textWhite,
    backgroundColor: defaultBlue,
    '&:hover': { 
        bgcolor: hoverBlue 
    },
}

export const ExpandButtonStyle = {
    color: ExpandCompressIcon, 
    transform: 'rotate(180deg)'
}

export const titleText = {
    color: defaultBlue, 
    textAlign: 'center'
}