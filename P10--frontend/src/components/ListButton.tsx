import React from "react";
import { Button } from "react-bootstrap";
import '../styles/listButton.scss';

import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import Checkbox from "@mui/material/Checkbox";
import Collapse from '@mui/material/Collapse';
import ExpandLess from '@mui/icons-material/ExpandLess';
import ExpandMore from '@mui/icons-material/ExpandMore';
interface Props {
    listItems: string[];
    listName: string;
}

const ListButton: React.FC<Props> = ({ listItems , listName }) => {
    const [checked, setChecked] = React.useState([""]);
    const [open, setOpen] = React.useState(false);

    // Handles the checkmark in the list
    const handleToggle = (value: string ) => () => {
        const currentIndex = checked.indexOf(value);
        const newChecked = [...checked];
        if (currentIndex === -1) {
            newChecked.push(value);
        } else {
            newChecked.splice(currentIndex, 1);
        }
        setChecked(newChecked);
        console.log(checked)
    };

    // Handles the open and close click for the list
    const handleExpandClick = () => {
        setOpen(!open);
    };
    
    const outerListStyle = {
        display: 'flex',
        backgroundColor: '#FF8C21',
        margin: 0,
        gap: 1,
        padding: 1,
        color: "#000000",
        fontWeight: 3,
        border: '1px solid black',
        
    }

    const innerListStyle = {
        display: 'flex-row',
        background: '#ffffff',
        color: "#000000",
        border: '1px solid white',
        py: 0,
    }

    const itemStyle = {
        background: "#ffffff",
        margin: 0,
        padding: 0
    }

    const checkboxStyle = {
        color: "#FF8C21",
        fill: "#FF8C21",
        paddingTop: 1,
        // background: "#FF8C21",
    }

    return (
        <div>
            <List>
                <ListItemButton sx={outerListStyle} onClick={handleExpandClick}>
                    <ListItemText primary={listName} />
                    {open ? <ExpandLess /> : <ExpandMore />}
                </ListItemButton>
                <Collapse in={open} timeout="auto" unmountOnExit>
                    <List component="div" disablePadding sx={innerListStyle}>
                        {listItems.map((value) => {
                            const labelId = `checkbox-list-label-${value}`;

                            return (
                                <ListItem key={value}>
                                    <ListItemButton
                                        role={undefined}
                                        onClick={handleToggle(value)}
                                        dense
                                        sx={itemStyle}
                                    >
                                        <ListItemIcon>
                                            <Checkbox
                                                edge="start"
                                                checked={checked.indexOf(value) !== -1}
                                                tabIndex={-1}
                                                disableRipple
                                                inputProps={{ "aria-labelledby": labelId }}
                                                sx={checkboxStyle}
                                            />
                                        </ListItemIcon>

                                        <ListItemText id={labelId} primary={value} />
                                    </ListItemButton>
                                </ListItem>
                            );
                        })}
                    </List>
                </Collapse>
            </List>
        </div>
    );
}

export default ListButton;