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
    const [checked, setChecked] = React.useState([0]);
    const [open, setOpen] = React.useState(true);

    // Handles the checkmark in the list
    const handleToggle = (value: number) => () => {
        const currentIndex = checked.indexOf(value);
        const newChecked = [...checked];

        if (currentIndex === -1) {
            newChecked.push(value);
        } else {
            newChecked.splice(currentIndex, 1);
        }

        setChecked(newChecked);
    };

    // Handles the open and close click for the list
    const handleExpandClick = () => {
        setOpen(!open);
    };

    return (
        <div>
            <List>
                <ListItemButton className="list-button" sx={{background: "#FF8C21"}} onClick={handleExpandClick}>
                    <ListItemText primary={listName} />
                    {open ? <ExpandLess /> : <ExpandMore />}
                </ListItemButton>
                <Collapse in={open} timeout="auto" unmountOnExit>
                    <List component="div" disablePadding>
                        {[0, 1, 2, 3, 4].map((value) => {
                            const labelId = `checkbox-list-label-${value}`;

                            return (
                                <ListItem key={value}>
                                    <ListItemButton
                                        role={undefined}
                                        onClick={handleToggle(value)}
                                        dense
                                    >
                                        <ListItemIcon>
                                            <Checkbox
                                                edge="start"
                                                checked={checked.indexOf(value) !== -1}
                                                tabIndex={-1}
                                                disableRipple
                                                inputProps={{ "aria-labelledby": labelId }}
                                            />
                                        </ListItemIcon>

                                        <ListItemText id={labelId} primary={listItems.at(value)} />
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