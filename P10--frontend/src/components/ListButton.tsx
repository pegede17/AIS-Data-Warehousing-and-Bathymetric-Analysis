import React, {useEffect} from "react";

import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import Checkbox from "@mui/material/Checkbox";
import Collapse from '@mui/material/Collapse';
import ExpandLess from '@mui/icons-material/ExpandLess';
import ExpandMore from '@mui/icons-material/ExpandMore';
import * as ListStyles from '../styles/muiSidebarStyling';
import {MapDetailsContext} from "../contexts/mapDetailsContext";
import {shipList} from "../models/FiltersDefaults";

interface Props {
    listItems: string[];
    listName: string;
    setChecked: (list: string[]) => void;
    checkedList: string[];
}

const ListButton: React.FC<Props> = ({listItems, listName, setChecked, checkedList}) => {
    const {filters, setFilters} = React.useContext(MapDetailsContext);
    const [open, setOpen] = React.useState(false);

    useEffect(() => {
        console.log(checkedList);
    }, [checkedList])

    // Handles the checkmark in the list
    const handleToggle = (value: string) => () => {
        console.log(value);
        const currentIndex = checkedList.indexOf(value);
        const newChecked = [...checkedList];
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
        <List sx={{py: 0.2}}>
            <ListItemButton sx={{px: 2, py: 0.5, borderRadius: 1, ...ListStyles.outerListStyle}}
                            onClick={handleExpandClick}>
                <ListItemText primary={listName}/>
                {open ? <ExpandLess/> : <ExpandMore/>}
            </ListItemButton>
            <Collapse in={open} timeout="auto" unmountOnExit sx={{border: 1, borderColor: '#D3D8E3'}}>
                <List component="div" disablePadding sx={ListStyles.innerListStyle}>
                    {listItems.map((value) => {
                        const labelId = `checkbox-list-label-${value}`;

                        return (
                            <ListItem key={value}>
                                <ListItemButton
                                    role={undefined}
                                    onClick={handleToggle(value)}
                                    dense
                                    sx={ListStyles.itemStyle}
                                >
                                    <ListItemIcon>
                                        <Checkbox
                                            edge="start"
                                            checked={checkedList.indexOf(value) !== -1}
                                            tabIndex={-1}
                                            disableRipple
                                            inputProps={{"aria-labelledby": labelId}}
                                            sx={ListStyles.checkboxStyle}
                                        />
                                    </ListItemIcon>

                                    <ListItemText id={labelId} primary={value} sx={ListStyles.textStyle}/>
                                </ListItemButton>
                            </ListItem>
                        );
                    })}
                </List>
            </Collapse>
        </List>
    );
}

export default ListButton;
