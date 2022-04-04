import React from "react";
import { Button } from "react-bootstrap";
import '../styles/listButton.scss';

const ListButton: React.FC = () => {
    const tempList = ["Sailing", "Pleasure", "Cargo", "Passenger", "Military"]
    // const {buttonName, setButtonName} = React.useContext(ListButtonContext);

    return (
        <div>
            <Button className="list-button">Ship Types 🢓</Button>
        </div>
    );
};

export default ListButton;