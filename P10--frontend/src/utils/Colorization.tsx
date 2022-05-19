import Gradient from "javascript-color-gradient";
import React from "react";
import {MapDetailsContext} from "../contexts/mapDetailsContext";


export const testingGradient = (index: number) => {
    const {
        gradientColors,
        setGradientColors
    } = React.useContext(MapDetailsContext);

    return new Gradient()
        .setColorGradient(gradientColors.colorOne, gradientColors.colorTwo)
        .setMidpoint(50)
        .getColor(index);
}
