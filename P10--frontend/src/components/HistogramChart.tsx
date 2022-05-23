import React, {useEffect} from "react";

import Chart, {Legend, ScrollBar, Series, ZoomAndPan} from 'devextreme-react/chart';
import LoadingIndicator from "./LoadingIndicator";

interface Props {
    histogramData: number[];
    isLoading: boolean;
}


interface ChartDataElement {
    arg: number,
    val: number
}

export const exampleData = [{
    arg: 0.5,
    val: 30,
}, {
    arg: 1.0,
    val: 36,
}, {
    arg: 1.5,
    val: 44,
}, {
    arg: 2.0,
    val: 22,
}, {
    arg: 2.5,
    val: 61,
}, {
    arg: 3.0,
    val: 9,
}];


const HistogramChart: React.FC<Props> = ({histogramData, isLoading}) => {
    const handleConversion = (histogramData: number[]) => {
        const dataSet: ChartDataElement[] = [];
        if (histogramData) {
            for (let index = 0; index < histogramData.length; index++) {
                if (histogramData[index] > 0) {
                    const element: ChartDataElement = {arg: (index-2) / 10, val: histogramData[index]};
                    dataSet.push(element);
                }
            }
        }

        return dataSet;
    };

    useEffect(() => {
        console.log(isLoading);
    }, [isLoading])

    if (isLoading) {
        return <LoadingIndicator status={true} message={"Fetching histogram data"}/>
    }

    return (
        <div>
            <Chart
                title={"Histogram for Draughts"}
                dataSource={handleConversion(histogramData)}
                id='chart'
                onPointClick={(e) => {
                    console.log(e)
                    e.target.showTooltip()
                }}

            >
                <ScrollBar visible={true}/>
                <ZoomAndPan argumentAxis="both" valueAxis="both"/>
                {/* <CommonSeriesSettings
                argumentField="state"
                // type="bar"
                barPadding={0.5}
              /> */}

                {/*
                <ArgumentAxis tickInterval={1.0}>
                    <Label format="decimal"/>
                </ArgumentAxis>
*/}
                <Series
                    type="bar"
                    barWidth={7}
                    // barPadding={5.5}
                    hoverMode="onlyPoint"
                />

                <Legend
                    visible={false}
                />
            </Chart>
        </div>
    )
}

export default HistogramChart;
