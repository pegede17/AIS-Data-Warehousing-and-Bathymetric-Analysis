import React from "react";

import Chart, {
    ArgumentAxis,
    Label,
    Legend,
    Series,
  } from 'devextreme-react/chart';

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


const HistogramChart = () => {
    return (
      <div>Test</div>
        // <Chart
        //     title={"Histogram for Draughts"}
        //     dataSource={exampleData}
        //     id='chart'
        // >
        //     <ArgumentAxis tickInterval={1.0} >
        //         <Label format="decimal" />
        //     </ArgumentAxis>

        //     <Series
        //         type="bar"
        //     />

        //     <Legend
        //         visible={false}
        //     />
        // </Chart>
    )
}

export default HistogramChart;