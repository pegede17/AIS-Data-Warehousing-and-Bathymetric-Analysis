import React from "react";

import Chart, {
    ArgumentAxis,
    Label,
    Legend,
    Series,
    CommonSeriesSettings
  } from 'devextreme-react/chart';
import { Histogram } from "../hooks/useHistogram";
interface Props {
  histogramData: number[];
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



const HistogramChart: React.FC<Props> = ({histogramData}) => {
    const handleConversion = (histogramData: number[]) => {
      const dataSet: ChartDataElement[]= [];
      if (histogramData) {
        for (let index = 0; index < histogramData.length; index++) {
          const element:ChartDataElement = {arg: index/10, val: histogramData[index]};
          dataSet.push(element);
          
        }
      }
      return dataSet;
    };

    return (
      <div>
          <Chart 
              title={"Histogram for Draughts"}
              dataSource={handleConversion(histogramData)}
              id='chart'
              style={{width: 1000}}
              
          >
              <CommonSeriesSettings
                argumentField="state"
                // type="bar"
                barPadding={0.5}
              />
                
              <ArgumentAxis tickInterval={1.0} >
                  <Label format="decimal" />
              </ArgumentAxis>

              <Series
                  type="bar"
                  // barPadding={5.5}
              />

              <Legend
                  visible={false}
              />
          </Chart>
        </div>
    )
}

export default HistogramChart;