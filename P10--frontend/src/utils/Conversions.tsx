
/*
 def convertTimestampToDateId(timestamp):
        if(timestamp):
            date = datetime.strptime(timestamp, "%d/%m/%Y %H:%M:%S")
            return int(date.strftime("%Y%m%d"))
        else:
            return 0

 */
import moment from "moment";

export const ConvertJSDateToSmartKey = (date: Date): string => {
    return moment(date).format('YYYYMMDD');
}
