# AIS-Data-Warehousing-and-Bathymetric-Analysis
Requirements: 
- PostgreSQL 14
- Python 3.10
- PostGIS 3.2
- ReactJS 17

## starting the application 
### To run ETL process and initialize data warehouse  
Open CMD in folder `P10--ETL`
run python scripts/main.py 
Commandline arguments 
- `-i` for initialize 
- `-sd` `DD/MM/YYYY` start date 
- `-ed` `DD/MM/YYYY` end date
- `-l` Perform loading of the dates
- `-cr` Perform cleaning and trajectory reconstuction of the dates 



### Example running with data
remember to edit application propeties to your local folder 

#### Download example data  
https://web.ais.dk/aisdata/aisdk-2022-06-07.zip

#### Initalize database
from folder `P10--ETL`run the following in cmd 
``` python scripts/main.py -i ```

#### Perform ETL process 
```python scripts/main.py -sd 07/06/2022 -ed 07/06/2022 -l -cr```

### Running the api
navigate the P10--api folder and run the following command 
```flask run ``` 

### running the frontend 
navigate to P10--Frontend and run the following commands 
first intall all dependencies 
```npm install```
run the application 
```npm start```
