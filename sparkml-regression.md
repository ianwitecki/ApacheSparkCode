# Spark Sql Assignment  

## In Class Questions
1. Below are the description of the required columns.
    1. GENHLTH

       | Summary    | GENHLTH         |
       |------------|-----------------|
       |  count     |           486303|
       |   mean     |5.473830101808955|
       | stddev     |3.447015277360491|
       |    min     |             -1.0|
       |    max     |              9.0|

    1. PHYSHLTH

       | Summary    | PHYSHLTH        |
       |------------|-----------------|
       |  count     |           486303|
       |   mean     | 65.5723818277905|
       | stddev     |31.16934023520132|
       |    min     |              0.0|
       |    max     |             99.0|


    1. MENTHLTH

       | Summary    | MENTHLTH         |
       |------------|------------------|
       |  count     |            486303|
       |   mean     |30.810548156190688|
       | stddev     |32.125000004590426|
       |    min     |               0.0|
       |    max     |              99.0|

    1. POORHLTH

       | Summary    | POORHLTH         |
       |------------|------------------|
       |  count     |            486303|
       |   mean     |29.994067484675192|
       | stddev     | 36.05172810537815|
       |    minv    |               1.0|
       |    maxv    |              99.0|

    1. EXERANY2

       | Summary    | EXARNY2           |
       |------------|-------------------|
       |  count     |             486303|
       |   mean     |0.12180471845742263|
       | stddev     | 0.8013351687687944|
       |    min     |               -1.0|
       |    max     |                9.0|

    1. SLEPTIM1

       | Summary    | SLEPTIM1        |
       |------------|-----------------|
       |  count     |           486303|
       |   mean     |68.85272145144077|
       | stddev     |16.74114625597331|
       |    min     |             -1.0|
       |    max     |             99.0|


## Out of Class Questions

1. In order to predict the values of each of the below categories from other values in the data set, I created a correlation matrix. From this correlation matrix, I pulled out the columns with highest corelation to the field in question. I then ran a linear regression on two of the columns selected columns and got predictions. Below are the required fields, the highest predictions, and the related columns.

1. GENHLTH

|summary |        prediction|
|--------|------------------|
|  count |            486303|
|   mean | 5.473830101808844|
| stddev |0.5981059187372053|
|    min |1.7053179697537528|
|    max |15.337392590640363|

The questions with with the most significance in predicting the value of GENHLTH are CHCCOPD1 and HAVARTH3.

1. PHYSHLTH

|summary|        prediction|
|-------|------------------|
|  count|            486303|
|   mean| 65.57238182779444|
| stddev|  6.34983972854637|
|    min|28.588118112274103|
|    max| 128.7774640799186|


The questions with with the most significance in predicting the value of PHYSHLTH are SLEPTIM1 and CHCCOPD1.

1. MENTHLTH

|summary|        prediction|
|-------|------------------|
|  count|            486303|
|   mean| 30.81054815619021|
| stddev|15.086402888506774|
|    min| 22.91018858115651|
|    max|119.32403272381589|

 The questions with with the most significance in predicting the value of MENTHLTH are _RFHLTH and _PHYS14D.


1.POORHLTH

|summary|        prediction|
|-------|------------------|
|  count|            486303|
|   mean|29.994067484677764|
| stddev| 15.17115394934401|
|    min|20.774557585695142|
|    max|144.38420925779346|


The questions with with the most significance in predicting the value of POORHLTH are _RFHLTH and _PHYS14D.


