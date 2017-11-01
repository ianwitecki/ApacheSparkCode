# Spark Classification Assignment  

## In Class Questions
1. There are 47 columns and 2,946 rows.
1. The last column has 5 different values
1. Number of rows for each value in the last column
    1. 
        |values|count |
        |------|------|
        |   3  |   29 |
        |   0  |  642 |
        |   1  |   15 |
        |   4  | 1192 |
        |   2  | 1068 |

1. The start of the table from problem 4
   1. 1.0                     -0.08515838753061852   ... (44 total)
      -0.08515838753061852    1.0                    ...
      0.04864301699458318     0.02074597863178863    ...
      0.07788055691716779     0.030339382185619126   ...
1. Columns 22, 23, and 26 are the most highly correlated with the last column.
   1. 
        |column|value |
        |------|------|
        |  22  | 0.38 |
        |  23  | 0.21 |
        |  26  | 0.10 |


## Out of Class Questions

1. The best type of classification scheme for the last column with mutliple classifications is the OneVsRest using a Linear Regression. Below are the different schemes and their evaluations. The key columns are below. The most important columns are 19, 20, 6, and 9 as they appear in both of the key features this and the binary classifiers.  
   1. 
        | Scheme          | Accuracy |
        |-----------------|----------|
        |  Random Forest  | 0.53496  |
        |  Decision Tree  | 0.50874  |
        |  OneVsRest      | 0.54020  |

   1. 
        | Weight  | Column  |
        |---------|---------|
        |  0.236  | 19      |
        |  0.096  | 20      |
        |  0.086  | 6       |
        |  0.086  | 9       |
        |  0.060  | 8       |

1. The best type of classification scheme for the last column with a binary classification is the Random Forest Scheme. Below are the different schemes and their evaluations. The key columns are below. The most important features for the binary classification are also displayed below.
   1. 
        | Scheme                  | Accuracy |
        |-------------------------|----------|
        |  Random Forest          | 0.86555  |
        |  Decision Tree          | 0.81257  |
        |  Linear Regression      | 0.84111  |

   1. 
        | Weight  | Column  |
        |---------|---------|
        |  0.375  | 19      |
        |  0.120  | 6       |
        |  0.113  | 20      |
        |  0.029  | 10      |
        |  0.023  | 9       |


