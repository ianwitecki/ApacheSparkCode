# Spark GraphX Assignment  

## In Class Questions
1. There are 22,547 distinct descriptor names.
1. The 10 most common descriptor names are:

| Descritpors        |
|--------------------|
|(Humans,117261)     |
|(Female,42298)      |
|(Male,39738)        |
|(Animals,37379)     | 
|(Adult,25642)		 |
|(Middle Aged,20427) |
|(Aged,14201) 		 |
|(Adolescent,11319)  |
|(Child,11035)		 |
|(Rats,9767)		 |

1. The 10 most common descriptor names in major topics are:

| Descritpors        	   |
|--------------------------|
|(Research,1649)     	   |
|(Disease,1349)      	   |
|(Neoplasms,1123)    	   |
|(Tuberculosis,1066) 	   |
|(Public Policy,816)	   |
|(Jurisprudence,796) 	   |
|(Demography,763)		   |
|(Population Dynamics,753) |
|(Economics,690)  		   |
|(Medicine,682)		 	   |

1. Theoretically could be 254,172,331  pairs.
1. There are 2,717,162  actual pairs.
1. The graph of pair terms should not be directed becuase there will never be case where a descriptor has an ed    ge to another descriptor, but there is no edge from that the destination descriptor back. This implies that our gra    ph is only interested in how related different descriptors are to each other.


## Out of Class Questions

## All Descriptors

1. There are 878 connected components. One component has 13,610 nodes and the rest have under 5 nodes. 

1. The top words by page rank are: 
| Page Rank       | Descriptor       |
|-----------------|------------------|
|109.132		  | Humans			 |
|78.1916	      |	Animals			 |
|76.2472          | Female			 |
|76.2188		  |	Male			 |
|58.2823	 	  |	Adult			 |
|51.6020		  |	Middle Aged		 |
|41.9215		  |	Aged			 |
|39.5861		  |	Adolescent		 |
|37.8576		  |	Time Factors	 |
|37.8156	 	  |	Child			 |

1. The following is histogram of the degree distribution for all nodes:

	![alt text](/images/degDist)

1. Shortest Path
	1. 1 apart
	1. 2 apart
	1. 2 apart


## Major Topic Descriptors

1. There are two connected components. Component 0 has 22,546 nodes and Component 1 has 1 node.
1. The top words by page rank are: 
| Page Rank       | Descriptor       	|
|-----------------|---------------------|
|72.2580		  | Research			|
|50.2305      	  |	Disease			 	|
|34.5911          | Neoplasms			|
|26.0305		  | Blood				|
|24.5881		  | Pharmacology     	|
|22.2252	      | Tuberculosis     	|
|19.5722          | Drug Therapy		|
|19.4702		  | Toxicology			|
|17.3685	      | Plants				|
|16.7228          | Biomedical Research |

1. The following is histogram of the degree distribution for all nodes:

	![alt text](/images/degDistMajor)

1. Shortest Path
	1. 2 apart
	1. 2 apart
	1. 3 apart

























































































































































