# Project Title

In Class Repository - Data Set Reports

## Data Set #1 Austin 311 Calls 
This data set contains 436,000 public complaints to the city of Austin between the years 2013 and 2017. The data is in the format of a .csv file, which would make parsing very simple. Each complaint comes with key identifying information such as: description, address, district code, date, time, and the department responsible for resolving the issue. An important thing to note about the descriptions is that they are brief two to five word phrases that are often reused for similiar compliants, for example "Loose Dog". Each complaint also has a complaint type in keycode form that could be used to make large groupings of complaints. 

```
[Austin 311 Calls](https://www.kaggle.com/jboysen/austin-calls) - A link to the data file
```

### Questions
Due to the details recorded with each compliant, there are many questions that could be answered with this dataset. What areas of the city see the most complaints and what type of complaints are they? What areas of the city see the least amount of complaints? What complaints are reported the most often? What city department has the largest number of complaints/the most amount of work? What is the  number of open cases and the department associated with them?

### Applications
I find this data set interesting becuase if it is analyzed, it could provide a lot of useful information for the city of Austin. With the information provided, Austin could try and make the process of solving complaints as efficient as possible. If they know that a certain area is historically the most common to have wild animal citings, then they could keep more Animal Control officers on staff there. They could also find out simpler information like what the people of Austin complain about the most. If there is an area that has the least amount of complaints, they could see what that area is doing right and apply it to other areas. I imagine representing this data with google maps and plotting all of the complaints. You could filter by keycode, department, or even more specific complaints such as "Loose Dog". This would be an interesting map to look at, especially if you are familiar with Austin.



## Data Set #2 Medical Appointment No Shows
This data set contains 300,000 medical appointment records from Brazil, identifying variables of each patient, and whether the showed up to their appointment or not. The non-medical information reported on each patient is their age, gender, whether they are on a government assistance program, whether they received a text message about their appointment, and the neighbourhood of the hospital. The medical information is provided as either a 1 or a 0 for Hipertension, Diabetes, Alcoholism, and Handicap.


```
[Medical Appointments](https://www.kaggle.com/joniarroba/noshowappointments) - A link to the data file
```

### Questions
There are many obvious questions that could be asked about this dataset to determine what some of the factors could be for patients to miss their appointments. What medical condition/combination of medical conditions are most associated with patients not coming to appointments? Do text messages before appointments make a large difference in whether the patient shows up or not? What age range has the difficult showing up to appointments? What hospitals have the hardest/easiest time getting patients to show up to their appointments? What time of day are the most appointments missed? You also have 300,000 records on citizens' health and so this dataset could be used to figure out questions like what is the gender distribution between brazilians effected by alcoholism.

### Applications
This data set interests me because it asks an interesting question about what causes people to miss apointments and the health demographics of the patients. There are obvious assumptions that I would make about this data, but I am interested in seeing if they are real. For example, do a larger percent of alcoholics mis there apointments? I would also want to know if text mesages before appointments really help make sure that the patients show up. This is a pretty unique data set and I think that it could shed some light on what parts of their health that people care about.


## Data Set #3 Uber Pickups
This data set contains data on over 17.8 million Uber pickups in New York Cty. The data was taken from April to September in 2014 and from January to June in 2015. In addition to Uber pickups, the data set also contains pickup information from other vehicle for hire companies in New Yoirk, such as Lyft and taxi companies. The regular format for the data is date, time, and location of the pickup. The data set is 115 MB with 6 Uber .csv data files and 10 .csv files associated with other companies.

```
[Uber Pickups](https://www.kaggle.com/fivethirtyeight/uber-pickups-in-new-york-city) - A link to the data file
```

### Questions
There are many questions that could be answered with data set that both pertain specifically to Uber and to the vehicle for hire service industry as a whole. How much of the vehicle for hire business does Uber control? What are the busiest pickup times and zones for Uber on any given day? Who are the major players in the vehicle for hire industry in New York City? What is the average amount of pickups at any location/zone by hour? Are there city areas where one vehicle for hire company is preffered over others? How does the amount of vehicle for hire pickups coincide with heavy traffic times? 

### Applications
This is an interesting article for me because I recently read an article that explained how Uber prices its rides. The article mentioned that Uber bases prices on the concentration of pickup requests in specific zones, and that sometimes to get a cheaper uber all that you have to do is cross the street into a less busy zone. If you could get the average pickup amount of pickups by hour or half for each zone then you could figure out the cheapest time to get an uber from any zone. Of course you would need to know how Uber splits up its zones and split the data files into collections for each zone based off of the location. This could be a pretty useful application for this for this data set. It would also be interesting to learn if Uber was at the top of the vehicle for hire industry that year and who the other close competitors where. If there are parts of the city where a certain company does better than others, than maybe you could link it to a reason. For example, taxi services might be better in locations with a higher concentration of old people. 


