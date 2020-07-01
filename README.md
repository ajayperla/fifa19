# fifa19


Prerequisites:
--> Java 8 --> IntelliJIdea --> SBT plugin need to be configured in intelliJIdea

build.sbt connectors:

postgresql 
spark-core
spark-sql



Below are the steps to execute the this project :
Clone the project from git and import it into intelliJIdea and build the project
Fifa is the driver program which you need to run before runnig your input file should be in data directory in intelliJIdea.

Once you run the driver code, kindly notice console for the expected output

Approach: 

Loaded data into spark dataframes and selected required columns and persisted into memory for faster performance. Wherever the intermediate dataframes are reusable, I have Persisted and Unpersisted from memory once the usage of that dataframe is done.

	   1. Filter out the data which includes only within 30 years of age with left footed players and found most number by doing descending order

     2. First I have taken players who are having highest Overall in thier respective Positions. Then taken 4 higest overall midfielders, 4 highest overal defenders and          2 highest overall strikers and one highest overall goal keeper "Irrespective of Club and Country".
		
		Assumption: I have took an assuption as there is no mandatory position among each group of midfielders, defenders and strikers.
		ex: In midfielders RM/RWM LCM/CM RCM/CM LM/LWM , who are having top most 4 overall among declared positions.

     3. I have cleaned the wage and value columns by removing special character and changed M and K into respective numbers by multipliing 1000000,1000 respectively.And        taken average of Wage and value columns and taken the Club who has the highest cummulative_Value and cummulative_Wage.
     
     4. We have to do group by of position and take average of value and get highest value by sorting
     
     5. I have taken the the goal keeper who is having highest Overall and selected attribute columns only. Then converted the columns and values into individual lists         and created a sorted map by values.Then took top 4 attributes from that which will be considered as the best qualities that a goal keeper should have.
     
     6. I have taken the the Striker who is having highest Overall and selected attribute columns only. Then converted the columns and values into individual lists and         created a sorted map by values.Then took top 5 attributes from that which will be considered as the best qualities that a striker should have.
     
    
Finally, Writing a data to postgres database using write spark API's with respected modes on docker
