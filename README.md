# fifa19

How to execute ?
Please install Postgres database into docker using below steps:

Open docker terminal.
run docker pull postgres:11.5.
run docker run --name [container_name] -e POSTGRES_PASSWORD=[your_password] -d postgres.
verify whether postgres container is created, run docker ps.
login into postgres database, run psql -U postgres.
check existing database list, run \l.
create new database by running CREATE DATABASE [database_name] or take existing database for this project.
Please download the project from gitHub and import it into Intellij.

Download scala plugin for Intellij and add scala Framework Support for the imported project.

Download source file from https://www.kaggle.com/karangadiya/fifa19/download and unzip it.

Open Fifa.scala in IntelliJ and specify below attributes as per your setup.

Put below required connectors in build.sbt file

postgresql
spark-core 
spark-sql

As of now the postgrep connection parameters I have hardcoded which is installed in my local.

Execute Fifa.scala from IntelliJ.

Output for Step 2 is available from Run section of IntelliJ.

Postgres table data can be checked from shell using below steps

login into postgres database, run psql -U postgres
use particular database, run \c [database_name]
verify whether table is created, run \dt
check table description, run \d [table_name]
check record count, run select count(1) from [table_name]. Right now output will be 18207.
check table data, run select * from [table_name] limit 5


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
