BUILD INSTRUCTIONS
------------------

To build this project, run the following command:

    ./gradlew clean build assemble

This should produce a file called 'project-1.0.0-SNAPSHOT.jar' in the
'build/libs' subdirectory. This file contains the compiled code for the project.

RUNNING THE JOBS
----------------

The *.jar file contains two separate main classes:

- AllHourlyStatisticsJob
- LocationFilteredHourlyStatisticsJob

ALL HOURLY STATISTICS JOB
-------------------------

The "All Hourly Statistics Job" computes hourly statistics for all taxi trips
provided in the input dataset without performing any filtering.

To execute this job run the following command on the Hadoop NameNode:

    hadoop jar project-1.0.0-SNAPSHOT.jar edu.njit.cs643.groups3.jobs.AllHourlyStatisticsJob

The job will place the outputted results in the '/output/all_hourly_statistics'
directory of HDFS.

LOCATION-FILTERED HOURLY STATISTICS JOB
---------------------------------------

The "Location-Filtered Hourly Statistics Job" computes hourly statistics for all
taxi trips between two locations. The locations are integer values representing
different sections of NYC and the surrounding area. Refer to the provided
"location_ids.csv" file for the complete list of location IDs.

To execute this job, runt he following command on the Hadoop NameNode:

    hadoop jar project-1.0.0-SNAPSHOT.jar edu.njit.cs643.groups3.jobs.LocationFilteredHourlyStatisticsJob <pickup_location> <drop-off_location>

where <pickup_location> and <drop-off_location> are the integer location IDs.

For more information on the location IDs and to see maps of the different
locations, please visit https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
and review the "Taxi Zone Maps and Lookup Tables" section at the bottom of the
page.

NOTES ABOUT EXECUTING THE JOBS
------------------------------

In our experimentation, we found that t3.xlarge or similar AWS EC2 instances
were the minimum-sized instances that could consistently be used to execute the
jobs in this project. Smaller instances suffered from memory over-allocation
which required all the nodes in the cluster to be restarted in order to reset
their memory.

NOTES ABOUT THE INPUT DATA
--------------------------

For our testing, we used the "Yellow Taxi Trip Records (CSV)" files for June,
July, and August 2019 as our dataset. We did not test the jobs on any of the
other TLC datasets or months. However, we believe this project should work with
the "Green Taxi Trip Records (CSV)" dataset as the files are similarly
formatted. Additionally, any arbitrary selection of months should also work
correctly.

It should be noted however that the "For-Hire Vehicle Trip Records" and
"High Volume For-Hire Vehicle Trip Records" dataset will NOT work with this
project. This is because the For-Hire datasets provide a very different set of
data-points for each trip to the Yellow and Green Taxi datasets and are thus
not compatible with file parsing routines in this project.