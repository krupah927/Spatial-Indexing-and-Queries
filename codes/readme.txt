*.scala files must be put under Simba's 'examples' folder, then build Simba by "sbt package" to get the jar.

To run a scala program, execute the following command:

spark-submit --class org.apache.spark.sql.simba.examples.[CLASS_NAME] simba_2.11-1.0.jar [ARGUMENTS]


Part 1:

sample.py
# Sample 10% Of the trajectories.
Usage: ./sample.py Input_Trajectories_CSV Sample_File

Trajectories.scala
# Partition the points and compute MBRs, output to a text file Of points and MBRs
CLASS_NAME: Trajectories
ARGUMENTS: Numer_Of_Partitions Sample_File Points_MBRs_CSV

plot_points_mbrs.py
# Plot points and MBRs as a graph.
Usage: ./plot_points_mbrs.py Points_MBRs_File_In_CSV Output_Image_In_PNG


Part 2:

Task 1:
Task1.scala
# Find all restaurants in a rectangle
CLASS_NAME: Task1
ARGUMENTS: Numer_Of_Partitions Input_POI_CSV Output_Result_CSV

Task 2:
Task2.scala
# Compute average number Of visitors near Tiananmen Square per hour.
CLASS_NAME: Task2
ARGUMENTS: Numer_Of_Partitions Input_Trajectories_CSV Output_Result_CSV

Task 3:
Task3.scala
# Compute the number of trajectories starting and stopping in the same quadrant or in different quadrants.
CLASS_NAME: Task3
ARGUMENTS: Numer_Of_Partitions Input_Trajectories_CSV Output_Result_TXT

Task 4:
Task4.scala
# Find nearby points and rank them (top 20).
CLASS_NAME: Task4
ARGUMENTS: Number_Of_Threads Numer_Of_Partitions Input_Trajectories_CSV Radius_In_Meters Output_Result_CSV

Task 5:
Task5.scala
# Count top 10 popular POIs in 2008 and 2009.
CLASS_NAME: Task5
ARGUMENTS: Number_Of_Threads Numer_Of_Partitions Input_Trajectories_CSV Input_POI_CSV Radius_In_Meters Output_File_2008 Output_File_2009
