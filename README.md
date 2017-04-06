# Insight Data Engineering Coding Challenge.
1. [Solution Description](README.md#solution-description)
2. [Requirements and Dependencies](README.md#requirements-dependencies)
3. [General Comments](README.md#general-comments)

# Solution Description
In general, I was trying to provide all the features via Spark's Datasets. The reason is simple - scalability without changing any piece of the code. However, both F3 and F4 I was not able to solve via just datasets. 

### Code Organization
- apps/MetricsProviderSparkApp.scala - driver with Spark
- common/package.scala provides a set of common definitions/declarataions for this project
- login/LoginManager.scala - responsible for managing login attempts/blocking/tracking failures. Represented as 2 HashMaps, which get cleaned (either every 5mins or 20s basically)
- frame/TimeFrame.scala - models a sliding Time Window without storing all of the information for the whole dataset. **The idea is to avoid multiple passes over the data**

### Feature 1:
**Solution:** A simple query: grouping rows by host name and counting within each group. 

### Feature 2:
This is quite a confusing problem. May be because I do not quite understand the meaning of frequency - frequency is not just the number of times a resource is accessed! Frequency is the number of times it's accessed per unit of time! Given the non-uniform nature of accesses in here - I simplified the problem and computed the total bytes sent over the whole period.

**Solution:** similar to F1, group by resource name, using only GET/POST methods (basically apply filtering/cleaning) and then apply aggregation.

### Feature 3:
For this task, I've created a TimeFrame class that acts as a sliding time window. The idea is to avoid multiple passes over the data! I have not tested it thouroughly as I'm running late, but it does pass the provided test. However, the idea itself is correct, there might be some small details in implementation that could raise issues after further testing.

### Feature 4:
For this task, I've created a LoginManager class that is responsible for accepting an Event and making a decision about security breaches via the logic described in the assignment

### Feature 3-like Additional:
At first, I didn't understand the point of Feature 3 assignment. In general, I do not think that it's important from both technical and business points of view to know up to a second the busiest hours. A much cleaner idea is to separate by the hour of the day and identify periods during the day that are the busiest ones. In the log\_output directory you can find a file: F3-like.txt that will identify the top 10 busiest hours out of all them. Here, it's natural to use Spark's ability and API to group the Events by the hour and then further process.

# Requirements and Dependencies
- Apache Spark >= 2.0 and having spark-submit in the `$PATH`
- Scala 2.11

# General Comments
- No Personal Unit Tests...
- I assume you are running from the root repo folder with just:
```
bash run.sh
```
