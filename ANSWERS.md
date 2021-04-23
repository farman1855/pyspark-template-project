#Answers of Bonus Questions 
- Config management

```
Implemented
```
- Logging and alerting

```
Implemented
```
- Data quality checks (like input/output dataset validation).

```
Implemented
```

- How would you implement CI/CD for this application?
```
As per my experiences i mostly used jenkins to build a CI/CD pipeline for pyspark application and in case of scala there is sbt.
 ```
- How would you diagnose and tune the application in case of performance problems?
```
In spark when we run a job main parameter is memory and the amount of memory is dependent on your data and what you run before write.
If you are doing reading and writing data, then you will need very little memory per cpu because the dataset is never fully materialized 
before writing it out. If you are doing aggregate operations all of those will require much ore memory. 
The exception to this rule is that spark isn't really tuned for large files and generally is much more performant when dealing with sets of 
reasonably sized files.So in our case we need to read data using partitions on date.

```
- How would you schedule this pipeline to run periodically?
```
Using Airflow we can schedule our job which will run preodically.
```
