## Overview

I approached this problem more like what one would see in a Jupyter notebook and less like "productionalizing" an ETL as the task seemed more exploratory in nature. That and I think it's a bit easier to follow that way than breaking it into a bunch of tiny classes.

The technology used here is Spark via Java. Scala of course would be better, but at my current company we write most of our Spark code in Java and that's what I'm most comfortable with at the moment.

My intent was to structure the analysis as a straightforward series of transformations, where it's easy to see what's happening at each step. These transformations are:
1. **Load**: load the original data file
2. **Clean**: parse the original data frame and pull it some fields (IPs, endpoints) useful for later analysis
3. **Sessionize**: this is the first interesting transformation, as this does the calculation of sessions via window functions.
4. **Aggregate**: the aggregations are run on the sessionized data frame to calculate session metrics like length and unique visits.
5. **Analyze**: answer the questions from the prompt and print the output

The tests were written to support this flow and test the correctness of these transformations.

## Build and Run Instructions

This is a maven project. To build the jar:
```sh
mvn clean compile assembly:single
```

This should put the artifact in the `target/` directory. To run:
```sh
java -jar target/DataEngineerChallenge-1.0-SNAPSHOT-jar-with-dependencies.jar
```

To run tests:
```sh
mvn test
```

## Answers to Analysis Questions

Answers are based on a 15-minute inactivity threshold.

1. Average session time (seconds): 100.73
2. Unique visits per session: 8.04
3. Most engaged users:

| client_ip      | session_length_seconds |
|----------------|-----------------------:|
| 106.186.23.95  | 2069                   |
| 52.74.219.71   | 2069                   |
| 119.81.61.166  | 2069                   |
| 125.19.44.66   | 2068                   |
| 125.20.39.66   | 2068                   |
| 192.8.190.10   | 2067                   |
| 54.251.151.39  | 2067                   |
| 180.211.69.209 | 2067                   |
| 122.15.156.64  | 2066                   |
| 203.191.34.178 | 2066                   |

## Summary

Indeed there were some challenges here. In particular, it was tough to come up with a good heuristic to filter out what were obviously bots scraping the site. I didn't think we wanted to include those in our analysis of sessions, but I could see an argument for it.
A simple way to do this might be to define a threshold of clicks/second of a session and filter out any sessions with too high a click rate. Another thought I had, and you can see that somewhat implemented, was to label endpoints as "api" or "user". This allows us to filter one or the other out of the dataset and produce metrics accordingly. I believe there's definitely more work to be done here, but I ran out of time before pursuing it further.

Overall this was a fun exercise and I enjoyed thinking through it. Thanks again for your time and consideration.
