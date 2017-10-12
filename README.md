# myTrusted.Reviews

myTrusted.Reviews is an e-commerce platform with a social network to provide product recommendations. The goal is to help users find the right product quickly by providing them personalized statistics on how many reviews were written by their trusted reviewers for each research result, and the reviews themselves. So users can focus on getting to know more about the product from their trusted reviewers. To try out the platform, please visit [myTrusted.Reviews](http://myTrusted.Reviews). Here are the [slides of my presentation](http://bit.ly/2g5l0rp). 

## Data
From Amazon.com between 1996 and 2014, containing 83 million reviews, 9 million products, and 81 million ratings. 

## User-Trusted Reviewer Relations
If user A endorses user B and user E's reviews, then user B and user E are user A's first-degree trusted reviewers. Further, if user B endorses user C and user D's reviews, then user C and user D are user A's second-degree trusted reviewers.

## Pipeline
Data in flat files were first loaded into S3 via shell script. Spark then reads data from S3, generates the product-reviewer table and the user-trusted reviewer relations tables, and writes results directly into Elasticsearch. Flask was used to build an interactive website to process user queries. 
![project pipeline](https://github.com/EmpiricalAnalysis/myTrusted.Reviews/blob/master/extra/pipeline.jpeg "Pipeline")


## Acknowledgment
The data used to power this platform were courtesy of Julian McAuley of UCSD. 