# ETL pipeline for mobility data

This is a quick and dirty pipeline that I am using to prepare a dataset, that will be used for research purposes.
It is not intended to be a production pipeline, as it currently has many structural flaws. 

The pipeline collects data from a data warehouse containing CDRs and their metadata. Then it saves this data in a local HDFS filesystem, updating it with new data every week.
