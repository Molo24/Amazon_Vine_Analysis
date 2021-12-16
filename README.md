# Amazon_Vine_Analysis

## Overview
Amazon Vine is a service which allows Goods Providers to receive reviews for their products. After paying a fee, Goods Providers provide products to Amazon Vine members who then publish reviews of the product. The purpose of the below analysis was to investiage Amazon Reviews data (both Vine and non-Vine reviews) of video games sold on Amazon to determine if there is a bias in the Vine reviews compared to the non-Vine reviews. 

To perform the described analysis, the selected [Video_Games_v1_00](https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt) dataset underwent an ETL process utilizing PySpark with the Extracted and Transformed data being Loaded onto an AWS RDS Postgres instance. Following the ETL process, PySpark was used to determine if there was any bias toward favorable reviews from Vine members.
