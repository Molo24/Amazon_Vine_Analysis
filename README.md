# Amazon_Vine_Analysis

## Overview
Amazon Vine is a service which allows Goods Providers to receive reviews for their products. After paying a fee, Goods Providers provide products to Amazon Vine members who then publish reviews of the product. The purpose of the below analysis was to investiage Amazon Reviews data (both Vine and non-Vine reviews) of video games sold on Amazon to determine if there is a bias in the Vine reviews compared to the non-Vine reviews. 

To perform the described analysis, the selected [Video_Games_v1_00](https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt) dataset underwent an ETL process utilizing PySpark with the Extracted and Transformed data being Loaded onto an AWS RDS Postgres instance (a Postgres schema was also created via SQL). Following the ETL process, PySpark was used to determine if there was any bias toward favorable reviews from Vine members.

#### Tools Used
Tools used for this analysis: Google Colab (PySpark, ETL and bias analysis), pgAdmin (creation of SQL database and schema), Amazon Web Servies (RDS instance for Postgres database).

## Analysis
### ETL Process
The ETL process resulted in the creation of 4 tables using the Amazon review dataset. These 4 tables were then written to the RDS instance.

![customers_table](https://user-images.githubusercontent.com/89284280/146303021-f976cc7b-6722-48bb-984b-ded8db9b21d8.JPG)

![products_table](https://user-images.githubusercontent.com/89284280/146303029-42cbf199-a0b8-457a-a3e9-f5168191a232.JPG)

![reviews_table](https://user-images.githubusercontent.com/89284280/146303043-e77199ac-d167-4e5f-a3a5-4259069d071a.JPG)

![vine_table](https://user-images.githubusercontent.com/89284280/146303052-4378e234-543b-42ab-b36f-c74fd2833d8f.JPG)

![write_tables_to_RDS](https://user-images.githubusercontent.com/89284280/146303059-cb31a069-f5b5-4172-90fc-853a2472a473.JPG)

### Reviews Analysis
From the Vine Table found on RDS, it will then be read back in to a PySpark DataFrame:
![vine_table_read_in](https://user-images.githubusercontent.com/89284280/146303294-fb596197-dad1-41eb-a495-95e81de3627e.JPG)

Next, the goal was to arrive at a final data frame whereby Vine and non-Vine reviews were grouped separately to determine which group gave more favorable reviews. To get there, a series of new dataframes were created:<br>
- DataFrame of only Vine reviews:
```
vine_reviews_y_df = vine_vote_perc_updated_df.filter(vine_vote_perc_updated_df.vine == 'Y')
```

- DataFrame of only non-Vine reviews:
```
vine_reviews_n_df = vine_vote_perc_updated_df.filter(vine_vote_perc_updated_df.vine == 'N')
```

- Determine total reviews Vine v non-Vine:
```
total_paid_reviews = vine_reviews_y_df.count()
total_non_paid_reviews = vine_reviews_n_df.count()
```

- Determine total 5-star reviews Vine v non-Vine:
```
five_star_paid_reviews = vine_reviews_y_df.filter(vine_reviews_n_df.star_rating == 5).count()
five_star_non_paid_reviews = vine_reviews_n_df.filter(vine_reviews_n_df.star_rating == 5).count()
```

- Determine 5-Star rate (5) between Vine v non-Vine reviewers:
```
paid_5_star_rate = ((five_star_paid_reviews / total_paid_reviews)*100)
non_paid_5_star_rate = ((five_star_non_paid_reviews / total_non_paid_reviews)*100)
```
- Create the Summary DataFrame:
```
results_df = spark.createDataFrame(
    [
     ("Paid/Vine", total_paid_reviews, five_star_paid_reviews, paid_5_star_rate),
     ("Unpaid/non-Vine", total_non_paid_reviews, five_star_non_paid_reviews, non_paid_5_star_rate),
    ],
    ["Review Type", "Total Reviews", "Total 5-Star Reviews", "5-Star Rate"]
)
```
- Format the Summary DataFrame:
```
results_df = results_df.withColumn('Total Reviews (#)', F.format_number('Total Reviews', 0))
results_df = results_df.withColumn('Total 5-Star Reviews (#)', F.format_number('Total 5-Star Reviews', 0))
results_df = results_df.withColumn('5-Star Rate (%)', F.format_number('5-Star Rate', 2))
results_df = results_df.drop("Total Reviews", "Total 5-Star Reviews", "5-Star Rate")
```

## Results
From the analysis, we can see the following:
- How many Vine reviews and non-Vine reviews were there?
  - Vine: 94
  - non-Vine: 40,471
- How many Vine reviews were 5 stars? How many non-Vine reviews were 5 stars?
  - Vine: 48
  - non-Vine: 15,663
- What percentage of Vine reviews were 5 stars? What percentage of non-Vine reviews were 5 stars?
  - Vine: 51.06%
  - non-Vine: 38.70%

![summary_results](https://user-images.githubusercontent.com/89284280/146304909-bdc91663-6a63-4439-8e7a-be80af08af14.JPG)


