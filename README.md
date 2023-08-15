# recsys_real_time_user_behavior_analysis
Combine Kafka and Flink to real-time receive and analyze user behaivor data, in some case will store analyzed result to db.
### Hot item module
Kafka real-time receive user click data, Flink calculate popular products within one hour, and store the results in Redis for use by the recommendation system.
