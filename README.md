## PinInterest-Data-Pipeline
#Pinterest crunches billions of data points every day to decide how to provide more value to their users. Through the use of AWS Cloud, I will mimick a similar system to handle a similar workload

# Architecture: AWS Managed Services for Kafka (AWS MSK)

## Why AWS MSK

Amazon Managed Streaming for Apache Kafka (Amazon MSK)* is a fully managed service used to build and run applications that use Apache Kafka to process data. Apache Kafka is an open-source technology for distributed data storage, optimized for ingesting and processing streaming data in real-time. 

Apache Kafka clusters are challenging to setup, scale, and manage in production. Amazon MSK makes it easy for you to build and run production applications on Apache Kafka without needing Apache Kafka infrastructure management expertise.

## The Data and Kafka

Within the PinInterest architecture, I have identitfied three main data structures: which are posts, user and geo data

******************************************************************
post example data: {'index': 3454, 'unique_id': '46bd3f86-b09d-4e29-9033-7ff2df595e51', 'title': 'What can you use to color resin?', 'description': 'HELPFUL RESOURCES – Check out my resin colorants resources page here with links to all the products mentioned in this article (and more). Let me know if you have any that you lo…\xa0', 'poster_name': 'Mixed Media Crafts', 'follower_count': '6k', 'tag_list': 'Epoxy Resin Art,Diy Resin Art,Diy Resin Crafts,Resin Molds,Ice Resin,Resin Pour,Diy Epoxy,Diy Resin Painting,Diy Resin Dice', 'is_image_or_video': 'image', 'image_src': 'https://i.pinimg.com/originals/d4/12/78/d4127833023ca32600571ddca16f1556.jpg', 'downloaded': 1, 'save_location': 'Local save in /data/diy-and-crafts', 'category': 'diy-and-crafts'}
******************************************************************
geolocation data: {'ind': 3454, 'timestamp': datetime.datetime(2021, 7, 25, 2, 20, 29), 'latitude': -0.375174, 'longitude': 49.8106, 'country': 'Cambodia'}
******************************************************************
user data: {'ind': 3454, 'first_name': 'Robert', 'last_name': 'Murphy', 'age': 48, 'date_joined': datetime.datetime(2017, 9, 26, 16, 31, 56)}

This data will be stored in three topics each with a sufix of .pin, .geo and .user representing the three data structures seen above

![Kafka Bucket with Confluent IO](image.png)

![creation of plugin and connector](image-1.png)
