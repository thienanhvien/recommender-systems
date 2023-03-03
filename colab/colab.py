### Collaborative Filtering

import findspark
findspark.init()
from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName('Recommendation_system').getOrCreate()
from pyspark.ml.recommendation import ALSModel
import pickle
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import col
import _thread
import streamlit as st

@st.cache_resource
def create_spark_session():
    spark = SparkSession.builder.appName('Recommendation_system').getOrCreate()
    return spark

spark = create_spark_session()


# Load data and models
# df = pd.read_csv('Files/Review.csv', index_col=0)
# df_schema = StructType([
#     StructField("customer_id", StringType(), True),
#     StructField("product_id", StringType(), True),
#     StructField("customer_rating", IntegerType(), True),
# ])
# # data = spark.createDataFrame(df, schema=df_schema)

# data = spark.read.csv("Files/Review.csv",header=False,schema=df_schema)

# data = spark.read.csv("Files/Review.csv", header=True, inferSchema=True)


# data = data.drop('_c0')

# data = data.drop_duplicates()
# indexer = StringIndexer(inputCol='product_id', outputCol='product_id_idx')
# indexer_model = indexer.fit(data)
# data_indexed = indexer_model.transform(data)
# indexer1 = StringIndexer(inputCol='customer_id', outputCol='customer_id_idx')
# indexer1_model = indexer1.fit(data_indexed)
# data_indexed = indexer1_model.transform(data_indexed)
# als_model = ALSModel.load('RecommendationSystem_ALS')
# df_product = spark.read.csv('Files/Product_image.csv', header=True, inferSchema=True)

# @st.cache(allow_output_mutation=True)
st.cache_resource
def load_data():
    data = spark.read.csv("Files/Review.csv", header=True, inferSchema=True)
    data = data.drop('_c0')
    data = data.drop_duplicates()
    indexer = StringIndexer(inputCol='product_id', outputCol='product_id_idx')
    indexer_model = indexer.fit(data)
    data_indexed = indexer_model.transform(data)
    indexer1 = StringIndexer(inputCol='customer_id', outputCol='customer_id_idx')
    indexer1_model = indexer1.fit(data_indexed)
    data_indexed = indexer1_model.transform(data_indexed)
    als_model = ALSModel.load('RecommendationSystem_ALS')
    df_product = spark.read.csv('Files/Product_image.csv', header=True, inferSchema=True)
    return data_indexed, als_model, df_product

data_indexed, als_model, df_product = load_data()

# Define recommend_product function
def recommend_product(customer_id):
    customer_recs = als_model.recommendForAllUsers(10)
    df_customer_customer_id = data_indexed.select('customer_id_idx', 'customer_id').distinct()
    new_customer_recs = customer_recs.join(df_customer_customer_id, on='customer_id_idx', how='left')
    df_product_product_idx = data_indexed.select('product_id_idx', 'product_id').distinct()
    find_customer_rec = new_customer_recs.filter(new_customer_recs['customer_id'] == (customer_id))
    customer = find_customer_rec.first()
    lst = []
    for row in customer['recommendations']:
        row_f = df_product_product_idx.filter(df_product_product_idx.product_id_idx == row['product_id_idx'])
        row_f_first = row_f.first()
        lst.append((row['product_id_idx'], row_f_first['product_id'], row['rating']))
    df_rec = spark.createDataFrame(lst, ['product_id_idx', 'product_id', 'rating'])
    df_joined = df_rec.join(df_product, on='product_id')
    df_joined = df_joined.select('product_id', 'product_name', 'rating', 'image')
    customer_id_idx = df_customer_customer_id.filter(df_customer_customer_id.customer_id == (customer_id)).select('customer_id_idx').collect()[0][0]
    df_final = df_joined.withColumn('customer_id', lit(customer_id_idx))
    return df_final


