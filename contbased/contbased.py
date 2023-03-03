### Content-based filtering

import pandas as pd
import pickle
import numpy as np
import streamlit as st

# Load the product data and the similarity matrix
product_data = pd.read_csv('Files/Product_data.csv', index_col=0)
product_images = pd.read_csv('Files/Product_image.csv', index_col=0)

partitions = []
for i in range(2):
    partition = np.load(f'ContentBased/cosine_similarity_partition_{i}.npy')
    partitions.append(partition)

# Combine the partitions into the cosine similarity matrix
sim = np.concatenate(partitions, axis=0)

@st.cache
# Define helper functions for lookup and similarity
def find_index_from_name(name) :
    return product_data[product_data["product_name"] == name]["Index"].values[0]
@st.cache
def content_based_product(name, n = 10):
    # Get the index of the given product name
    ind = find_index_from_name(name)

    # Get all the similarity values for the product, and convert it to a list
    sim_product = list(enumerate(sim[ind]))

    # Sort the product based on similarity values
    names, score = map(list, zip(*sorted(sim_product, key = lambda x : x[1], 
                                        reverse = True)[1:]))
    
    # Find the indexes of the top n most similar products
    top_n_indexes = names[:n]

    # Get the dataframe rows based on the indexes
    top_n_products = product_data.loc[product_data.index[top_n_indexes]]

    # Add the similarity scores to the dataframe
    top_n_products['Similarity-Score'] = score[:n]

    # Merge product images with top_n_products
    top_n_products = pd.merge(top_n_products, product_images, on='product_name')

    # Return the dataframe
    return top_n_products[['Similarity-Score', 'product_name', 'image']]
