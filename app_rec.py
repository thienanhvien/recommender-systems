from colab.colab import recommend_product
from contbased.contbased import content_based_product
import streamlit as st
# import findspark
import pandas as pd
# findspark.init()
# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName('Recommendation_system').getOrCreate()

# Load the product data and the similarity matrix
product_data = pd.read_csv('Files/Product_data.csv', index_col=0)
product_images = pd.read_csv('Files/Product_image.csv', index_col=0)

# Set up the Streamlit app
st.title("Product Recommendation System")

sidebar_option = st.sidebar.radio("Table of Contents", ["Business Understanding", "Recommender Systems"])

if sidebar_option == "Business Understanding":
    st.header("Business Understanding")

    # Add some text about recommendation systems
    st.write("Build a Recommendation System to help Tiki recommends and suggests products for users/customers.")
    st.write("<h1 style='font-size: 20px;'>Collaborative Filtering</h1>", unsafe_allow_html=True)
    st.write("Collaborative filtering relies on the preferences of similar users to offer recommendations to a particular user.")
    st.write("Collaborative does not need the features of the items to be given. Every user and item is described by a feature vector or embedding.")
    st.write("It creates embedding for both users and items on its own. It embeds both users and items in the same embedding space.")
    st.write("It considers other users’ reactions while recommending a particular user. It notes which items a particular user likes and also the items that the users with behavior and likings like him/her likes, to recommend items to that user.")
    st.write("It collects user feedbacks on different items and uses them for recommendations.")
    st.image("https://i0.wp.com/analyticsarora.com/wp-content/uploads/2022/03/collaborative-filtering-shown-visually.png?resize=800%2C600&ssl=1")
    st.write("<h1 style='font-size: 20px;'>Content-Based Filtering</h1>", unsafe_allow_html=True)
    st.write("Content-Based recommender system tries to guess the features or behavior of a user given the item’s features, they react positively to.")
    st.write("It makes recommendations by using keywords and attributes assigned to objects in a database and matching them to a user profile.")
    st.write("The user profile is created based on data derived from a user’s actions, such as purchases, ratings (likes and dislikes), downloads, items searched for on a website and/or placed in a cart, and clicks on product links.")
    st.image("https://www.iteratorshq.com/wp-content/uploads/2021/06/content_based_collaborative_filtering.jpg")
    # Add some images to illustrate the two types of recommendation systems
    
elif sidebar_option == "Recommender Systems":
    st.header("Recommender Systems")

    # Explain the two types of recommendation systems
    st.write("Here's two types of recommendation systems: Content-Based Filtering and Collaborative Filtering.")
    st.write("<strong>Content-Based Filtering</strong> recommends products based on the accumulated knowledge of users. It consists of a resemblance between the items. The proximity and similarity of the product are measured based on the similar content of the item.", unsafe_allow_html=True)
    st.write("<strong>Collaborative Filtering</strong> recommends products based on based on the user's historical choices. It focuses on relationships between the item and users; items’ similarity is determined by their rating given by customers who rated both the items.", unsafe_allow_html=True)

    option = st.selectbox(
        "Select the type of recommendation system you would like to use",
        ("Content-Based Filtering", "Collaborative Filtering")
    )

    if option == "Content-Based Filtering":
        st.header("Content-Based Filtering")

        product_names = product_data['product_name'].tolist()

        # Add a search box for product name
        search_term = st.text_input('Enter a product name to search:', '')

        # Filter the product names based on the search term
        if search_term:
            product_names = [name for name in product_names if search_term.lower() in name.lower()]

        if len(product_names) == 0:
            st.write('No products found for the given search term.')
        else:
            # Add a dropdown to select a product
            selected_product = st.selectbox('Select a product', product_names)

            # Add a search button
            if st.button('Search'):
                # Get the recommendations for the selected product
                recommendations = content_based_product(selected_product)

                # Display the recommendations
                st.write(f'Top 10 products similar to {selected_product}:')

                # Display the first 5 products in the first column and the remaining 5 products in the second column
                num_rows = 5
                cols = st.columns(2)
                with cols[0]:
                    for i, row in recommendations.head(num_rows).iterrows():
                        if row['product_name'] == selected_product:
                            st.write(row['product_name'], ": **chosen product**", unsafe_allow_html=True)
                        else:
                            st.write(row['product_name'])
                        st.image(row['image'], width=None)
                        st.write(f"Similarity score: {row['Similarity-Score']:.2f}")
                with cols[1]:
                    for i, row in recommendations.tail(num_rows).iterrows():
                        if row['product_name'] == selected_product:
                            st.write(row['product_name'], ": **chosen product**", unsafe_allow_html=True)
                        else:
                            st.write(row['product_name'])
                        st.image(row['image'], width=None)
                        st.write(f"Similarity score: {row['Similarity-Score']:.2f}")

    elif option == "Collaborative Filtering":
        st.header("Collaborative Filtering")

        # Add a text input to get the customer ID
        customer_id = st.text_input('Enter your customer ID:')

        # Add a search button
        if st.button('Search'):
            # Get the recommendations for the selected customer ID
            recommendations = recommend_product(customer_id)

            # Display the recommendations
            st.write(f'Top 10 products recommended for customer {customer_id}:')

            # Create two columns to display the products side by side
            col1, col2 = st.columns(2)

            # Convert the recommendations into a Python list
            rec_list = recommendations.take(10)

            # Display the first 5 products in the first column
            with col1:
                for row in rec_list[:5]:
                    st.image(row['image'])
                    st.write(f"Product Name: {row['product_name']}")
                    st.write(f"Rating: {row['rating']:.2f}")

            # Display the remaining 5 products in the second column
            with col2:
                for row in rec_list[5:]:
                    st.image(row['image'])
                    st.write(f"Product Name: {row['product_name']}")
                    st.write(f"Rating: {row['rating']:.2f}")