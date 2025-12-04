# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col, when_matched

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders! :cup_with_straw:")
st.write(
    """Orders that need to be filled.
    """
)

# --- 1. ESTABLISH THE CONNECTION ---
# We replace get_active_session() with st.connection
cnx = st.connection("snowflake")
session = cnx.session()

# --- 2. GET THE DATA ---
my_dataframe = (
    session.table("smoothies.public.orders")
    .filter(col("ORDER_FILLED") == 0)
    .collect()
)

if my_dataframe:
    # Make the dataframe editable
    editable_df = st.data_editor(my_dataframe)
    
    submitted = st.button('Submit')
    
    if submitted:
        # --- 3. SAVE THE CHANGES ---
        try:
            og_dataset = session.table("smoothies.public.orders")
            edited_dataset = session.create_dataframe(editable_df)

            og_dataset.merge(
                edited_dataset,
                (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID']),
                [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
            )
            st.success('Orders updated!', icon='👍🏻')
        except Exception as e:
            st.warning(f'Something went wrong: {e}', icon='⚠️')

else:
    st.success('There are no pending orders right now.', icon='👍🏻')
