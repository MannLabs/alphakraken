"""UI components for the web application."""

import pandas as pd
import streamlit as st


def show_filter(df: pd.DataFrame, filter_text: str) -> pd.DataFrame:
    """Filter the DataFrame on textual comparison in all columns."""
    user_input = st.text_input(filter_text, None)
    if user_input is not None:
        user_input = user_input.lower()
        mask = df.applymap(lambda x: user_input in str(x).lower()).any(axis=1)
        return df[mask]
    return df
