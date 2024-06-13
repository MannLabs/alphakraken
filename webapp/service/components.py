"""UI components for the web application."""

import pandas as pd
import streamlit as st


def show_filter(df: pd.DataFrame, text_to_display: str) -> pd.DataFrame:
    """Filter the DataFrame on user input by case-insensitive textual comparison in all columns."""
    user_input = st.text_input(text_to_display, None)
    if user_input is not None:
        user_input = user_input.lower()
        mask = df.map(lambda x: user_input in str(x).lower()).any(axis=1)
        mask |= df.index.map(lambda x: user_input in str(x).lower())
        return df[mask]
    return df
