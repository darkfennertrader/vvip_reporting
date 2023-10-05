import pandas as pd
import numpy as np
from IPython.display import display, HTML


def wrapping_data(df):
    custom_styling = [
        {
            "selector": "th",
            "props": [
                ("background", "#00abe7"),
                ("color", "white"),
                ("font-family", "tahoma"),
                ("text-align", "center"),
                ("font-size", "15px"),
            ],
        },
        {
            "selector": "td",
            "props": [
                ("font-family", "tahoma"),
                ("color", "black"),
                ("text-align", "left"),
                ("font-size", "15px"),
            ],
        },
        {
            "selector": "tr:nth-of-type(odd)",
            "props": [
                ("background", "white"),
            ],
        },
        {"selector": "tr:nth-of-type(even)", "props": [("background", "#e8e6e6")]},
        {"selector": "tr:hover", "props": [("background-color", "#bfeaf9")]},
        {"selector": "td:hover", "props": [("background-color", "#7fd5f3")]},
    ]
    s1 = df.style
    s1.set_table_styles(custom_styling)
    # s1.hide(axis="index")
    # NOTE: Adding div Style with overflow!

    return display(
        HTML("<div style='width: 1800px; overflow: auto;'>" + s1.to_html() + "</div>")
    )
