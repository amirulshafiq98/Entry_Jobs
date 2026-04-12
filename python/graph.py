"""
GRAPH SCRIPT
"""

import plotly.express as px
import pandas as pd
from datetime import datetime

# Load final output from pipeline
df = pd.read_csv("output/graph_ready.csv")

# Helper function:
def top_x(label):
    filtered = df[df["timeframe"] == label].copy()
    filtered = filtered.sort_values(by="avg_salary", ascending=False).head(10)
    return filtered

# Start with YTD as the default view
data = top_x("YTD")

fig = px.bar(
    data,
    x="role",
    y="avg_salary",
    text=data["job_count"].astype(str) + " jobs",
    template="plotly_white",
    height=650
)

# Build timeframe buttons
btns = []
for label in ["YTD", "Last Month", "Last 6 Months", "Last Year"]:
    df_step = top_x(label)

    if label == "YTD":
        display_name = f"YTD {datetime.now().strftime('%Y')}"
    else:
        display_name = label

    btns.append(
        dict(
            label=display_name,
            method="restyle",
            args=[{
                "x": [df_step["role"]],
                "y": [df_step["avg_salary"]],
                "text": [df_step["job_count"].astype(str) + " jobs"]
            }]
        )
    )

# Clean up layout
fig.update_traces(textposition="outside")
fig.update_xaxes(tickangle=45, title="Job Role")
fig.update_yaxes(title="Avg Salary ($SGD)")

fig.update_layout(
    title={
        "text": "Average Salary by Role ($SGD)",
        "y": 0.97,
        "x": 0.5,
        "xanchor": "center",
        "yanchor": "top"
    },
    updatemenus=[
        dict(
            type="buttons",
            direction="right",
            active=0,
            x=0.5,
            y=1.2,
            xanchor="center",
            yanchor="top",
            buttons=btns
        )
    ],
    margin=dict(t=150)
)

fig.show()