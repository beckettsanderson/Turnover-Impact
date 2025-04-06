import streamlit as st
import pandas as pd
import plotly.express as px
import os

# Paths to data folders
PLAYER_STATS_DIR = "data/player_stats"
TEAM_STATS_DIR = "data/team_stats"

# Abbreviated column names to full explanation
COLUMN_MAP = {
    "team": "Team Abbreviation",
    "player": "Player Name",
    "GP": "Games Played",
    "TOs": "Total Turnovers",
    "TOI": "Turnover Impact",
    "TOUCHES": "Total Touches",
    "TIME_OF_POSS": "Total Time of Possession (Minutes)",
    "TOI_POSS": "Turnover Impact per Possession",
    "TOI_10_TOUCHES": "Turnover Impact per 10 Touches",
    "TO_POSS": "Turnovers per Possession",
    "TO_10_TOUCHES": "Turnovers per 10 Touches"
}

# Load data
@st.cache_data
def load_stats(year: str, stat_type: str):
    path = os.path.join(
        PLAYER_STATS_DIR if stat_type == "Player" else TEAM_STATS_DIR,
        f"{'player' if stat_type == 'Player' else 'team'}_stats_{year}.csv"
    )
    df = pd.read_csv(path)
    return df

# Page layout
st.set_page_config(page_title="Turnover Impact Analysis", layout="wide")

# --- Header ---
st.title("Turnover Impact Analysis")
st.markdown("### by name and name")

# --- Sidebar Filters ---
stat_type = st.sidebar.selectbox("View Stats By", ["Player", "Team"])

years = [f"{y}_{str(y+1)[-2:]}" for y in range(2013, 2025)]
selected_year = st.sidebar.selectbox("Select Season", years[::-1])

df = load_stats(selected_year, stat_type)

# Drop irrelevant columns
columns_to_drop = ['Unnamed: 0', 'player_id', 'PLAYER_ID']
df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

# Sort by TOI descending
df = df.sort_values(by="TOI", ascending=False)

# Team + game filters (only for player stats)
if stat_type == "Player":
    teams = sorted(df['team'].unique())
    team_options = ['All'] + teams
    selected_team = st.sidebar.selectbox("Select Team", team_options)

    if selected_team != "All":
        df = df[df["team"] == selected_team]

    min_gp = st.sidebar.slider("Minimum Games Played", 
                               min_value=int(df["GP"].min()), 
                               max_value=int(df["GP"].max()), 
                               value=10)
    df = df[df["GP"] >= min_gp]

# Y-axis and corresponding X-axis logic
y_axis_options = {
    "Turnover Impact (TOI)": "TOI",
    "TOI per Possession": "TOI_POSS",
    "TOI per 10 Touches": "TOI_10_TOUCHES"
}
x_axis_map = {
    "TOI": "TOs",
    "TOI_POSS": "TO_POSS",
    "TOI_10_TOUCHES": "TO_10_TOUCHES"
}
selected_y_label = st.sidebar.radio("Y-axis for Scatter Plot", list(y_axis_options.keys()))
selected_y = y_axis_options[selected_y_label]
selected_x = x_axis_map[selected_y]

# --- Main Table ---
st.subheader(f"{stat_type} Stats – {selected_year}")
display_cols = [col for col in COLUMN_MAP if col in df.columns]
st.dataframe(df[display_cols], use_container_width=True)

# --- Scatter Plot ---
if selected_x in df.columns and selected_y in df.columns:
    st.subheader(f"{selected_x} vs {selected_y}")
    hover = "player" if "player" in df.columns else "team"
    fig = px.scatter(
        df,
        x=selected_x,
        y=selected_y,
        hover_name=hover,
        labels={
            selected_x: selected_x,
            selected_y: selected_y
        },
        title=f"{selected_x} vs {selected_y}"
    )
    st.plotly_chart(fig, use_container_width=True)

# --- Glossary ---
with st.expander("ℹ️ Glossary of Stats"):
    for short, full in COLUMN_MAP.items():
        st.markdown(f"**{short}** – {full}")
