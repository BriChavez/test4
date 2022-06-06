import pandas as pd
from datetime import datetime as dt
from flask import Flask

# create a dictionary containing superhero information
super_data = [
    {"name": "Rogue", "superpower": "Can absorb others powers",
        "weakness": "fragility of her broken mind"},
    {"name": "Psylocke", "superpower": " telepathy, psychic knife",
        "weakness": "abilities are tied to emotions"},
    {"name": "Kitty Pryde", "superpower": "Phasing",
        "weakness": "inability to control powers"}
]


# turn said dictionary into a dataframe
super_df = pd.DataFrame(super_data)
# set index to the column name
super_df.set_index(keys="name", drop=False, inplace=True)



# Create our Flask app
app = Flask(__name__)
app.config["db"] = super_df


@app.route('/see_stats')
def stats():
    """GET the superhero stats"""
    # call on the dataframe
    global super_df
    # set arguements for the name, superpower, and weakness
    name = request.args.get('name', default= 'Bri')
    superpower = request.args.get("superpower", default= "Slay all day")
    weakness = request.args.get("weakness", default="come closer and find out")

    """write a function that allows a user to query by name, superpower, and / or weakness."""
