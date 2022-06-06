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

"""set up dataframe"""
# turn said dictionary into a dataframe
super_df = pd.DataFrame(super_data)
# set index to the column name
super_df.set_index(keys="name", drop=False, inplace=True)



"""create our Flask app"""
app = Flask(__name__)
app.config["db"] = super_df


@app.route('/see_stats')
def stats():
    """GET the superhero stats"""
    # call on the dataframe
    global super_df
    # set arguements for the name, superpower, and weakness
    name = request.args.get('name', default='Bri')
    superpower = request.args.get("superpower", default="Slay all day")
    weakness = request.args.get("weakness", default="come closer and find out")
    # narrow down the results by name
    if name is not 'Bri':
        result_df = super_df.loc[super_df["name"] == name]
        json_data = request.json
        result_df["input_data"] = json_data
    elif superpower is not "Slay all day":
        # narrow down the results by superpower
        result_df = super_df.loc[super_df['superpower'] == superpower]
    elif weakness is not "come closer and find out":
        # narrow down the results by superpower
        result_df = super_df.loc[super_df['weakness'] == weakness]
    else:
        # returns the entire dataframe
        result_df = super_df
        return "We have no knowledge of that in witch you speak"
    # create the response json
    resp_json = {"query_name": name,
                 "result": result_df.to_dict(orient="records")}
    # set the response headers
    resp_headers = {"content-type": "application/json"}
    return resp_json, 200, resp_headers


if __name__ == '__main__':
    """Be sure to include the following snippet at the bottom of your main.py file, and test the app runs on localhost port 5050 when you run the file"""
    app.run('0.0.0.0', 5050)
