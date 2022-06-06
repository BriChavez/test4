import pandas as pd
from flask import Flask, request

# create a dictionary containing superhero information
super_data = [{"name": "Rogue", "superpower": "Can absorb others powers",
                "weakness": "fragility of her broken mind"},
            {"name": "Psylocke", "superpower": " telepathy, psychic knife",
                "weakness": "abilities are tied to emotions"},
            {"name": "Kitty Pryde", "superpower": "Phasing",
                "weakness": "inability to control powers"}]


"""set up dataframe"""
# turn said dictionary into a dataframe
super_df = pd.DataFrame(super_data)
# set index to the column name
super_df.set_index(keys="name", drop=False, inplace=True)


"""create our Flask app"""
app = Flask(__name__)
app.config["db"] = super_df


@app.route('/see_stats', methods = ["GET"])
def stats():
    """GET the superhero stats"""
    # call on the dataframe
    global super_df
    # set arguements for the name, superpower, and weakness
    name = request.args.get('name', default='Bri')
    superpower = request.args.get("superpower", default="Slay all day")
    weakness = request.args.get("weakness", default="come closer and find out")
    if name is not 'Bri':
        # narrow down the results by name
        result_df = super_df.loc[super_df["name"] == name]
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
    resp_json = {"result": result_df.to_dict(orient="records")}
    # set the response headers
    resp_headers = {"content-type": "application/json"}
    return resp_json, 200, resp_headers


@app.route("/add_stats", methods=['POST'])
def add_stats():
    """function to add heros to the database"""
    # get db from flask cache
    global super_df
    try:
        # placeholders to return results later
        new_recruits = []
        hard_pass = []
        # iterate through the request
        data = request.json
        for hero in data:
            # double check to see if alll required information is present
            if ('name' in hero) and ('superpower' in hero) and ('weakness' in hero):
                # sets the index for the new hero to their name
                index = hero['name']
                # give them their own row
                super_df.loc[hero['name']] = hero
                # add them to the roster
                new_recruits.append(hero)
            else:
                # sorry, next time come back with full data
                hard_pass.append(hero)
        # generate the response
        resp_json = {"heroes_added": len(new_recruits),
                     "new_team": new_recruits,
                     "reject_pile": hard_pass}
        # response headers
        resp_headers = {"content-type": "application/json"}
        # return status as ok
        return resp_json, 200, resp_headers

    except Exception as err:
        # return error. does not compute
        return {"status": 'error',
                "error_msg": str(err)}, 400, {
                "content-type": 'application/json'}


if __name__ == '__main__':
    """Be sure to include the following snippet at the bottom of your main.py file, and test the app runs on localhost port 5050 when you run the file"""
    app.run('0.0.0.0', 5050)
