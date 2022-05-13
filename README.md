# Return to the air
Now that we've learned how to interact with SQL databases, let's return to the airline data we saw in Chapter 1. In this chapter, we will build our first data pipelines, both locally and on the cloud. The first step in any of these pipelines is to understand the data you will be ingesting. In this course, we will be building the data infrastructure for an airline company, **Deb-on-air**. In Chapter 1, we saw some of this data but let's review the four main data sources. These CSVs can be found in the `../data/` directory. The flight data is large so must be downloaded using the provided `get_flights.sh` script.

- **Airports**: This contains a list of airports locations, names, etc.
- **Routes**: These are the airport to airport routes flown by the airlines. Each has a source and destination along with the airline.
- **Airlines**: Information about each airline operating in the airspace.
- **Aircraft**: Information about the individual planes including model information and airline affiliation.
- **Flights**: The actual flights from airport to airport that individual aircraft flew for their airlines along the routes.

#### Pandas Refresher Exercise
Load the data from the CSV files into Pandas DataFrames, and use profiling tools we learned in Chapter 1 to see the head(), shape(), and info() of the data. Once you've done this, we're ready to build a data loading class we can use within our data engineering pipeline.


## Load the data!
For this pipeline, we're going to make a Python class called `DataLoader`. This class will have the following properties:
- Initialize by loading specified CSV(s) into a Pandas DataFrame
- Create a unique index for the DataFrame
- Join other DataFrame(s) to the loaded data.


To start, we need to import our python libraries as well as initialize a logger:

```python
import pandas as pd
from glob import glob
import logging
import sys
import warnings
from logging import INFO


# setup logging and logger
logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=INFO,
                    stream=sys.stderr)
logger: logging.Logger = logging
```

The logger is similar to the `print` statements we have been using but the `basicConfig` line above sets a more verbose default output that also tells us the timestamp and where in the code the logging took place. Another advantage is that logged messages only show up in the terminal and don't affect the output of the code, so you don't have to delete them all before deploying like you would with print statements. It is good practice to add logging to your Python scripts to both track their output and assist in debugging.


>Check out [Real Python](https://realpython.com/python-logging/)'s tutorial on logging for a good introduction.

#### Exercise
Run the `main.py` file in your terminal to see the logged messages.


With this setup in place, we're ready to create our class. The first step will be to define the initialization function for this class. Let's initialize our class with a filepath to the CSV(s) containing our data. We can also specify which column will be our index and whether to expect a single CSV or multiple CSVs. Our function will use the filepath to load the data into a Pandas DataFrame and set the index if provided:

```python
class DataLoader():
    def __init__(self, filepath, index=None, multi_file_load=False) -> None:
        # If we want to load multiple files
        if multi_file_load:
            store = []
            for filenum, filename in enumerate(glob(filepath)):
                logger.info(f"\tLoading file number {filenum}")
                tmp = pd.read_csv(filename, header=0)
                store.append(tmp)
            keep_index = index is None
            df = pd.concat(store, axis=0, ignore_index=keep_index)
        # Load a single file using read_csv
        else:
            df = pd.read_csv(filepath, header=0)
        # Set index of our data if specified.
        if index is not None:
            df =df.set_index(index)
        self.df = df
```

And there we have our class with `df` attribute containing our data. Let's add a quick util to expose the `info()` method of our data frame on our base class. Add the following class function:

```python
    def info(self):
        """
        bind info() to the info of our DataFrame
        """
        return self.df.info()
```

## Add index
If our data doesn't already have an index column, let's create a function that can create an index from a concatenation of other columns. This function will take a list of the column names to concatenate as well as an optional name for this index:

```python
    def add_index(self, col_list, index_name="index") -> None:
        """
        Create an index column by concatenating a list of columns into a str
        """
        df = self.df
        logger.info(f"\tAdding index {index_name}")
        # Concatenate the columns in col_list into an index column to serve as our primary key
        df[index_name] = df[col_list].apply(lambda row: "-".join(row.values.astype(str)), axis=1)
        df.set_index(index_name, inplace=True)
        self.df = df
```

And that gives us the ability to add indices when one is not already present!

## Joining util
Let's add one more util that will join another DataFrame to our loaded data. This exposes the merge functionality of our loaded data into a convenient function. This function will take a DataFrame, the left and right column names to join on, the columns of the new DataFrame to join, and optionally the method for the join:

```python
def join_column(self, dataframe, left_on, right_on, join_cols, how='left'):
        """
        join columns specified by the join_cols list from dataframe into self.df
        """
        self.df = pd.merge(left=self.df, right=dataframe[join_cols], left_on=left_on, right_on=right_on, how=how)

```

# Subclasses

We can use the ability to subclass our new class to create a loader for each of our data sources with the default options pre-set. We simply base our subclass off `DataLoader` and within its `__init__` method, simply call the `super().__init__()` with the desired default options. This call invokes the parent's initialization without having to set the index columns each time, etc:

```python
class AirportLoader(DataLoader):
    def __init__(self, filepath, index='iata', multi_file_load=False) -> None:
        super().__init__(filepath, index=index, multi_file_load=multi_file_load)


class RouteLoader(DataLoader):
    def __init__(self, filepath, index=None, multi_file_load=False) -> None:
        super().__init__(filepath, index=index, multi_file_load=multi_file_load)


class AirlineLoader(DataLoader):
    def __init__(self, filepath, index='airline_id', multi_file_load=False) -> None:
        super().__init__(filepath, index=index, multi_file_load=multi_file_load)


class AircraftLoader(DataLoader):
    def __init__(self, filepath, index='n_number', multi_file_load=False) -> None:
        super().__init__(filepath, index=index, multi_file_load=multi_file_load)
        self.df['mfr_year'] = pd.to_datetime(self.df['mfr_year'], format="%Y", errors='coerce')


class FlightLoader(DataLoader):
    def __init__(self, filepath, index=None, multi_file_load=False) -> None:
        super().__init__(filepath, index=index, multi_file_load=multi_file_load)
        self.df['flight_date'] = pd.to_datetime(self.df['flight_date'], format="%Y-%m-%d")
```


This also allows us to add a few more class specific functions. For example, within `FLightLoader`, let's make a couple data checking utils. First, let's make a function that creates a Boolean flag that is **True** if we have flights taking routes not covered in our route data:

```python
    def flown_routes(self, route_data) -> None:
        """
        Check src-dest pairs representing routes flown against our route data
        """
        # Get our flight and route data
        df = self.df
        route_df = route_data.df.copy()
        # Set True flag on our routes
        route_df['flag'] = True
        # Group flight data by src, dest, and airline, and take the first one
        col_list = ['src', 'dest', 'airline']
        grouped_df = df.groupby(col_list).first()
        # Join the routes onto these flown routes
        grouped_df = pd.merge(left=grouped_df, right=route_df, on=col_list, how='left')
        # Drop the routes that are covered and set our flag:
        grouped_df = grouped_df[grouped_df.flag != True]
        if grouped_df.size:
            warnings.warn(f"There are {grouped_df.shape[0]} flown routes that are not covered by our route data", Warning)
            self.unknown_routes = True
        else:
            self.unknown_routes = False
```

Similarly, we can add a check to see if we have airport information for each flown flight:

```python
    def flight_check(self, airport_data) -> None:
        """
        Check flight data to see if the src and dest are in our airport file
        """
        # Find flights who's src is not in our airport's iata code index
        missing_src = self.df[~self.df.src.isin(airport_data.df.index)]['src'].unique()
        if len(missing_src):
            warnings.warn(f"The following src airports are not in our data: {', '.join(missing_src)}", Warning)
            self.missing_src = missing_src
        else:
            self.missing_src = None
        # Do the same for dest
        missing_dest =self.df[~self.df.dest.isin(airport_data.df.index)]['dest'].unique()
        if len(missing_dest):
            warnings.warn(f"The following src airports are not in our data: {', '.join(missing_dest)}", Warning)
            self.missing_dest = missing_dest
        else:
            self.missing_dest = None
```

Now that we have our loading utils created, let's use them to make the first step of a data pipeline.

# Start pipeline
For this episode, we will start our pipeline by using all our `DataLoaders` and checking their output. We will make a new file, called `main.py` which will import our loading functions, use them to load our data, then log the output. This checks that we are loading what we expect and next episode we can *ingest* our loaded data. Our simple `main.py` will look like:

```python
import os
import logging
import sys
from logging import INFO
from dataloaders import AirportLoader, RouteLoader, AirlineLoader, AircraftLoader, FlightLoader


# setup logging and logger
logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=INFO,
                    stream=sys.stderr)
logger: logging.Logger = logging


def run():
    data_dir = "../data/"
    airport_data = AirportLoader(os.path.join(data_dir, "deb-airports.csv"))
    route_data = RouteLoader(os.path.join(data_dir, "deb-routes.csv"))
    airline_data = AirlineLoader(os.path.join(data_dir,  "deb-airlines.csv"))
    aircraft_data = AircraftLoader(os.path.join(data_dir, "deb-aircrafts.csv"))
    flight_data = FlightLoader(os.path.join(data_dir, "flights/2018/*0[1-2].csv"), multi_file_load=True)

        
    logger.info("Airports")
    logger.info(airport_data.info())
    logger.info("\nRoutes")
    logger.info(route_data.info())
    logger.info("\nAirlines")
    logger.info(airline_data.info())
    logger.info("\nAircraft")
    logger.info(aircraft_data.info())
    logger.info("\nFlights")
    logger.info(flight_data.info())
    
    # Check to see if we have flights from unknown airports
    flight_data.flight_check(airport_data)
    # Check to see if any flights we have do not appear in the routes data    
    flight_data.flown_routes(route_data)
    # let's join the aircraft model to each flight!
    flight_data.join_column(aircraft_data.df, left_on='tailnumber', right_on='n_number', join_cols=['model'])
    logger.info('\nUpdated flights')
    logger.info(flight_data.info())
    logger.info(flight_data.df.head())


if __name__=="__main__":
    run()
```