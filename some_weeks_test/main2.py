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

class DataLoader():
    def __init__(self, filepath, index = None, multi_file_load = False) -> None:
        df = pd.read_csv(filepath, header = 0)
    # load a file into a csv
    # set index if specified
        if index is None:
            df = pd.set_index(index)
        self.df = df

    def head(self) -> None:
        """prints the head of the dataframe to console"""
        print(self.df.head())

    # utility to expose the info method of our data frame onto our base class
    def info(self):
        """bind info to our dataframe"""
        return self.df.info

    # def add_index(self, index_name:str, colum_names:list) -> None:

        # if our data doesn't already have one, this is a function that creates an index from a concat of columns as a list as well is an optional name for index
    def add_index(self, col_list, index_name="index") -> None:
        """create an index column by concating s list of columns into a string"""
        df = self.df
        # summon our logger buddy so he can later let us know when we make an index and what we named him
        logger.info(f"\tAdding inex {index_name}")
        # concats cols in col_list into an index column to serve us faithfully as our loyal primary key
        df[index_name] = df[col_list].apply(
            lambda row: "-".join(row.values.astype(str)), axis=1)
        df.set_index(index_name, inplace=True)
        self.df = df
        # this gives us the ability to add an index when one is not present


    def sort(self, column_name:str) -> None:
        """
        Sorts the dataframe by a particular column

        Args:
            column_name (str): column name to sort by
        """

    def load_to_db(self, db_engine, db_table_name:str) -> None:
        """
        Loads the dataframe into a database table.

        Args:
            db_engine (SqlAlchemy Engine): SqlAlchemy engine (or connection) to use to insert into database
            db_table_name (str): name of database table to insert to
        """



def db_engine(db_host:str, db_user:str, db_pass:str, db_name:str="spotify") -> sa.engine.Engine:
    """Using SqlAlchemy, create a database engine and return it

    Args:
        db_host (str): datbase host and port settings
        db_user (str): database user
        db_pass (str): database password
        db_name (str): database name, defaults to "spotify"

    Returns:
        sa.engine.Engine: sqlalchemy engine
    """
    pass


def db_create_tables(db_engine, drop_first:bool = False) -> None:
    """
    Using SqlAlchemy Metadata class create two spotify tables (including their schema columns and types)
    for **artists** and **albums**.


    Args:
        db_engine (SqlAlchemy Engine): SqlAlchemy engine to bind the metadata to.
        drop_first (bool): Drop the tables before creating them again first. Default to False
    """
    meta = sa.MetaData(bind=db_engine)

    # your code to define tables go in here
    #   - Be careful, some of the columns like album.available_markets are very long. Make sure you give enough DB length for these. ie: 10240 (10kb)

    # your code to drop and create tables go here


def main():
    """
    Pipeline Orchestratation method.

    Performs the following:
    - Creates a DataLoader instance for artists and albums
    - prints the head for both instances
    - Sets artists index to id column
    - Sets albums index to artist_id, name, and release_date
    - Sorts artists by name
    - creates database engine
    - creates database metadata tables/columns
    - loads both artists and albums into database
    """
    pass


if __name__ == '__main__':
    main()