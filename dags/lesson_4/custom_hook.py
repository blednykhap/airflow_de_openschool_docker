# import the hook to inherit from
from airflow.hooks.base import BaseHook
from bs4 import BeautifulSoup
import requests


# define the class inheriting from an existing hook class
class ScraperHook(BaseHook):
    """
    Interact with external websites.
    :param url: URL to call
    """

    # provide the name of the hook
    hook_name = "ScraperHook"

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(self, url: str, parser: str = "html.parser", *args, **kwargs) -> None:
        # initialize the parent hook
        super().__init__(*args, **kwargs)
        # assign class variables
        self.url = url
        self.parser = parser

    def get_website(self):
        """Function that returns content of website."""

        r = requests.get(self.url)
        if r.status_code == 200:
            soup: BeautifulSoup = BeautifulSoup(r.content, self.parser)
            return soup
        else:
            return r.status_code