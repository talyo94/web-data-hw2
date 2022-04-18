from queue import Queue
import queue
import sys
from typing import Any, Callable
import requests
import lxml.html
import re

DOMAIN = "http://example.org/"


def get_first_num(s: str):
    # some numbers are in the format: xxx.xxx.xxx
    # like indonesia population
    # so we check this pattern first to get a match.
    res = re.findall(r"\d+\.\d\d\d[\.\d\d\d]*", s)
    if res:
        s = s.replace(".", "")
    res = re.findall(r"\d+", s.replace(",", ""))
    if res:
        return int(res[0])
    return None


def extract_label_from_infobox(table, label: str):
    text = table.xpath(f"./tbody/tr[th//text()='{label}']/td//text()")
    return list(filter(lambda x: x and x[0].isalpha(), text))


def extract_merged_label_from_infobox(table, label: str):
    """Merged labels are labels like populations etc.,
    Where the box have a title, matching label, and subvalues
    """
    text = table.xpath(
        f"./tbody/tr[th//text()='{label}']/th/../following-sibling::tr[1]/td//text()")
    return filter(lambda x: x and x != "\n", text)


def create_wiki_url(name: str):
    """Formats a wiki url from entity name

    Args:
        name (str): Any name, e.g. Emmanuel Macron

    Returns:
        str: formatted wikipedia url
    """
    name = name.replace(" ", "_")
    return f"https://en.wikipedia.org/wiki/{name}"


class Crawler:
    start_url = "https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)"

    def __init__(self) -> None:
        self.queue = Queue()
        self.visited = set()

    def run(self):
        res = self.download_page(self.start_url)
        self.start_parser(res)

        while not self.queue.empty():
            task = self.queue.get()
            page = self.download_page(task['url'])
            task['handler'](page, task.get("meta"))

    def download_page(self, url: str):
        """download a url and returned the parsed lxml.html string

        If request is unsuccessful, will return None

        Args:
            url (str): url to download

        Returns:
            Any: formatted html document from the request content
        """
        res = requests.get(url)
        if 200 <= res.status_code < 300:
            self.visited.add(url)
        else:
            return None
        doc = lxml.html.fromstring(res.content)
        return doc

    def enqueue_page(self, url: str, handler: Callable, meta: Any = None):
        """Adds a page to the queue, with handler function
        A page will be queue if url is not in visited

        note::

            url is visited if it was **queued**

        Args:
            url (str): _description_
            handler (Callable): _description_
        """
        if url in self.visited:
            print("Already visited", url)
            return
        self.visited.add(url)
        self.queue.put({"url": url, "handler": handler, "meta": meta})

    def start_parser(self, page, meta=None):
        """Parser for first page (countries list)"""
        table = page.xpath('//table[contains(@class, "wikitable")]')[0]
        for a in table.xpath("//tr/td[1]/span/a"):
            name = a.xpath("@title")[0]
            href = a.xpath("@href")[0]
            self.enqueue_page(
                create_wiki_url(name),
                self.parse_state,
                {"name": name, "href": href}
            )

    def parse_state(self, page, meta=None):
        """Parser for country page"""
        print(meta)
        infobox = page.xpath("//table[contains(@class, 'infobox')]")[0]
        data = {
            "country": meta['name'],
            "capital": next(iter(extract_label_from_infobox(infobox, "Capital")), None),
            # "largest_city": next(iter(extract_label_from_infobox(infobox, "Largest city")), None),
            "government": extract_label_from_infobox(infobox, "Government"),
            "area": next(iter(extract_merged_label_from_infobox(infobox, "Area ")), None),
            "population": next(iter(extract_merged_label_from_infobox(infobox, "Population")), None),
        }
        if data["area"]:
            data["area"] = get_first_num(data["area"])
        if data["population"]:
            data["population"] = get_first_num(data["population"])


    def parse_person(self, page, meta=None):
        """Parser for person (president/PM)"""


def create():
    c = Crawler()
    c.run()


def qna():
    pass


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Invalid usage: python3 geo_qa (create|question) [params]")
        exit(1)
    if sys.argv[1] == "create":
        create()
    elif sys.argv[1] == "question":
        qna()
    else:
        print("Unknown command", sys.argv[1])
        exit(1)
