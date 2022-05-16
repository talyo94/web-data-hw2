from typing import Union
import re
import sys
from queue import Queue
from typing import Any, Callable
from webbrowser import BackgroundBrowser

import lxml.html
import rdflib
import requests
from rdflib import Literal, URIRef
from rdflib.namespace import DCTERMS, FOAF, RDF, SDO, XSD

# GLOBALS AND CONSTANTS

DOMAIN = "http://example.org/"
global list_of_countries, pre
list_of_countries = set()
DBPEDIA_BASE = "https://dbpedia.org/page/"

# Graph Relations
g = rdflib.Graph()
president_of = URIRef('https://dbpedia.org/ontology/president')
prime_minister_of = URIRef('https://dbpedia.org/ontology/PrimeMinister')
capital_of = URIRef('https://dbpedia.org/ontology/capital')
type_government_of = URIRef('https://dbpedia.org/ontology/governmentType')
area_of = URIRef('https://dbpedia.org/ontology/PopulatedPlace/area')
population_of = URIRef('https://dbpedia.org/property/populationCensus')
vp_of = URIRef('https://dbpedia.org/ontology/VicePresident')
pm_of = URIRef('https://dbpedia.org/ontology/PrimeMinister')
birth_day = URIRef('https://dbpedia.org/ontology/birthDate')
has_the_role_of = URIRef('https://dbpedia.org/ontology/role')
birth_place = URIRef('https://dbpedia.org/ontology/birthPlace')


def format_name_to_ont(string: str) -> str:
    return string.replace(" ", "_")


def format_name_from_ont(string: str) -> str:
    return string.rpartition("/")[-1].replace("_", " ")


def add_to_graph(a: Union[str, URIRef], b: Union[str, URIRef], c: Union[str, URIRef]):
    if not isinstance(a, URIRef):
        a = URIRef(DBPEDIA_BASE + format_name_to_ont(a))
    if not isinstance(b, URIRef):
        b = URIRef(DBPEDIA_BASE + format_name_to_ont(b))
    if not isinstance(c, URIRef):
        c = URIRef(DBPEDIA_BASE + format_name_to_ont(c))
    g.add((a, b, c))


def get_first_num(s: str):
    # some numbers are in the format: xxx.xxx.xxx
    # like indonesia population
    # so we check this pattern first to get a match.
    match = re.search("\d", s)
    if match is None:
        return None
    s = s[match.start():]

    res = re.search(r"^\d{1,3}((,\d{3})*)(\.\d+)?", s)
    if res is not None:
        n = float(res.group().replace(",", ""))
        return int(n) if n.is_integer() else n

    # For those using . as ,
    res = re.search(r"^\d{1,3}((\.\d{3})*)(,\d+)?", s)

    if res is not None:
        n = float(res.group().replace(".", "").replace(",", "."))
        return int(n) if n.is_integer() else n
    res = re.findall(r"\d+", s.replace(",", ""))
    if res:
        return int(res[0])
    return None


def extract_label_from_infobox(table, label: str):
    """Get a specific label from infobox"""
    text = table.xpath(f"./tbody/tr[th//text()='{label}']/td//text()")
    return list(filter(lambda x: x and x[0].isalpha(), text))


def extract_government_type_from_infobox(table):
    href = table.xpath(f"./tbody/tr[th//text()='Government']/td//a/@href")
    return list(map(lambda x: x.rpartition("/")[-1], filter(lambda x: x and x.startswith("/wiki"), href)))


def extract_link_from_infobox(table, label: str):
    """Extract next wiki link"""
    href = table.xpath(f"./tbody/tr[th//text()='{label}']/td//a/@href")
    link = next(filter(lambda x: x.startswith("/wiki"), href), None)
    if link is None:
        return None

    return create_wiki_url(link.replace("/wiki/", ""))


def extract_merged_label_from_infobox(table, label: str, contains=False):
    """Merged labels are labels like populations etc.,
    Where the box have a title, matching label, and subvalues
    """

    if contains:
        predicate = f"contains(th//text(), '{label}')"
    else:
        predicate = f"th//text()='{label}'"
    text = table.xpath(
        f"./tbody/tr[{predicate}]/th/../following-sibling::tr[1]/td//text()")
    return filter(lambda x: x and x not in ("\n", " "), text)


def create_wiki_url(name: str):
    """Formats a wiki url from entity name

    Args:
        name (str): Any name, e.g. Emmanuel Macron

    Returns:
        str: formatted wikipedia url
    """
    name = name.replace(" ", "_")
    return f"https://en.wikipedia.org/wiki/{name}"


def check_born_country(infobox):
    born = infobox.xpath(f"./tbody/tr[th//text()='Born']/td")
    if not born:
        return None
    # Check if a known country
    links = born[0].xpath(".//a/@href")
    for link in links:
        c = link.rpartition("/")[-1]
        if c in list_of_countries:
            return c
    text = born[0].xpath(".//text()")
    for c in text:
        if not re.search(r"[a-zA-Z]", c):
            continue
        c = format_name_to_ont(c.replace(r",", "").strip())
        if c in list_of_countries:
            return c


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
            try:
                page = self.download_page(task['url'])
                task['handler'](page, task.get("meta"))
            except Exception as e:
                print("Error running task:", task["url"])
                print(e)
        print(f"Visited {len(self.visited)} pages")

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

        # return
        table = page.xpath('//table[contains(@class, "wikitable")]')[0]
        for a in table.xpath("//tr/td[1]/span/a"):
            name = a.xpath("@title")[0]
            href = a.xpath("@href")[0]
            self.enqueue_page(
                create_wiki_url(name),
                self.parse_state,
                {"name": name, "href": href}
            )
            list_of_countries.add(href.rpartition("/")[-1])
            # if name == "China":
            #     break
        # self.enqueue_page(
        #     "https://en.wikipedia.org/wiki/Manasseh_Sogavare",
        #     self.parse_person,
        #     {"name": "Dominican_Republic",
        #         "href": "https://en.wikipedia.org/wiki/Manasseh_Sogavare"}
        # )

    def parse_state(self, page, meta=None):
        """Parser for country page"""
        # print("Parsing", meta)
        infobox = page.xpath("//table[contains(@class, 'infobox')]")[0]
        data = {
            "country": meta['name'],
            "capital":  extract_link_from_infobox(infobox, "Capital"),
            # "largest_city": next(iter(extract_label_from_infobox(infobox, "Largest city")), None),
            "government": extract_government_type_from_infobox(infobox),
            "area": next(iter(extract_merged_label_from_infobox(infobox, "Area", True)), None),
            "population": next(iter(extract_merged_label_from_infobox(infobox, "Population")), None),
            "president": next(iter(extract_label_from_infobox(infobox, "President")), None),
            "vp": next(iter(extract_label_from_infobox(infobox, "Vice President")), None),
            "pm": next(iter(extract_label_from_infobox(infobox, "Prime Minister")), None),
            # Different name for prime minister
            "premier": next(iter(extract_label_from_infobox(infobox, "Premier")), None),
        }
        if data["capital"]:
            data["capital"] = data["capital"].rpartition("/")[-1]
        else:
            print("NO CAPITAL FOR: ", meta)
        if data["area"]:
            data["area"] = get_first_num(data["area"])
        if data["population"]:
            data["population"] = int(get_first_num(data["population"]))

        country_name = format_name_to_ont(data["country"])
        country = rdflib.URIRef(DBPEDIA_BASE + country_name)
        list_of_countries.add(country_name)

        if data["president"]:
            self.enqueue_page(
                extract_link_from_infobox(
                    infobox, "President") or create_wiki_url(data["president"]),
                handler=self.parse_person,
                meta={
                    "role": "president",
                    "name": data["president"], "country": meta["name"]
                }
            )
            add_to_graph(data["president"], president_of, country)

        if data["pm"]:
            self.enqueue_page(
                extract_link_from_infobox(
                    infobox, "Prime Minister") or create_wiki_url(data["pm"]),
                handler=self.parse_person,
                meta={"role": "pm",
                      "name": data["pm"], "country": meta["name"]}
            )
            add_to_graph(data["pm"], prime_minister_of, country)

        if data["premier"]:
            self.enqueue_page(
                extract_link_from_infobox(
                    infobox, "Primier") or create_wiki_url(data["premier"]),
                handler=self.parse_person,
                meta={"role": "pm",
                      "name": data["premier"], "country": meta["name"]}
            )
            add_to_graph(data["premier"], prime_minister_of, country)

        population = data["population"]
        area = data["area"]
        vp = data["vp"]
        capital = Literal(data["capital"])
        government = data["government"]
        if population not in (None, "None"):
            population = Literal(data["population"])
            g.add((country, population_of, population))
        if area not in (None, "None"):
            area = Literal(data["area"])
            g.add((country, area_of, area))
        if vp not in (None, "None"):
            vp = Literal(data["vp"])
            g.add((country, vp_of, vp))
        if capital not in (None, "None"):
            g.add((country, capital_of, capital))
        if government not in (None, "None"):
            for i in government:
                gov = format_name_to_ont(i)
                gov = rdflib.URIRef(DBPEDIA_BASE + gov)
                g.add((country, type_government_of, gov))

    def parse_person(self, page, meta=None):
        """Parser for person (president/PM)"""
        # print("Parsing", meta)
        infobox = page.xpath("//table[contains(@class, 'infobox')]")[0]

        try:
            bcountry = check_born_country(infobox)
        except:
            bcountry = None

        if bcountry is None:
            print("---- No birth country for:", meta)
        bday = next(iter(infobox.xpath("//span[@class='bday']/text()")), None)

        data = {
            "name": meta["name"],
            "role": meta["role"],
            "bday": bday,
            "bcountry": bcountry,
        }
        president_name = format_name_to_ont(data["name"])
        president_name = format_name_to_ont(president_name)
        president = rdflib.URIRef(DBPEDIA_BASE + president_name)
        role = data["role"]
        if role not in (None, "None"):
            if (data["role"] == "president"):  # redundant
                g.add((president, has_the_role_of, president_of))
            else:
                g.add((president, has_the_role_of, prime_minister_of))
        if bday not in (None, "None"):
            bday = Literal(data["bday"], datatype=XSD.date)
            g.add((president, birth_day, bday))
        if bcountry not in (None, "None"):
            bcountry = rdflib.URIRef(
                DBPEDIA_BASE + format_name_to_ont(data["bcountry"]))
            g.add((president, birth_place, bcountry))


def create():
    c = Crawler()
    c.run()
    g.serialize("graph.nt", format="nt")


def adjust_str(s):
    s = s.replace(' ', '_')
    return s.capitalize()


QUESTIONS = [
    # Q1
    {
        "pattern": r"Who is the president of (?P<country>.+)\?",
    },
    # Q2
    {
        "pattern": r"Who is the prime minister of (?P<country>.+)\?"
    },
    # Q3
    {
        "pattern": r"What is the population of (?P<country>.+)\?"
    },
    # Q4
    {
        "pattern": r"What is the area of (?P<country>.+)\?"
    },
    # Q5
    {
        "pattern": r"What is the form of government in (?P<country>.+)\?"
    },
    # Q6
    {
        "pattern": r"What is the capital of (?P<country>.+)\?"
    },
    # Q7
    {
        "pattern": r"When was the president of (?P<country>.+) born\?"
    },
    # Q8
    {
        "pattern": r"Where was the president of (?P<country>.+) born\?"
    },
    # Q9
    {
        "pattern": r"When was the prime minister of (?P<country>.+) born\?"
    },
    # Q10
    {
        "pattern": r"Where was the prime minister of (?P<country>.+) born\?"
    },
    # Q11
    {
        "pattern": r"Who is (?P<entity>.+)\?"
    },
    # Q12
    {
        "pattern": r"How many (?P<government_form1>.+) are also (?P<government_form2>.+)?\?"
    },
    # Q13
    {
        "pattern": r"List all countries whose capital name contains the string (?P<str>.+)"
    },
    # Q14
    {
        "pattern": r"How many presidents were born in (?P<country>.+)\?"
    },
]


def load_graph() -> rdflib.Graph:
    g = rdflib.Graph()
    g.parse("graph.nt", format="nt")
    return g


def answer(question_num: int, params: dict):
    graph = load_graph()

    # val = list(params.values())
    # for i in range(len(val)):
    #     val[i] = adjust_str(val[i])

    # country = DBPEDIA_BASE + val[0]

    if question_num == 0:  # Who is the president of
        country = format_name_to_ont(params["country"])

        q = (
            "SELECT ?x WHERE {"
            f"?x <{president_of}> <{DBPEDIA_BASE + country}> ."
            "}"
        )

    elif question_num == 1:
        country = format_name_to_ont(params["country"])

        q = (
            "SELECT ?x WHERE {"
            f"?x <{prime_minister_of}> <{DBPEDIA_BASE + country}> ."
            "}"
        )

    elif question_num == 2:
        country = format_name_to_ont(params["country"])

        q = (
            "SELECT ?x WHERE {"
            f"<{DBPEDIA_BASE + country}> <{population_of}> ?x ."
            "}"
        )

    elif question_num == 3:
        country = format_name_to_ont(params["country"])

        q = (
            "SELECT ?x WHERE {"
            f"<{DBPEDIA_BASE + country}> <{area_of}> ?x ."
            "}"
        )

    elif question_num == 4:
        country = format_name_to_ont(params["country"])

        q = (
            "SELECT ?x WHERE {"
            f"<{DBPEDIA_BASE + country}> <{type_government_of}> ?x ."
            "}"
        )

    elif question_num == 5:
        country = format_name_to_ont(params["country"])

        q = (
            "SELECT ?x WHERE {"
            f"<{DBPEDIA_BASE + country}> <{capital_of}> ?x ."
            "}"
        )

    elif question_num == 6:
        country = format_name_to_ont(params["country"])

        q = (
            "SELECT ?bd WHERE {"
            f"?president <{president_of}> <{DBPEDIA_BASE + country}> . "
            f"?president <{birth_day}> ?bd ."
            " }"
        )

    elif question_num == 7:  # Where was the president of <country> born?
        country = format_name_to_ont(params["country"])

        q = (
            "SELECT ?bd WHERE {"
            f"?president <{president_of}> <{DBPEDIA_BASE + country}> . "
            f"?president <{birth_place}> ?bd ."
            " }"
        )

    elif question_num == 8:  # When was the prime minister of <country> born?
        country = format_name_to_ont(params["country"])

        q = (
            "SELECT ?bd WHERE {"
            f"?pm <{prime_minister_of}> <{DBPEDIA_BASE + country}> . "
            f"?pm <{birth_day}> ?bd ."
            " }"
        )

    elif question_num == 9:  # Where was the prime minister of <country> born?
        country = format_name_to_ont(params["country"])

        q = (
            "SELECT ?bd WHERE {"
            f"?pm <{prime_minister_of}> <{DBPEDIA_BASE + country}> . "
            f"?pm <{birth_place}> ?bd ."
            " }"
        )

    elif question_num == 10:
        entity = DBPEDIA_BASE + format_name_to_ont(params["entity"])
        q = (
            "SELECT ?role ?country WHERE {"
            f"<{entity}> <{has_the_role_of}> ?role . "
            f"<{entity}> ?role ?country"
            " }"
        )

    elif question_num == 11:
        gf1 = format_name_to_ont(params["government_form1"])
        gf2 = format_name_to_ont(params["government_form2"])

        q = (
            "SELECT ?y WHERE {"
            f"?y <{type_government_of}> <{DBPEDIA_BASE + gf1}> . "
            f"?y <{type_government_of}> <{DBPEDIA_BASE + gf2}> ."
            " }"
        )

    elif question_num == 12:
        q = (
            "SELECT ?country WHERE {"
            f"?country <{capital_of}> ?capital ; "
            f"filter contains(lcase(?capital), '{params['str'].lower()}') "
            " }"
        )

    elif question_num == 13:
        country = format_name_to_ont(params["country"])
        q = (
            "SELECT ?president WHERE {"
            f"?president <{has_the_role_of}> <{president_of}> . "
            f"?president <{birth_place}> <{DBPEDIA_BASE + country}>"
            " }"
        )

    ent = list(graph.query(q))
    if question_num in (0, 1, 5, 6, 7, 8, 9):
        ans = str(format_name_from_ont(ent[0][0]))
    elif question_num in (2,):
        ans = "{:,}".format(ent[0][0].toPython())
    elif question_num in (3,):
        ans = "{:,} km squared".format(ent[0][0].toPython())
    elif question_num in (4, 12):
        x = [format_name_from_ont(x[0]) for x in ent]
        x.sort()
        ans = ", ".join(x)
    elif question_num in (10,):
        x = [
            f"{format_name_from_ont(r).capitalize()} of {format_name_from_ont(c)}" for r, c in ent]
        x.sort()
        ans = ", ".join(x)
    elif question_num in (11, 13):
        ans = str(len(ent))
    print(ans)


def qna(question: str):
    question = question.strip()
    question = re.sub(' +', ' ', question)
    for idx, q in enumerate(QUESTIONS):
        match = re.match(q['pattern'], question)
        if match:
            answer(idx, match.groupdict())
            return
    print("Don't know what question it is...")


qs = [
    "Who is the president of China?",
    "Who is the president of Portugal?",
    "Who is the president of Guam?",
    "Who is the prime minister of Eswatini?",
    "Who is the prime minister of Tonga?",
    "What is the population of Isle of Man?",
    "What is the population of Tokelau?",
    "What is the population of Djibouti?",
    "What is the area of Mauritius?",
    "What is the area of Luxembourg?",
    "What is the area of Guadeloupe?",
    "What is the form of government in Argentina?",
    "What is the form of government in Sweden?",
    "What is the form of government in Bahrain?",
    "What is the form of government in North Macedonia?",
    "What is the capital of Burundi?",
    "What is the capital of Mongolia?",
    "What is the capital of Andorra?",
    "What is the capital of Saint Helena, Ascension and Tristan da Cunha?",
    "What is the capital of Greenland?",
    "List all countries whose capital name contains the string hi",
    "List all countries whose capital name contains the string free",
    "List all countries whose capital name contains the string alo",
    "List all countries whose capital name contains the string baba",
    "How many  Absolute monarchy are also Unitary state?",
    "How many Dictatorship are also Presidential system?",
    "How many Dictatorship are also Authoritarian?",
    "How many presidents were born in Iceland? ",
    "How many presidents were born in Republic of Ireland? ",
    "When was the president of Fiji born?",
    "When was the president of United States born?",
    "Where was the president of Indonesia born?",
    "Where was the president of Uruguay born?",
    "Where was the prime minister of Solomon Islands born?",
    "When was the prime minister of Lesotho born?",
    "Who is Denis Sassou Nguesso?",
    "Who is David Kabua?", ]


def run_demo_questions():
    for q in qs:
        print("Q:", q)
        qna(q)


if __name__ == "__main__":
    # run_demo_questions()
    if len(sys.argv) < 2:
        print("Invalid usage: python3 geo_qa (create|question) [params]")
        exit(1)
    if sys.argv[1] == "create":
        create()
    elif sys.argv[1] == "question":
        if len(sys.argv) < 3:
            print("Invalid usage: python3 geo_qa question (question)")
            exit(1)
        qna(sys.argv[2])
    else:
        print("Unknown command", sys.argv[1])
        exit(1)
