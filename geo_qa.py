from queue import Queue
import queue
import sys
from typing import Any, Callable
from webbrowser import BackgroundBrowser
import rdflib
import requests
import lxml.html
import re
from rdflib import URIRef, Literal
from rdflib.namespace import FOAF, DCTERMS, XSD, RDF, SDO

DOMAIN = "http://example.org/"
global list_of_countries, pre
list_of_countries = set()
pre = "https://dbpedia.org/page/"

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


def extract_link_from_infobox(table, label: str):
    """Extract next wiki link"""
    href = table.xpath(f"./tbody/tr[th//text()='{label}']/td/a/@href")
    link = next(filter(lambda x: x.startswith("/wiki"), href), None)
    if link is None:
        return None

    return create_wiki_url(link.replace("/wiki/", ""))


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


def create_base_graph():
    global g, president_of, prime_minister_of, capital_of, type_government_of, area_of, population_of, vp_of, birth_day, has_the_role_of, birth_place
    g = rdflib.Graph()
    president_of = URIRef('https://dbpedia.org/ontology/president')
    prime_minister_of = URIRef('https://dbpedia.org/ontology/PrimeMinister')
    capital_of = URIRef('https://dbpedia.org/ontology/capital')
    type_government_of = URIRef('https://dbpedia.org/ontology/governmentType')
    area_of = URIRef('https://dbpedia.org/ontology/PopulatedPlace/area')
    population_of = URIRef('https://dbpedia.org/property/populationCensus')
    vp_of = URIRef('https://dbpedia.org/ontology/VicePresident')
    birth_day = URIRef('https://dbpedia.org/ontology/birthDate')
    has_the_role_of = URIRef('https://dbpedia.org/ontology/role')
    birth_place = URIRef('https://dbpedia.org/ontology/birthPlace')


class Crawler:
    start_url = "https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)"
    create_base_graph()

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
            # page = self.download_page(task['url'])
            # task['handler'](page, task.get("meta"))
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
        print("Parsing", meta)
        president_name = None
        pm_name = None
        infobox = page.xpath("//table[contains(@class, 'infobox')]")[0]
        data = {
            "country": meta['name'],
            "capital": next(iter(extract_label_from_infobox(infobox, "Capital")), None),
            # "largest_city": next(iter(extract_label_from_infobox(infobox, "Largest city")), None),
            "government": extract_label_from_infobox(infobox, "Government"),
            "area": next(iter(extract_merged_label_from_infobox(infobox, "Area ")), None),
            "population": next(iter(extract_merged_label_from_infobox(infobox, "Population")), None),
            "president": next(iter(extract_label_from_infobox(infobox, "President")), None),
            "vp": next(iter(extract_label_from_infobox(infobox, "Vice President")), None),
            "pm": next(iter(extract_label_from_infobox(infobox, "Prime Minister")), None),
            # Different name for prime minister
            "premier": next(iter(extract_label_from_infobox(infobox, "Premier")), None),
        }
        if data["area"]:
            data["area"] = get_first_num(data["area"])
        if data["population"]:
            data["population"] = get_first_num(data["population"])

        if data["president"]:
            self.enqueue_page(
                extract_link_from_infobox(
                    infobox, "President") or create_wiki_url(data["president"]),
                handler=self.parse_person,
                meta={"role": "president",
                      "name": data["president"], "country": meta["name"]}
            )
            president_name = data["president"].replace(' ', '_')
            pres = True
        if data["pm"]:
            self.enqueue_page(
                extract_link_from_infobox(
                    infobox, "Prime Minister") or create_wiki_url(data["pm"]),
                handler=self.parse_person,
                meta={"role": "pm",
                      "name": data["pm"], "country": meta["name"]}
            )
            pm_name = data["pm"].replace(' ', '_')
        if data["premier"]:
            self.enqueue_page(
                extract_link_from_infobox(
                    infobox, "Primier") or create_wiki_url(data["premier"]),
                handler=self.parse_person,
                meta={"role": "pm",
                      "name": data["premier"], "country": meta["name"]}
            )
            pm_name = data["premier"].replace(' ', '_')
        country_name = data["country"].replace(' ', '_')
        country_name = country_name.capitalize()
        list_of_countries.add(country_name)
        country = rdflib.URIRef('https://dbpedia.org/page/' + country_name)
        if president_name not in (None, "None"):
            president_name = president_name.capitalize()
            president = rdflib.URIRef('https://dbpedia.org/page/' + president_name)
        else:
            president = None
        if pm_name not in (None, "None"):
            pm_name = pm_name.capitalize()
            pm = rdflib.URIRef('https://dbpedia.org/page/' + pm_name)
        else:
            pm = None
        population = data["population"]
        area = data["area"]
        vp = data["vp"]
        capital = Literal(data["capital"])
        government = data["government"]
        if president not in (None, "None"):
            g.add((president, president_of, country))
        if pm not in (None, "None"):
            g.add((pm, prime_minister_of, country))
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
                gov = i.replace(' ', '_')
                gov = gov.capitalize()
                gov = rdflib.URIRef('https://dbpedia.org/page/' + gov)
                g.add((country, type_government_of, gov))

    def parse_person(self, page, meta=None):
        """Parser for person (president/PM)"""
        print("Parsing", meta)
        infobox = page.xpath("//table[contains(@class, 'infobox')]")[0]
        born = infobox.xpath(f"./tbody/tr[th//text()='Born']/td")
        try:
            bcountry = born[0].xpath(".//text()")[-1]
            bcountry = re.sub(r"[\(\),\.]", "", bcountry)
            bcountry = bcountry.strip()
            if bcountry.startswith(","):
                bcountry = re.findall(r"\w+", bcountry)[0]
            if not bcountry and meta["country"] in "".join(born[0].xpath(".//text()")):
                bcountry = meta["country"]
        except:
            bcountry = None
        bday = next(iter(infobox.xpath("//span[@class='bday']/text()")), None)

        data = {
            "name": meta["name"],
            "role": meta["role"],
            "bday": bday,
            "bcountry": bcountry,
        }
        president_name = data["name"].replace(' ', '_')
        president_name = president_name.capitalize()
        president = rdflib.URIRef('https://dbpedia.org/page/' + president_name)
        role = data["role"]
        bday = data["bday"]
        b_country = data["bcountry"]
        if role not in (None, "None"):
            if (data["role"] == "president"):  # redundant
                g.add((president, has_the_role_of, president_of))
            else:
                g.add((president, has_the_role_of, prime_minister_of))
        if bday not in (None, "None"):
            bday = Literal(data["bday"], datatype=XSD.date)
            g.add((president, birth_day, bday))
        if b_country not in (None, "None"):
            b_country = Literal(data["bcountry"]).replace(' ', '_')
            b_country = b_country.capitalize()
            if b_country in list_of_countries:
                b_country = rdflib.URIRef('https://dbpedia.org/page/' + b_country)
                g.add((president, birth_place, b_country))

        print(data)


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


def answer(question_num: int, params: dict):
    question_num +=1
    print("The question num is:", question_num)
    print("The params are:", params)
    val = list(params.values())
    subs = val[0]
    for i in range (len(val)):
        val[i] = adjust_str(val[i])
    country = pre + val[0]
    entity = pre + val[0]
    if (len(params)>1):
        gf1 = pre + val[0]
        gf2 = pre + val[1]
    ans = ""
    if question_num == 0:
        pass
    elif question_num == 1:
        pass
    elif question_num == 2:
        pass
    elif question_num == 3:
        pass
    elif question_num == 4:
        pass
    elif question_num == 5:
        pass
    elif question_num == 6:
        pass
    elif question_num == 7:
        q = "SELECT ?y WHERE " \
            "{ ?x <" + president_of + "> <" + country + "> ." \
                                                        " ?x <" + birth_day + "> ?y " \
                                                                              "}"
    elif question_num == 8:
        q = "SELECT ?y WHERE " \
            "{ ?x <" + president_of + "> <" + country + "> ." \
                                                        " ?x <" + birth_place + "> ?y " \
                                                                                "}"
    elif question_num == 9:
        q = "SELECT ?y WHERE " \
            "{ ?x <" + prime_minister_of + "> <" + country + "> ." \
                                                             " ?x <" + birth_day + "> ?y " \
                                                                                   "}"
    elif question_num == 10:
        q = "SELECT ?y WHERE " \
            "{ ?x <" + prime_minister_of + "> <" + country + "> ." \
                                                             " ?x <" + birth_place + "> ?y " \
                                                                                     "}"
    elif question_num == 11:
        q = "SELECT ?x ?y WHERE " \
            "{ <" + entity + "> <" + has_the_role_of + "> ?x ." \
                                                       " <" + entity + "> <" + president_of + "> ?y .}"
    elif question_num == 12:
        q = "SELECT ?y WHERE { ?y <" + type_government_of + "> <" + gf1 + "> . ?y <" + type_government_of + "> <" + gf2 + "> .}"
    elif question_num == 13:
        q = "SELECT ?x ?y WHERE " \
            "{ ?x <" + capital_of + "> ?y .}"
    elif question_num == 14:
        q = "SELECT ?y WHERE " \
            "{ ?y <" + has_the_role_of + "> <" + president_of + "> ." \
                                                                "?y <" + birth_place + "> <" + country + "> .}"

    x = g2.query(q)
    if question_num in (7,8,9,10,11):
        print(list(x))
    if question_num == 13:
        lst_str = list(x)
        strings_with_substring = []
        for i in lst_str:
            a = i[1]
            if (subs in i[1]):
                strings_with_substring.append(i[0])
        print(strings_with_substring)
    if question_num in (12,14):
        print(len(x))

    # return ans


def qna(question: str):
    global g2
    g2 = rdflib.Graph()
    g2.parse("graph.nt", format="nt")
    for idx, q in enumerate(QUESTIONS):
        match = re.match(q['pattern'], question)
        if match:
            ans = answer(idx, match.groupdict())
            # print(ans)
            return
    print("Don't know...")


if __name__ == "__main__":
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

