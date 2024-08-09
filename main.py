import json
import os
import pathlib
import urllib.request
import uuid

from dotenv import load_dotenv
from neo4j import GraphDatabase
from tqdm import tqdm


load_dotenv()

def download_json():
    url = os.getenv("JSON_URL")
    save_path = os.getenv("JSON_PATH")

    def download_progress_hook(block_num, block_size, total_size):
        if block_num == 0:
            print(f"Downloading `${save_path}` ...")
            global pbar
            pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc=save_path)
        downloaded = block_num * block_size
        if downloaded < total_size:
            pbar.update(block_size)
        else:
            pbar.close()

    urllib.request.urlretrieve(url, save_path, reporthook=download_progress_hook)


def restructure_data():

    data = None
    with (pathlib.Path(__file__).parent / os.getenv("JSON_PATH")).open() as f:
        data = json.loads(f.read())["data"]

    mapping = {}

    for anime in data:
        _id = uuid.uuid4().hex
        sources = anime["sources"]
        title = anime["title"]
        
        for source in sources:
            mapping[source] = {"_id": _id,"title": title }

    restructured_data = []

    for anime in data:
        source = anime["sources"][0]
        _id = mapping[source]["_id"]
        related = [mapping[source]["_id"] for source in anime["relatedAnime"]]

        restructured_data.append({
            "_id": _id,
            "title": anime["title"],
            "type": anime["type"],
            "episodes": anime["episodes"],
            "status": anime["status"],
            "season": anime["animeSeason"],
            "tags": anime["tags"],
            "related": related
        })

    with (pathlib.Path(__file__).parent / os.getenv("JSON_PATH")).open("w") as f:
        f.write(json.dumps(restructured_data))


def load_to_neo4j():

    URI = os.getenv("NEO4J_URI")
    AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

    data = None
    with (pathlib.Path(__file__).parent / os.getenv("JSON_PATH")).open() as f:
        data = json.loads(f.read())
        
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()

        driver.execute_query(
            """
            CREATE INDEX IF NOT EXISTS FOR (a:Anime) ON (a._id)
            """
        )

        # # NOTE: following query was able to insert ~200 animes in 1 minute.
        # for anime in data:
        #     driver.execute_query(
        #         "CREATE (a:Anime {_id: $_id, title: $title, num_episodes: $num_episodes})",
        #         _id=anime["_id"], title=anime["title"], num_episodes=anime["episodes"],
        #         database_="neo4j"
        #     )
        
        # NOTE: following query was able to insert all animes in 22 seconds.
        # https://neo4j.com/docs/python-manual/current/performance/#batch-data-creation
        driver.execute_query(
            """
            WITH $data AS animes
            UNWIND animes AS anime
            CREATE (a:Anime)
            SET a._id = anime.id, a.title = anime.title, a.num_episodes = anime.episodes
            """,
            data=data
        )

        # TODO: create following nodes and relationships 
        # - (:Anime)-[:OF_TYPE]-(:Type {name: 'SPECIAL'})
        # - (:Anime)-[:HAS_STATUS]-(:Status {name: 'FINISHED'})
        # - (:Anime)-[:HAS_TAG]-(:Tag {name: 'shonen'})
        # - (:Anime)-[:IN_SEASON]-(:Season {name: 'FALL'})
        # - (:Anime)-[:IN_YEAR]-(:Year {value: 2000})
        # - (:Anime)-[:IS_RELATED]-(:Anime)


if __name__ == "__main__":

    download_json()
    restructure_data()
    load_to_neo4j()
