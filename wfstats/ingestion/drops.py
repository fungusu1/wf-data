import requests as r
from helpers import get_con

def get_mission_rewards(db):
    pass

def get_transient_rewards(db):
    pass

if __name__ == "__main__":
    db = get_con()
    get_mission_rewards(db)
    get_transient_rewards(db)
    