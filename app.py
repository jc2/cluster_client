from collections import defaultdict
from random import randint
from time import sleep

from flask import Flask, jsonify, request

app = Flask(__name__)

data = defaultdict(list)


def alter_rute(f):
    def wrapper(*args, **kwargs):
        sleep(randint(1, 5))
        if randint(2, 3) == 3:
            return "", 500
        a = f(*args, **kwargs)
        print("NUMBER OF NODES:", len({k: v for k, v in data.items if v}))
        print("DATA:", data)
        return a
    wrapper.__name__ = f.__name__
    return wrapper


@app.route("/<node>/v1/group", methods=['POST'])
@alter_rute
def create_group(node):
    group = request.json.get("groupId")
    if group not in data:
        data[node].append(group)
        return jsonify({"groupId": group}), 201
    else:
        return "", 400


@app.route("/<node>/v1/group", methods=['DELETE'])
@alter_rute
def delete_group(node):
    group = request.json.get("groupId")
    try:
        data[node].remove(group)
    except ValueError:
        pass
    return "", 200


@app.route("/<node>/v1/group/<name>", methods=['GET'])
@alter_rute
def get_group(node, name):
    if name in data[node]:
        return jsonify({"groupId": name}), 200
    else:
        return "", 404
