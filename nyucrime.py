from datetime import timedelta
from time import time

import googlemaps
import polyline
from flask import Flask, g, jsonify, request
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth


app = Flask(__name__)
app.config.from_envvar('CONFIG_FILE')


def get_es():
    if hasattr(g, 'es'):
        return g.es
    else:
        g.es = Elasticsearch(
            host=app.config['ES_HOST'],
            port=80,
            connection_class=RequestsHttpConnection,
            http_auth=AWSRequestsAuth(
                aws_access_key=app.config['AWS_ACCESS_KEY'],
                aws_secret_access_key=app.config['AWS_KEY_SECRET'],
                aws_host=app.config['ES_HOST'],
                aws_region=app.config['ES_REGION'],
                aws_service='es'
            )
        )
        return g.es


def get_gmaps():
    if hasattr(g, 'gmaps'):
        return g.gmaps
    else:
        g.gmaps = googlemaps.Client(key=app.config["GOOGLE_MAPS_KEY"])
        return g.gmaps


def get_bounding_box(lat, lon):
    return {
        "geo_distance": {
            "distance": app.config['CRIME_PROXIMITY'],
            "location": {
                "lat": lat,
                "lon": lon
            }
        }
    }


def make_query(points):
    return {
        "size": 10000,
        "query": {
            "bool": {
                "must": {
                    "match_all": {}
                },
                "should": [get_bounding_box(x[1], x[0]) for x in points],
                "minimum_should_match": 1
            }
        }
    }


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/directions')
def get_routes():
    # Request Parameters

    departure = request.args.get('departure_time', int(time()))
    travel_mode = request.args.get('mode', "walking")
    frm = request.args.get('from', "5816, 4th Avenue, Brooklyn, NY, 11220")
    to = request.args.get('to', "6 MetroTech Center, Brooklyn, NY, 11201")
    try:
        till = int(request.args.get('till', 0))
        assert till < 24
    except (ValueError, AssertionError):
        return "Till should by integer between 1 and 24", 400
    else:
        till = timedelta(hours=till)

    # Response Object

    response = {
        'departure': departure,
        'travel_mode': travel_mode,
        'from': frm,
        'to': to,
        'routes': []
    }

    time_offset = timedelta(hours=0)
    while time_offset <= till:
        directions = get_gmaps().directions(
            frm, to,
            mode=travel_mode,
            alternatives=True,
            departure_time=departure + time_offset.seconds
        )
        for r in directions:
            points = []
            dist = 0.0
            for leg in r[u'legs']:
                for step in leg[u'steps']:
                    if step[u'travel_mode'] == "WALKING":
                        dist = dist + step[u'distance'][u'value']
                        points = points + polyline.decode(step[u'polyline'][u'points'])
            query = make_query(points)
            results = get_es().search(index=app.config['INDEX_NAME'], body=query)
            crime_per_meter = len(results[u'hits'][u'hits']) / dist
            response['routes'].append({
                "polyline": r[u'overview_polyline'][u'points'],
                "departure_offset": time_offset.seconds,
                "crime_rate": crime_per_meter,
                "crime_locations": [hit[u'_source'][u'location'] for hit in results[u'hits'][u'hits']]
            })
        time_offset += timedelta(hours=1)
    return jsonify(response)

if __name__ == '__main__':
    app.run()
