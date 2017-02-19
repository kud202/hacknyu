from datetime import timedelta, datetime
from time import time
import os

import googlemaps
import polyline
from flask import Flask, g, jsonify, request, redirect, url_for, flash
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config.from_envvar('CONFIG_FILE')


ALLOWED_EXTENSIONS = {'csv'}


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
                "lat": lon,
                "lon": lat
            }
        }
    }


def make_query(points,frm=0):
    return {
        "size": 10000,
        "from": frm,
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


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def get_weight_for_crime(req_time, crime):
    req_time = datetime.fromtimestamp(req_time)
    t1 = req_time.hour
    t2 = crime[u'hour']
    w_hour = 10-(min((t1 - t2) % 24, (t2 - t1) % 24)*10.0/12)
    t1 = req_time.month
    t2 = crime[u'month']
    w_month = 10-(min((t1 - t2) % 12, (t2 - t1) % 12)*10.0/6)
    return (10*crime[u'crime_weight'] + 5*w_hour + 2*w_month)/170.0


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

            query_offset = 0
            crimes = []
            while True:
                query = make_query(points, query_offset)
                results = get_es().search(index=app.config['INDEX_NAME'], body=query)
                crimes += results[u'hits'][u'hits']
                if len(results[u'hits'][u'hits']) == 10000:
                    query_offset += 10000
                    continue
                else:
                    break
            crime_per_meter = sum([
                get_weight_for_crime(
                    departure + time_offset.seconds,
                    c[u'_source']
                ) for c in crimes
            ]) / dist
            # crime_per_meter = len(results[u'hits'][u'hits']) / dist
            response['routes'].append({
                "polyline": r[u'overview_polyline'][u'points'],
                "departure_offset": time_offset.seconds,
                "crime_rate": crime_per_meter,
                "crime_locations": [hit[u'_source'][u'location'] for hit in crimes]
            })
        time_offset += timedelta(hours=1)
    return jsonify(response)


@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        csv_file = request.files['file']
        # if user does not select file, browser also
        # submit a empty part without filename
        if csv_file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if csv_file and allowed_file(csv_file.filename):
            filename = secure_filename(csv_file.filename)
            csv_file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            return redirect(url_for('uploaded_file',
                                    filename=filename))
    return '''
    <!doctype html>
    <title>Upload new data file.</title>
    <h1>Upload new data file (must be csv)</h1>
    <form method=post enctype=multipart/form-data>
      <p><input type=file name=file>
         <input type=submit value=Upload>
    </form>
    '''


if __name__ == '__main__':
    app.run()
