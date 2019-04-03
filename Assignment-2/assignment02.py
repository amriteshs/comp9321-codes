"""
COMP9321 2019 Term 1 Assignment Two Code Template
Student Name: Amritesh Singh
Student ID: z5211987
"""

import sqlite3
from flask import Flask
from flask_restplus import Resource, Api, fields, inputs
import requests
import json
from datetime import datetime

app = Flask(__name__)
api = Api(app, default='Collections', title='Collections Dataset', description='COMP9321 Assignment-2\n\n'
                                                                               'NOTE:\n'
                                                                               '1.) "collection_id" has been assigned the same value as "indicator_id".\n'
                                                                               '2.) Countries with "countryiso3code" set to NULL have also been considered.\n'
                                                                               '3.) For Q6, countries with "value" set to NULL are excluded from sorting when parameter "q" is specified.\n'
                                                                               '4.) The database name is "data.db" and table name is "collections".')

collection_model = api.model('collections', {
    'indicator_id': fields.String(required=True, example='NY.GDP.MKTP.CD')
})

parser = api.parser()
parser.add_argument('q', type=inputs.regex('^(top|bottom)([1-9][0-9]?|100)$'), help='Format: top/bottom<1-100>')

db_name = 'data.db'


def create_db(db_file):
    try:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

        c.execute('CREATE TABLE IF NOT EXISTS collections(  '
                  '     collection_id       TEXT,           '
                  '     indicator           TEXT,           '
                  '     indicator_value     TEXT,           '
                  '     creation_time       TEXT,           '
                  '     country             TEXT,           '
                  '     date                TEXT,           '
                  '     value               REAL            '
                  ')                                        '
                  )

        conn.commit()

        return conn
    except sqlite3.Error as e:
        print(e)


@api.route('/collections')
class Q1Q3(Resource):
    @api.response(200, 'OK')
    @api.response(404, 'No collections available')
    @api.doc(description='Retrieve list of available collections')
    def get(self):
        conn = create_db(db_name)
        c = conn.cursor()

        collection_list = [row for row in c.execute('SELECT DISTINCT collection_id, creation_time FROM collections')]

        conn.close()

        if not collection_list:
            return json.loads(json.dumps({'message': 'No collections are available in the database'})), 404

        response_body = []

        for row in collection_list:
            response_body.append({
                'location': f'/collections/{row[0]}',
                'collection_id': row[0],
                'creation_time': row[1],
                'indicator': row[0]
            })

        return json.loads(json.dumps(response_body)), 200

    @api.response(200, 'OK')
    @api.response(201, 'Created')
    @api.response(404, 'Invalid indicator')
    @api.doc(description='Import collection by indicator')
    @api.expect(collection_model, validate=True)
    def post(self):
        indicator_id = api.payload['indicator_id']

        url = f'http://api.worldbank.org/v2/countries/all/indicators/{indicator_id}?date=2013:2018&format=json&page=1&per_page=100'
        collection_data = requests.get(url=url).json()

        if 'message' in collection_data[0]:
            return json.loads(json.dumps({'message': collection_data[0]['message'][0]['value']})), 404

        conn = create_db(db_name)
        c = conn.cursor()

        if indicator_id in [row[0] for row in c.execute('SELECT DISTINCT collection_id FROM collections')]:
            response_body = {
                'location': f'/collections/{indicator_id}',
                'collection_id': indicator_id,
                'creation_time': [row for row in c.execute(f'SELECT DISTINCT creation_time FROM collections WHERE collection_id LIKE "{indicator_id}"')][0][0],
                'indicator': indicator_id
            }

            conn.close()

            return json.loads(json.dumps(response_body)), 200

        creation_time = f"{datetime.utcnow().isoformat().split('.')[0]}Z"

        db_data = {
            'collection_id': indicator_id,
            'indicator': indicator_id,
            'indicator_value': collection_data[1][0]['indicator']['value'],
            'creation_time': creation_time,
            'entries': []
        }

        for row in collection_data[1]:
            db_data['entries'].append({
                'country': row['country']['value'],
                'date': row['date'],
                'value': row['value']
            })

            c.execute(f'INSERT INTO collections VALUES(?,?,?,?,?,?,?)', (db_data['collection_id'], db_data['indicator'], db_data['indicator_value'], db_data['creation_time'], row['country']['value'], row['date'], row['value']))

        conn.commit()
        conn.close()

        response_body = {
            'location': f'/collections/{indicator_id}',
            'collection_id': indicator_id,
            'creation_time': creation_time,
            'indicator': indicator_id
        }

        return json.loads(json.dumps(response_body)), 201


@api.route('/collections/<string:collection_id>')
class Q2Q4(Resource):
    @api.response(200, 'OK')
    @api.response(404, 'Collection unavailable')
    @api.doc(description='Retrieve a collection')
    def get(self, collection_id):
        conn = create_db(db_name)
        c = conn.cursor()

        collection_data = [row for row in c.execute(f'SELECT * FROM collections WHERE collection_id LIKE "{collection_id}"')]

        conn.close()

        if not collection_data:
            return json.loads(json.dumps({'message': 'The specified collection is not available in the database'})), 404

        response_body = {
            'collection_id': collection_data[0][0],
            'indicator': collection_data[0][1],
            'indicator_value': collection_data[0][2],
            'creation_time': collection_data[0][3],
            'entries': []
        }

        for row in collection_data:
            response_body['entries'].append({
                'country': row[4],
                'date': row[5],
                'value': row[6]
            })

        return json.loads(json.dumps(response_body)), 200

    @api.response(200, 'OK')
    @api.response(404, 'Collection unavailable')
    @api.doc(description='Delete a collection')
    def delete(self, collection_id):
        conn = create_db(db_name)
        c = conn.cursor()

        if not [row for row in c.execute(f'SELECT DISTINCT collection_id FROM collections WHERE collection_id LIKE "{collection_id}"')]:
            conn.close()

            return json.loads(json.dumps({'message': 'The specified collection is not available in the database'})), 404

        c.execute(f'DELETE FROM collections WHERE collection_id LIKE "{collection_id}"')

        conn.commit()
        conn.close()

        return json.loads(json.dumps({'message': f'Collection = {collection_id} is removed from the database!'})), 200


@api.route('/collections/<string:collection_id>/<string:year>/<string:country>')
class Q5(Resource):
    @api.response(200, 'OK')
    @api.response(404, 'Collection unavailable')
    @api.doc(description='Retrieve indicator value for given country and year')
    def get(self, collection_id, year, country):
        conn = create_db(db_name)
        c = conn.cursor()

        collection_data = [row for row in c.execute(f'SELECT * FROM collections WHERE collection_id LIKE "{collection_id}" AND date LIKE "{year}" AND country LIKE "{country}"')]

        conn.close()

        if not collection_data:
            return json.loads(json.dumps({'message': 'The specified collection is not available in the database'})), 404

        response_body = {
            'collection_id': collection_data[0][0],
            'indicator': collection_data[0][1],
            'country': collection_data[0][4],
            'year': collection_data[0][5],
            'value': collection_data[0][6]
        }

        return json.loads(json.dumps(response_body)), 200


@api.route('/collections/<string:collection_id>/<string:year>')
class Q6(Resource):
    @api.response(200, 'OK')
    @api.response(404, 'Collection unavailable')
    @api.doc(description='Retrieve top/bottom indicator values for given year')
    @api.expect(parser, validate=True)
    def get(self, collection_id, year):
        conn = create_db(db_name)
        c = conn.cursor()

        collection_data = [row for row in c.execute(f'SELECT * FROM collections WHERE collection_id LIKE "{collection_id}" AND date LIKE "{year}"')]

        conn.close()

        query = parser.parse_args()['q']

        if query:
            if query.startswith('top'):
                collection_data = sorted([row for row in collection_data if row[6] is not None], key=lambda k: k[6], reverse=True)[:int(query[3:])]
            else:
                collection_data = sorted([row for row in collection_data if row[6] is not None], key=lambda k: k[6])[:int(query[6:])]

        if not collection_data:
            return json.loads(json.dumps({'message': 'The specified collection is not available in the database'})), 404

        response_body = {
            'indicator': collection_data[0][1],
            'indicator_value': collection_data[0][2],
            'entries': []
        }

        for row in collection_data:
            response_body['entries'].append({
                'country': row[4],
                'date': row[5],
                'value': row[6]
            })

        return json.loads(json.dumps(response_body)), 200


if __name__ == '__main__':
    app.run(debug=True)
