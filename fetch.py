import mechanize
import urllib
import json
import pprint
import argparse


def fetch(loginURL, params={}, referer=None, geometry=None, where=None):
	# Browser
	br = mechanize.Browser()
	br.set_handle_referer(False)    # allow everything to be written to
	br.set_handle_robots(False)   # no robots
	br.set_handle_refresh(True)  # can sometimes hang without this
	br.set_handle_redirect(False)
	br.set_handle_gzip(True)

	br.addheaders = [
		('Accept-Encoding','gzip, deflate'),
		('Accept-Language','en-US,en;q=0.8'),
		('User-Agent','Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.93 Safari/537.36'),
		('Content-Type','application/x-www-form-urlencoded'),
		('Accept','*/*'),
	#	('Referer','http://maps.indy.gov/MapIndy/index.swf'),
		('X-Requested-With','ShockwaveFlash/18.0.0.233'),
		('Connection','keep-alive'),
		('Content-Length','1742'),
		('Expect','100-continue')
		]

	default_params = {
		#'where':"(INCIDENTREPORTS LIKE '%Offense Against Person%' OR INCIDENTREPORTS LIKE '%Property Offense%' OR INCIDENTREPORTS LIKE '%Drug Offense%' OR INCIDENTREPORTS LIKE '%Offense Against Society%' OR INCIDENTREPORTS LIKE '%Other Offense%' OR INCIDENTREPORTS LIKE '%Other Incident%' AND IWFROM_DATE > 20150907)",
		#'inSR':'{"wkt":"PROJCS[\"NAD_1983_StatePlane_Indiana_East_FIPS_1301_USFeet\",GEOGCS[\"GCS_North_American_1983\",DATUM[\"D_North_American_1983\",SPHEROID[\"GRS_1980\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",328083.3333333333],PARAMETER[\"False_Northing\",820208.3333333331],PARAMETER[\"Central_Meridian\",-85.66666666666667],PARAMETER[\"Scale_Factor\",0.9999666666666667],PARAMETER[\"Latitude_Of_Origin\",37.5],UNIT[\"Foot_US\",0.3048006096012192]]"}',
		#'inSR':'{"wkt":"PROJCS["NAD_1983_StatePlane_Indiana_East_FIPS_1301_USFeet",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",328083.3333333333],PARAMETER["False_Northing",820208.3333333331],PARAMETER["Central_Meridian",-85.66666666666667],PARAMETER["Scale_Factor",0.9999666666666667],PARAMETER["Latitude_Of_Origin",37.5],UNIT["Foot_US",0.3048006096012192]]"}',
		'f':'json',
		#'outFields':'*',
		#'returnDistinctValues':'false',
		'spatialRel':'esriSpatialRelIntersects',
		'geometryType':'esriGeometryEnvelope',
		'returnGeometry':'false',
		'returnIdsOnly':'true',
	#	'geometry':'{"ymin":1595184.673673472,"ymax":1705840.0556179164,"xmin":137148.39186537726,"xmax":255203.94742093276}',
	}

	if where is not None:
		where_param = {
			'where':where
		}
		default_params.update(where_param)

	if geometry is not None:
		geometry_param = {
			'geometry':geometry
		}
		default_params.update(geometry_param)

	if referer is not None:
		br.addheaders.append(('Referer',referer))

	default_params.update(params)

	data = urllib.urlencode(default_params)
	print data
	request = mechanize.Request(loginURL,data=data)
	try:
		response = br.open(request)
	except mechanize.URLError:
		response = br.open(request)
	return json.loads(response.read())


pp = pprint.PrettyPrinter(indent=2)

parser = argparse.ArgumentParser(description='Harvest data from ArcGIS FeatureServer')
parser.add_argument('-u','--url', help='ArcGIS Server Query URL', required=False, default='http://imaps.indy.gov/arcgis/rest/services/MapIndyProperty/MapServer/10/query')
parser.add_argument('-g','--geometry', help='geometry for query', required=False, default='{"ymin":1595184.673673472,"ymax":1705840.0556179164,"xmin":137148.39186537726,"xmax":255203.94742093276}')
parser.add_argument('-r','--referer', help='referrer header, sometimes required by the server', required=False, default='http://maps.indy.gov/MapIndy/index.swf')
parser.add_argument('-w','--where', help='where section of query', required=False)

args = parser.parse_args()

loginURL = args.url
geometry = args.geometry
referer = args.referer
where = args.where

parsed_json = fetch(loginURL=loginURL,geometry=geometry, referer=referer, where=where)
pp.pprint(parsed_json)
object_ids = []

# gernate queries with up to 1000 objectids per query
for idx,val in enumerate(parsed_json['objectIds']):
	object_ids.append(val)
	if (idx==len(parsed_json['objectIds'])-1 or (idx+1)%1000==0):
		print "break point",idx
		query ='' # reset query

		while object_ids:
			query += "OBJECTID={0} OR ".format(object_ids.pop())

		query = query[0:len(query)-4] # trim the last " OR " off the query

		params = {
			'where':query,
			'returnGeometry':'true',
			'returnIdsOnly':'false',
			'outfields':'*'
		}

		parsed_json2 = fetch(params=params, loginURL=loginURL, referer=referer)

		f = open('incidents-{0}'.format(idx)+'.json','w')
		f.write(json.dumps(parsed_json2))
