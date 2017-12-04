from pymongo import MongoClient
import requests

#---- Elimino la coleccion de mensajes existente ---------------------
mongoClient = MongoClient('localhost',27017);
db = mongoClient.collectormessages
db.messages.drop()

#-----Creo la coleccion mensajes--------------------------------------
db = mongoClient.collectormessages
print("Base de datos creada")
collection = db.messages
print("Coleccion Creada")

#-----Cadenas con la direccion de texto de la mision
api_url = "http://octopull.me/api"
entry_point = "/messages?"
channel_id = "tAnmWvReQM-SaLo2GDPr"
api_ver = ";apiver=v2"

#------ Headers para la conexion
headers = {
	"Content-Type": "application/json",
    "Authorization": "token fd4461949e5a53a843254aa77bd87f8e"
}
'''Metodo para obtener todos los mensajes paginados'''
def getData(page):
	url = api_url + entry_point + "channel_id=" + channel_id + "&amp&page=" + str(page) + api_ver
	response = requests.get(url, headers=headers)
	if response.status_code==200:
		results = response.json()
		for result in results:
			print result["id"]
			db.messages.insert(result)
		if(response.headers["x-next-page"] !=""):
			getData(int(response.headers["x-next-page"]));
		print(response.headers["x-next-page"])
	else:
		print "Error code %s" % response.status_code

#---Obtengo los mensajes desde la pagina 1 ------------
getData(1)







