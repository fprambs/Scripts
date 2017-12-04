from pymongo import MongoClient
from datetime import datetime, timedelta
from datetime import datetime

#------Database Conexion --------------------------------------------------------------
mongoClient = MongoClient('localhost',27017);
db = mongoClient.metricscollector
collection = db.metrics
#------ Auxiliar variables ------------------------------------------------------------
addDateTime = "T00:00:00Z"
list_activity = []


'''Metodo que obtiene el primer dia en la coleccion de metricas '''
def getFirstDateMetrics():
	init_date = init_date = collection.find().sort("datetime",1).limit(1)
	for i in init_date:
		init_date=(i["datetime"])
	return init_date
'''Metodo que obtiene el dia actual '''
def getActualDay():
	actual_day =datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
	actual_day = actual_day[0:10]+"T"+actual_day[11:19]+"Z"
	return actual_day
'''Metodo para sumar 1 dia a cualquier fecha '''
def sumADay(day):
	finish_day = datetime.strptime(day[0:10], '%Y-%m-%d')
	finish_day = finish_day + timedelta(days=1)
	finish_day = finish_day.strftime('%Y-%m-%d') + addDateTime
	return finish_day
'''Metodo para obtener recomendaciones diarias desde el primer dia hasta una fecha dada '''
def getRecommendation(finish_date):
	cursor = collection.find({"datetime": finish_date})
	print("-----------------------------------------------------------")
	print("Recomendaciones para los usuarios el dia: "+finish_date)
	determination = 0
	activity = 0
	#---Recorre las metricas de ese dia y crea las recomendaciones pertinentes --------
	for i in cursor:
		for k in range(0,5): 
			if(i["user_metrics"][k]["rithm"]> 0.5 and i["user_metrics"][k]["performance"] < 0.5 ):
				print(i["user_metrics"][k]["user_name"] +", te recomiendo definir tareas mas cortas")
			if(i["user_metrics"][k]["leadership"]> 0.5 and i["user_metrics"][k]["performance"] < 0.5 ):
				print(i["user_metrics"][k]["user_name"] +", te recomiendo delegar tareas")
			if(i["user_metrics"][k]["activity"]> 0.5 and i["user_metrics"][k]["leadership"] < 0.5 ):
				print(i["user_metrics"][k]["user_name"] +", te recomiendo participar en mas tareas de la mision")
			if(i["user_metrics"][k]["activity"]<0.2):
				print(i["user_metrics"][k]["user_name"] +", te recomiendo participar mas en la mision")
			#----Promedio de determinacion y actividad en la mision ------------------------------
			determination = determination + i["user_metrics"][k]["determination"]
			activity = activity + i["user_metrics"][k]["activity"]

	activity = activity/5
	list_activity.append(activity)
	
	if(determination/5<0.5):
		print("Recomendacion para la mision")
		print("Recomiendo redefinir el perimetro de la mision o abandonarla")
	if(activity<0.2):
		print("La actividad esta bajando")



	#-----Llamada recursiva para obtener metricas hasta el ultimo dia ------
	if(getActualDay()[0:10] != finish_date[0:10]):
		getRecommendation(sumADay(finish_date))
	else:
		print("No existen mas recomendaciones !")
		#print(list_activity)


'''Obtengo las metricas desde el primer dia en que se registraron'''
getRecommendation(getFirstDateMetrics())