from metrics import *
from datetime import datetime


#------Conexion a la base de datos-------------
collection = db.messages
database = mongoClient.metricscollector
database.metrics.drop()
database = mongoClient.metricscollector
collection1 = database.metrics


#---------String auxiliares para manejar los DateTime --------------
addDateTime = "T00:00:00Z"


def getFirstDateMission():
	init_date = collection.find().sort("created_at",1).limit(1)
	for f in init_date:
		init_date = f["created_at"]
	return init_date

def getActualDay():
	actual_day =datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
	actual_day = actual_day[0:10]+"T"+actual_day[11:19]+"Z"
	return actual_day

def totalMessagesMission():
	totalMessages = collection.find().count()
	return totalMessages

def sumADay(day):
	finish_day = datetime.strptime(day[0:10], '%Y-%m-%d')
	finish_day = finish_day + timedelta(days=1)
	finish_day = finish_day.strftime('%Y-%m-%d') + addDateTime
	return finish_day

def setDailyMetrics(finish_day):

	first_datetime = getFirstDateMission()

	metrics = { "datetime": finish_day,
				"user_metrics": [
					{"user_name": "Matias Aravena", "activity": getActivity(33,first_datetime,finish_day), "leadership": getLeadership(33,first_datetime,finish_day), "performance": getPerformance(33,first_datetime,finish_day), "rithm": getRithm(33,first_datetime,finish_day), "determination": getDetermination(33,first_datetime,finish_day)},
					{"user_name": "Felipe Prambs", "activity": getActivity(34,first_datetime,finish_day), "leadership": getLeadership(34,first_datetime,finish_day), "performance": getPerformance(34,first_datetime,finish_day), "rithm": getRithm(34,first_datetime,finish_day), "determination": getDetermination(34,first_datetime,finish_day)},
					{"user_name": "Juan Perez", "activity": getActivity(61,first_datetime,finish_day), "leadership": getLeadership(61,first_datetime,finish_day), "performance": getPerformance(61,first_datetime,finish_day), "rithm": getRithm(61,first_datetime,finish_day), "determination": getDetermination(61,first_datetime,finish_day)},
					{"user_name": "Daniel Aravena", "activity": getActivity(62,first_datetime,finish_day), "leadership": getLeadership(62,first_datetime,finish_day), "performance": getPerformance(62,first_datetime,finish_day), "rithm": getRithm(62,first_datetime,finish_day), "determination": getDetermination(62,first_datetime,finish_day)},
					{"user_name": "Diego Fernandez", "activity": getActivity(63,first_datetime,finish_day), "leadership": getLeadership(63,first_datetime,finish_day), "performance": getPerformance(63,first_datetime,finish_day), "rithm": getRithm(63,first_datetime,finish_day), "determination": getDetermination(63,first_datetime,finish_day)}
				]
	}
	collection1.insert(metrics)

	if(getActualDay()[0:10] != finish_day[0:10]):
		setDailyMetrics(sumADay(finish_day))
		
	else:
		print("Metricas insertadas")


setDailyMetrics(sumADay(getFirstDateMission()))


