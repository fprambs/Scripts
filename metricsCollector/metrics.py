from pymongo import MongoClient
from dateutil import parser
from datetime import datetime, timedelta
import time, datetime
import pandas as pd
import json, ast
from datetime import datetime


#-------Conexion a la base de datos mongo -------------------------
mongoClient = MongoClient('localhost',27017);
db = mongoClient.collectormessages
collection = db.messages

'''Metodo para obtener el total de mensajes de la mision entre dos fechas'''
'''Parametros: type(User or Agent), start_date(Fecha inicio), finish_date(Fecha final)'''
def getTotalMessages(type,start_date,finish_date):
	cursor = collection.find({"created_at": {'$lt': finish_date, '$gte': start_date},"author.type":str(type)}).count();
	return cursor
'''Metodo para obtener el total de mensajes de un usuario en la mision'''
'''Parametros: user_id(Id del usuario en Octopull), start_date(Fecha inicio), finish_date(Fecha final)'''
def getMessagesTotalByUser(user_id,start_date,finish_date):
	cursor = collection.find({"created_at": {'$lt': finish_date, '$gte': start_date},"author.id":user_id}).count();
	return cursor
'''Metodo para obtener el total de las tareas en la mision entre dos fechas'''
'''Parametros:user_id(Id del usuario en Octopull), start_date(Fecha inicio), finish_date(Fecha final)'''
def getTotalTasks(start_date,finish_date):
	cursor = collection.find({"created_at": {'$lt': finish_date, '$gte': start_date},"type":"Promise"}).count();
	return cursor
'''Metodo para obtener el total de tareas de un usuario entre dos fechas'''
'''Parametros: start_date(Fecha inicio), finish_date(Fecha final)'''
def getTotalTaskByUser(user_id,start_date,finish_date):
	cursor = collection.find({"created_at": {'$lt': finish_date, '$gte': start_date},"type":"Promise","assigned.id":user_id}).count();
	return cursor
'''Metodo para obtener el total de las tareas completadas de un usuario entre dos fechas'''
'''Parametros: user_id(Id del usuario en Octopull), start_date(Fecha inicio), finish_date(Fecha final)'''
def countUserCompletedTask(user_id,start_date,finish_date):
	c=0
	cursor = collection.find({"created_at": {'$lt': finish_date, '$gte': start_date},"type":"Promise", "assigned.id":user_id, "completed_at": {"$ne": None }})
	for i in cursor:
		actual_day = datetime.strptime(finish_date[0:10], '%Y-%m-%d')
		query_day = datetime.strptime(i["completed_at"][0:10],'%Y-%m-%d')
		if(actual_day>query_day):
			c=c+1	
	return c
'''Metodo para obtener el total de las tareas completadas en la mision entre dos fechas'''
'''Parametros: user_id(Id del usuario en Octopull), start_date(Fecha inicio), finish_date(Fecha final)''' 
def countMissionCompletedTask(start_date,finish_date):
	c=0
	cursor = collection.find({"created_at": {'$lt': finish_date, '$gte': start_date},"type": "Promise", "completed_at":{"$ne": None}})
	for i in cursor:
		actual_day = datetime.strptime(finish_date[0:10], '%Y-%m-%d')
		query_day = datetime.strptime(i["completed_at"][0:10],'%Y-%m-%d')
		if(actual_day>query_day):
			c=c+1	
	return c
'''Metodo para contar el total de horas en tareas de un usuario entre dos fechas'''
'''Parametros: user_id(Id del usuario en Octopull), start_date(Fecha inicio), finish_date(Fecha final)'''
def countUserTaskTime(user_id,start_date,finish_date):
	cursor = list(collection.find({"created_at": {'$lt': finish_date, '$gte': start_date},"type": "Promise", "assigned.id": user_id},{'established_at': 1,'duedate': 1}));
	try:
		rithm = calculateRithm(cursor)
		rithm = rithm["total_hours"] / rithm["total_task"]
		return rithm
	except:
		return (0)
'''Metodo para contar el total de horas en tareas de una mision entre dos fechas'''
'''Parametros: start_date(Fecha inicio), finish_date(Fecha final)'''
def countMissionTaskTime(start_date,finish_date):
	cursor = list(collection.find({"created_at": {'$lt': finish_date, '$gte': start_date},"type": "Promise"},{'established_at': 1,'duedate': 1}));
	try:
		rithm = calculateRithm(cursor)
		rithm = rithm["total_hours"] / rithm["total_task"]
		return rithm
	except:
		return (0)
'''Metodo para obtener el total de horas y el total de tareas'''
'''Parametros:task(Listado con las tareas entre fechas)'''
def calculateRithm(task):
	total_hours = 0;
	total_task = 0;
	for k in task:
		established_at = parser.parse(k['established_at']).replace(tzinfo=None)
		established_at = pd.Timestamp(established_at)
		established_at = time.mktime(established_at.timetuple())

		duedate = parser.parse(k['duedate']).replace(tzinfo=None)
		duedate = pd.Timestamp(duedate)
		duedate = time.mktime(duedate.timetuple())
		time_diff = duedate - established_at
		hours = (time_diff/(60*60))
		total_hours = total_hours + hours;
		total_task = total_task + 1	

	return {"total_hours": total_hours, "total_task": total_task}
'''Metodo para calcular las tareas atrasadas y completadas con atraso de un usuario entre fechas'''
'''Parametros: user_id(Id del usuario en Octopull), start_date(Fecha inicio), finish_date(Fecha final)'''
def calculateCurrentlyOverdueAndCompletedOverdue(user_id,start_date,finish_date):
	cursor = list(collection.find({"created_at": {'$lt': finish_date, '$gte': start_date},"type": "Promise", "assigned.id": user_id},{'completed_at': 1,'duedate': 1,"created_at":1}));
	actual_datetime = parser.parse(finish_date).replace(tzinfo=None)
	currentlyOverdue=0
	completedOverdue = 0
	completed = 0
	cursing = 0
	for f in cursor:
		#print(f["created_at"])
		#print(f['duedate'])
		#print(f["completed_at"])
		#print(actual_datetime)
		a= parser.parse(f['duedate']).replace(tzinfo=None)
		try:
			completed_at = parser.parse(f['completed_at']).replace(tzinfo=None)
		except:
			complete_at = None
		if(actual_datetime>a):
			if(f['completed_at']==None):
				#print("Atrasada")
				completedOverdue=completedOverdue+1
			else:
				if(completed_at >actual_datetime):
					#print("En curso")
					cursing = cursing+1
				else:
					if(f['completed_at'] > f['duedate']):
						#print("Completada con atraso")
						completedOverdue = completedOverdue+1
					else:
						#print("Completada a tiempo")
						completed = completed+1	
		else:
			#print("En curso")
			cursing = cursing+1
	return({"currentlyOverdue": currentlyOverdue, "completedOverdue": completedOverdue, "completed":completed,"cursing": cursing})
'''Metodo para calcular las tareas atrasadas y completadas con atraso de la mision entre fechas'''
'''Parametros: start_date(Fecha inicio), finish_date(Fecha final)'''
def calculateCurrentlyOverdueAndCompletedOverdue1(start_date,finish_date):
	cursor = list(collection.find({"created_at": {'$lt': finish_date, '$gte': start_date},"type": "Promise"},{'completed_at': 1,'duedate': 1,"created_at":1}));
	actual_datetime = parser.parse(finish_date).replace(tzinfo=None)
	currentlyOverdue=0
	completedOverdue = 0
	completed = 0
	cursing = 0
	for f in cursor:
		#print(f["created_at"])
		#print(f['duedate'])
		#print(f["completed_at"])
		#print(actual_datetime)
		a= parser.parse(f['duedate']).replace(tzinfo=None)
		try:
			completed_at = parser.parse(f['completed_at']).replace(tzinfo=None)
		except:
			completed_at = None
		if(actual_datetime>a):
			if(f['completed_at']==None):
				#print("Atrasada")
				completedOverdue=completedOverdue+1
			else:
				if(completed_at >actual_datetime):
					#print("En curso")
					cursing = cursing+1
				else:
					if(f['completed_at'] > f['duedate']):
						#print("Completada con atraso")
						completedOverdue = completedOverdue+1
					else:
						#print("Completada a tiempo")
						completed = completed+1	
		else:
			#print("En curso")
			cursing = cursing+1
	return({"currentlyOverdue": currentlyOverdue, "completedOverdue": completedOverdue, "completed":completed,"cursing": cursing})

#---------------------------------------------
'''Metodo para obtener la acitivad de un usuario'''
'''Parametros: user_id(Id del usuario en Octopull), start_date(Fecha inicio), finish_date(Fecha final)'''
def getActivity(user_id,start_date,finish_date):
	messages_user = float(getMessagesTotalByUser(user_id,start_date,finish_date))
	task_user = float(getTotalTaskByUser(user_id,start_date,finish_date))
	messages_mission= float(getTotalMessages("User",start_date,finish_date))
	task_mission= float(getTotalTasks(start_date,finish_date))
	activity = float((messages_user+task_user)/(messages_mission+task_mission))
	return activity
'''Metodo para obtener el liderazgo de un usuario'''
'''Parametros: user_id(Id del usuario en Octopull), start_date(Fecha inicio), finish_date(Fecha final)'''
def getLeadership(user_id,start_date,finish_date):
	task_user = float(countUserCompletedTask(user_id,start_date,finish_date))

	task_mission = float(countMissionCompletedTask(start_date,finish_date))

	try:	
		leadership = float(task_user/task_mission)
		return leadership
	except:
		return(0)
'''Metodo para obtener el performance de un usuario'''
'''Parametros: user_id(Id del usuario en Octopull), start_date(Fecha inicio), finish_date(Fecha final)'''
def getPerformance(user_id,start_date,finish_date):
	task_user = float(countUserCompletedTask(user_id,start_date,finish_date))
	task_assigned_user = float(getTotalTaskByUser(user_id,start_date,finish_date))
	try:
		performance = float(task_user/task_assigned_user)
		return performance
	except:
		return(0)
'''Metodo para obtener el ritmo de un usuario'''
'''Parametros: user_id(Id del usuario en Octopull), start_date(Fecha inicio), finish_date(Fecha final)'''
def getRithm(user_id,start_date,finish_date):
	user = float(countUserTaskTime(user_id,start_date,finish_date))
	mission = float(countMissionTaskTime(start_date,finish_date))
	rythm = float(user/(mission*2))
	return rythm
'''Metodo para obtener la determinacion de un usuario'''
'''Parametros: user_id(Id del usuario en Octopull), start_date(Fecha inicio), finish_date(Fecha final)'''
def getDetermination(user_id,start_date,finish_date):
	delayed_by_user = calculateCurrentlyOverdueAndCompletedOverdue(user_id,start_date,finish_date)
	delayed_by_user = delayed_by_user["currentlyOverdue"] + delayed_by_user["completedOverdue"]
	total_delayed_by_mission = calculateCurrentlyOverdueAndCompletedOverdue1(start_date,finish_date)
	total_delayed_by_mission = total_delayed_by_mission["currentlyOverdue"] + total_delayed_by_mission["completedOverdue"]
	try:
		determination = (1- (float(float(delayed_by_user)/float(total_delayed_by_mission))))
		return determination
	except:
		return (0)