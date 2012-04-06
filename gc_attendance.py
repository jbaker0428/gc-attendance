import csv
import shutil
import os
import sqlite3
import datetime

db = os.path.join(os.getcwd(), 'gc-attendance.sqlite')
conn = sqlite3.connect(db)

activeRoster = []

class Excuse:
	''' A Student's excuse for missing an Event sent to gc-excuse. ''' 
	def __init__(self, dt, r, s):
		self.excuseDate = dt	# a datetime object
		self.reason = r		# Student's message to gc-excuse
		self.student = s
	
class signIn:
	''' Corresponds to a row in the RFID output file. '''
	def __init__(self, dt, s):
		self.signDate = dt	# a datetime object
		self.student = s

class Student:
	''' A Student who has signed into the attendance system. '''
	def __init__(self, r, name, email, cred):
		self.rfid = r		# Numeric ID seen by the RFID reader
		self.name = name
		self.email = email
		self.shm = False
		self.officer = False
		self.signins = []
		self.excuses = []
		self.goodStanding = True
		self.credit = cred	# Taking GC for class credit

class Event:
	''' An event where attendance is taken. '''
	TYPE_REHEARSAL = 'Rehearsal'
	TYPE_DRESS = 'Dress Rehearsal'	# Mandatory for a concert
	TYPE_CONCERT = 'Concert'
	
	def __init__(self, dt, t):
		self.eventDate = dt	# a datetime object
		self.eventType = t	# One of the Event.TYPE_ constants 
		self.signins = []
		self.excuses = []