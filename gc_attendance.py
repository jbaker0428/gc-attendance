import csv
import shutil
import os
import sqlite3
import datetime

db = os.path.join(os.getcwd(), 'gc-attendance.sqlite')
conn = sqlite3.connect(db)

activeRoster = []

class Excuse:
	''' A Student's excuse for missing an Event sent to gc-excuse. 
	The datetime and student ID are the primary key colums.''' 
	def __init__(self, dt, r, s):
		self.excuseDate = dt	# a datetime object
		self.reason = r		# Student's message to gc-excuse
		self.student = s
	
class signIn:
	''' Corresponds to a row in the RFID output file. 
	The datetime and student ID are the primary key colums.'''
	def __init__(self, dt, s):
		self.signDate = dt	# a datetime object
		self.student = s

class Student:
	''' A Student who has signed into the attendance system. 
	The student's RFID ID number is the primary key column.'''
	def __init__(self, r, fn, ln, email, shm=False, officer=False, cred=False):
		self.rfid = r		# Numeric ID seen by the RFID reader
		self.fname = fn
		self.lname = ln
		self.email = email
		self.shm = shm
		self.officer = officer
		self.signins = []
		self.excuses = []
		self.goodStanding = True
		self.credit = cred	# Taking GC for class credit

class Event:
	''' An event where attendance is taken. 
	The datetime is the primary key column.'''
	TYPE_REHEARSAL = 'Rehearsal'
	TYPE_DRESS = 'Dress Rehearsal'	# Mandatory for a concert
	TYPE_CONCERT = 'Concert'
	
	def __init__(self, dt, t):
		self.eventDate = dt	# a datetime object, primary key
		self.eventType = t	# One of the Event.TYPE_ constants 
		self.signins = []
		self.excuses = []