import csv
import shutil
import os
import sqlite3
import datetime

db = os.path.join(os.getcwd(), 'gc-attendance.sqlite')


activeRoster = []

def createTables():
	try:
		con = sqlite3.connect(db)
		cur = conn.cursor()
		
		cur.execute('''CREATE TABLE students
		(id INTEGER PRIMARY KEY, fname TEXT, lname TEXT, email TEXT, 
		shm INTEGER, officer INTEGER, goodstanding INTEGER, credit INTEGER)''')
		
		cur.execute('''CREATE TABLE excuses
		(dt TEXT, reason TEXT, student INTEGER,
		CONSTRAINT pk_excuse PRIMARY KEY (dt, student),
		CONSTRAINT fk_excuse_student FOREIGN KEY (student))''')
		
		cur.execute('''CREATE TABLE signins
		(dt TEXT, student INTEGER,
		CONSTRAINT pk_signin PRIMARY KEY (dt, student),
		CONSTRAINT fk_signin_student FOREIGN KEY (student))''')
		
		cur.execute('''CREATE TABLE events
		(dt TEXT, eventtype TEXT, student INTEGER,
		CONSTRAINT pk_event PRIMARY KEY (dt, eventtype))''')
		
	except:
		print 'createTables exception, probably because tables already created.'
		
	finally:
		cur.close()
		con.close()

class Student:
	''' A Student who has signed into the attendance system. 
	The student's RFID ID number is the primary key column.'''
	def __init__(self, r, fn, ln, email, shm=False, officer=False, standing=True, cred=False):
		self.rfid = r		# Numeric ID seen by the RFID reader
		self.fname = fn
		self.lname = ln
		self.email = email
		self.shm = shm
		self.officer = officer
		self.goodStanding = standing
		self.credit = cred	# Taking GC for class credit
		self.signins = []
		self.excuses = []
	
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