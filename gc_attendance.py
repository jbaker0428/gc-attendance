import csv
import shutil
import os
import sqlite3
import datetime

db = os.path.join(os.getcwd(), 'gc-attendance.sqlite')


active_roster = []

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
	
	@staticmethod
	def select_by_id(id):
		''' Return the Student of given ID. '''
		try:
			con = sqlite3.connect(db)
			cur = conn.cursor()
			
			symbol = (id,)
			cur.execute('SELECT * FROM students WHERE id=?', symbol)
			row = cur.fetchone()
			student = Student(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
			
		except:
			print 'Exception in Student.select_by_id( %s )' % id
			
		finally:
			cur.close()
			con.close()
			return student
	
	@staticmethod
	def select_by_name(fname='*', lname='*'):
		''' Return the Student(s) of given name. '''
		students = []
		try:
			con = sqlite3.connect(db)
			cur = conn.cursor()
			
			symbol = (fname, lname,)
			cur.execute('SELECT * FROM students WHERE fname=? AND lname=?', symbol)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
				students.append(student)
				
		except:
			print 'Exception in Student.select_by_name( %s, %s )' % fname, lname
			
		finally:
			cur.close()
			con.close()
			return students
	
	@staticmethod
	def select_by_email(email):
		''' Return the Student(s) with given email address. '''
		try:
			con = sqlite3.connect(db)
			cur = conn.cursor()
			
			symbol = (email,)
			cur.execute('SELECT * FROM students WHERE email=?', symbol)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
				students.append(student)
				
		except:
			print 'Exception in Student.select_by_email( %s )' % email
			
		finally:
			cur.close()
			con.close()
			return students
	
	@staticmethod
	def select_by_shm(shm):
		''' Return the list of Students in SHM (or not). '''
		try:
			con = sqlite3.connect(db)
			cur = conn.cursor()
			
			symbol = (int(shm),)
			cur.execute('SELECT * FROM students WHERE shm=?', symbol)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
				students.append(student)
				
		except:
			print 'Exception in Student.select_by_shm( %s )' % shm
			
		finally:
			cur.close()
			con.close()
			return students
	
	@staticmethod
	def select_by_officer(officer):
		''' Return the list of Students who are officers (or not). '''
		try:
			con = sqlite3.connect(db)
			cur = conn.cursor()
			
			symbol = (int(officer),)
			cur.execute('SELECT * FROM students WHERE officer=?', symbol)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
				students.append(student)
				
		except:
			print 'Exception in Student.select_by_officer( %s )' % officer
			
		finally:
			cur.close()
			con.close()
			return students
	
	@staticmethod
	def select_by_standing(good_standing):
		''' Return the list of Students of given standing. '''
		try:
			con = sqlite3.connect(db)
			cur = conn.cursor()
			
			symbol = (int(good_standing),)
			cur.execute('SELECT * FROM students WHERE goodstanding=?', symbol)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
				students.append(student)
				
		except:
			print 'Exception in Student.select_by_standing( %s )' % standing
			
		finally:
			cur.close()
			con.close()
			return students
	
	@staticmethod
	def select_by_credit(credit):
		''' Return the list of Students taking Glee Club for credit (or not). '''
		try:
			con = sqlite3.connect(db)
			cur = conn.cursor()
			
			symbol = (int(credit),)
			cur.execute('SELECT * FROM students WHERE goodstanding=?', symbol)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
				students.append(student)
				
		except:
			print 'Exception in Student.select_by_credit( %s )' % credit
		finally:
			cur.close()
			con.close()
			return students
	
	def __init__(self, r, fn, ln, email, shm=False, officer=False, standing=True, cred=False):
		self.rfid = r		# Numeric ID seen by the RFID reader
		self.fname = fn
		self.lname = ln
		self.email = email
		self.shm = shm
		self.officer = officer
		self.good_standing = standing
		self.credit = cred	# Taking GC for class credit
		self.signins = []
		self.excuses = []
		
	def __del__(self):
		self.delete()
	
	def fetch_signins(self):
		''' Fetch all Signins by this Student from the database. '''
		pass
	
	def fetch_excuses(self):
		''' Fetch all Excuses by this Student from the database. '''
		pass
	
	def update(self):
		''' Update an existing Student record in the DB. '''
		pass
	
	def insert(self):
		''' Write the Student to the DB. '''
		pass
	
	def delete(self):
		''' Delete the Student from the DB. '''
		pass
	
class Excuse:
	''' A Student's excuse for missing an Event sent to gc-excuse. 
	The datetime and student ID are the primary key colums.'''
	
	@staticmethod
	def select_by_student(id):
		''' Return the list of Excuses by a Student. '''
		try:
			con = sqlite3.connect(db)
			cur = conn.cursor()
			
			# Execute here
		except:
			print 'Exception in Excuse.select_by_student( %s )' % id
		finally:
			cur.close()
			con.close()
	
	@staticmethod
	def select_by_date(start_date, end_date):
		''' Return the list of Excuses in a given datetime range. '''
		try:
			con = sqlite3.connect(db)
			cur = conn.cursor()
			
			# Execute here
		except:
			print 'Exception in Excuse.select_by_date( %s, %s )' % start_date, end_date
		finally:
			cur.close()
			con.close()
	 
	def __init__(self, dt, r, s):
		self.excuse_date = dt	# a datetime object
		self.reason = r		# Student's message to gc-excuse
		self.student = s
	
	def __del__(self):
		self.delete()
		
	def update(self):
		''' Update an existing Excuse record in the DB. '''
		pass
	
	def insert(self):
		''' Write the Excuse to the DB. '''
		pass
	
	def delete(self):
		''' Delete the Excuse from the DB. '''
		pass
	
class Signin:
	''' Corresponds to a row in the RFID output file. 
	The datetime and student ID are the primary key colums.'''
	
	@staticmethod
	def select_by_student(id):
		''' Return the list of Signins by a Student. '''
		try:
			con = sqlite3.connect(db)
			cur = conn.cursor()
			
			# Execute here
		except:
			print 'Exception in Signin.select_by_student( %s )' % id
		finally:
			cur.close()
			con.close()
	
	@staticmethod
	def select_by_date(start_date, end_date):
		''' Return the list of Signins in a given datetime range. '''
		try:
			con = sqlite3.connect(db)
			cur = conn.cursor()
			
			# Execute here
		except:
			print 'Exception in Signin.select_by_date( %s, %s )' % start_date, end_date
		finally:
			cur.close()
			con.close()
	
	def __init__(self, dt, s):
		self.signin_date = dt	# a datetime object
		self.student = s
	
	def __del__(self):
		self.delete()
		
	def update(self):
		''' Update an existing Signin record in the DB. '''
		pass
	
	def insert(self):
		''' Write the Signin to the DB. '''
		pass
	
	def delete(self):
		''' Delete the Signin from the DB. '''
		pass

class Event:
	''' An event where attendance is taken. 
	The datetime is the primary key column.'''
	TYPE_REHEARSAL = 'Rehearsal'
	TYPE_DRESS = 'Dress Rehearsal'	# Mandatory for a concert
	TYPE_CONCERT = 'Concert'
	
	@staticmethod
	def select_by_date(start_date, end_date):
		''' Return the list of Events in a given datetime range. '''
		try:
			con = sqlite3.connect(db)
			cur = conn.cursor()
			
			# Execute here
		except:
			print 'Exception in Event.select_by_date( %s, %s )' % start_date, end_date
		finally:
			cur.close()
			con.close()
	
	@staticmethod
	def select_by_type(type):
		''' Return the list of Events of a given type. '''
		try:
			con = sqlite3.connect(db)
			cur = conn.cursor()
			
			# Execute here
		except:
			print 'Exception in Signin.select_by_type( %s )' % type
		finally:
			cur.close()
			con.close()
	
	def __init__(self, dt, t):
		self.event_date = dt	# a datetime object, primary key
		self.event_type = t	# One of the Event.TYPE_ constants 
		self.signins = []
		self.excuses = []
	
	def __del__(self):
		self.delete()
		
	def fetch_signins(self):
		''' Fetch all Signins for this Event from the database. '''
		pass
	
	def fetch_excuses(self):
		''' Fetch all Excuses for this Event from the database. '''
		pass
	
	def update(self):
		''' Update an existing Event record in the DB. '''
		pass
	
	def insert(self):
		''' Write the Event to the DB. '''
		pass
	
	def delete(self):
		''' Delete the Event from the DB. '''
		pass
	