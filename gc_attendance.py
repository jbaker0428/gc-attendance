import csv
import shutil
import os
import sqlite3
import datetime


active_roster = []
class AttendanceDB:
	''' Base class for the attendance database. '''
	db0 = os.path.join(os.getcwd(), 'gc-attendance.sqlite')
	
	def __init__(self, db=db0):
		self.db = db
	
	def con_cursor(self):
		''' Connect to the DB, enable foreign keys, and return a 
		(connection, cursor) pair. '''
		con = sqlite3.connect(self.db)
		con.execute('PRAGMA foreign_keys = ON')
		cur = con.cursor()
		return (con, cur)
	
	def createTables(self):
		''' Create the database tables. '''
		try:
			(con, cur) = self.con_cursor()
			# TODO: Add error handling clauses to the foreign key constraints
			cur.execute('''CREATE TABLE IF NOT EXISTS students
			(id INTEGER PRIMARY KEY, 
			fname TEXT, 
			lname TEXT, 
			email TEXT, 
			shm INTEGER, 
			goodstanding INTEGER, 
			credit INTEGER, 
			current INTEGER)''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS absences
			(student INTEGER REFERENCES students(id), 
			type TEXT, 
			eventdt TEXT REFERENCES events(dt), 
			excuseid TEXT REFERENCES excuses(id), 
			CONSTRAINT pk_absence PRIMARY KEY (eventdt, student))''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS excuses
			(id INTGER PRIMARY KEY
			dt TEXT, 
			reason TEXT, 
			student INTEGER REFERENCES students(id))''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS signins
			(dt TEXT, 
			student INTEGER,
			CONSTRAINT pk_signin PRIMARY KEY (dt, student),
			CONSTRAINT fk_signin_student FOREIGN KEY (student))''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS events
			(eventname TEXT, 
			dt TEXT PRIMARY KEY, 
			eventtype TEXT)''')
			
		except:
			print 'createTables exception, probably because tables already created.'
			
		finally:
			cur.close()
			con.close()

gcdb = AttendanceDB()

class Student:
	''' A Student who has signed into the attendance system. 
	The student's RFID ID number is the primary key column.'''
	
	@staticmethod
	def select_by_id(id):
		''' Return the Student of given ID. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
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
			(con, cur) = gcdb.con_cursor()
			
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
		students = []
		try:
			(con, cur) = gcdb.con_cursor()
			
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
		students = []
		try:
			(con, cur) = gcdb.con_cursor()
			
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
	def select_by_standing(good_standing):
		''' Return the list of Students of given standing. '''
		students = []
		try:
			(con, cur) = gcdb.con_cursor()
			
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
		students = []
		try:
			(con, cur) = gcdb.con_cursor()
			
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
	
	@staticmethod
	def select_by_current(current):
		''' Return the list of current Students on the roster (or not). '''
		students = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (int(credit),)
			cur.execute('SELECT * FROM students WHERE current=?', symbol)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
				students.append(student)
				
		except:
			print 'Exception in Student.select_by_current( %s )' % current
			
		finally:
			cur.close()
			con.close()
			return students
		
	@staticmethod
	def select_by_all(id='*', fname='*', lname='*', email='*', shm='*', standing='*', credit='*', current='*'):
		''' Return a list of Students using any combination of filters. '''
		if shm != '*':
			shm = int(shm)
			
		if standing != '*':
			standing = int(standing)
			
		if credit != '*':
			credit = int(credit)
		
		if current != '*':
			current = int(current)
			
		students = []
		
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (id, fname, lname, email, shm, standing, credit, current,)
			cur.execute('''SELECT * FROM students WHERE id=? INTERSECT 
			SELECT * FROM students WHERE fname=? INTERSECT 
			SELECT * FROM students WHERE lname=? INTERSECT 
			SELECT * FROM students WHERE email=? INTERSECT 
			SELECT * FROM students WHERE shm=? INTERSECT 
			SELECT * FROM students WHERE goodstanding=? INTERSECT 
			SELECT * FROM students WHERE credit=? INTERSECT
			SELECT * FROM students WHERE current=?''', symbol)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
				students.append(student)
				
		except:
			print 'Exception in Student.select_by_all( %s, %s, %s, %s, %s, %s, %s, %s )' % id, fname, lname, email, shm, standing, credit, current
			
		finally:
			cur.close()
			con.close()
			return students
	
	@staticmethod
	def merge(old, new):
		''' Merge the records of one student into another, deleting the first.
		This should be used when a student replaces their ID card, as the new
		ID card will have a different RFID number. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (new.rfid, old.rfid, )
			cur.execute('UPDATE excuses SET student=? WHERE student=?', symbol)
			cur.execute('UPDATE signins SET student=? WHERE student=?', symbol)
			cur.execute('UPDATE absences SET student=? WHERE student=?', symbol)
			
			symbol = (old.rfid, )
			cur.execute('DELETE FROM students WHERE id=?', symbol)
		
		except:
			print 'Exception in Student.merge( %s, %s )' % old, new
			
		finally:
			cur.close()
			con.close()
			new.fetch_signins()
			new.fetch_excuses()
	
	def __init__(self, r, fn, ln, email, shm=False, standing=True, cred=False, current=True):
		self.rfid = r		# Numeric ID seen by the RFID reader
		self.fname = fn
		self.lname = ln
		self.email = email
		self.shm = shm
		self.good_standing = standing
		self.credit = cred	# Taking GC for class credit
		self.current = current # Set false when no longer in active roster
		self.signins = []
		self.excuses = []
		
	def __del__(self):
		self.delete()
	
	def fetch_signins(self):
		''' Fetch all Signins by this Student from the database. '''
		self.signins = Signin.select_by_student(self.rfid)
	
	def fetch_excuses(self):
		''' Fetch all Excuses by this Student from the database. '''
		self.excuses = Excuse.select_by_student(self.rfid)
	
	def update(self):
		''' Update an existing Student record in the DB. '''
		pass
	
	def insert(self):
		''' Write the Student to the DB. '''
		pass
	
	def delete(self):
		''' Delete the Student from the DB. '''
		pass

class Absence:
	''' An instance of a Student not singing into an Event.
	May or may not have an Excuse attached to it. '''
	TYPE_PENDING = "Pending"
	TYPE_EXCUSED = "Excused"
	TYPE_UNEXCUSED = "Unexcused"
	
	@staticmethod
	def select_by_student(student_id):
		''' Return the list of Absences by a Student. '''
		absences = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (student_id,)
			cur.execute('SELECT * FROM absences WHERE student=?', symbol)
			for row in cur.fetchall():
				absence = Absence(row[0], row[1], row[2], row[3])
				absences.append(absence)
				
		except:
			print 'Exception in Absence.select_by_student( %s )' % student_id
			
		finally:
			cur.close()
			con.close()
			return absences
	
	@staticmethod
	def select_by_type(absence_type):
		''' Return the list of Absences of a given ABSENCE.TYPE_. '''
		absences = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (absence_type,)
			cur.execute('SELECT * FROM absences WHERE type=?', symbol)
			for row in cur.fetchall():
				absence = Absence(row[0], row[1], row[2], row[3])
				absences.append(absence)
				
		except:
			print 'Exception in Absence.select_by_type( %s )' % absence_type
			
		finally:
			cur.close()
			con.close()
			return absences
		
	@staticmethod
	def select_by_event_dt(event_dt):
		''' Return the list of Absences of a given datetime. '''
		absences = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (isoformat(event_dt),)
			cur.execute('SELECT * FROM absences WHERE eventdt=?', symbol)
			for row in cur.fetchall():
				absence = Absence(row[0], row[1], row[2], row[3])
				absences.append(absence)
				
		except:
			print 'Exception in Absence.select_by_event_dt( %s )' % event_dt
			
		finally:
			cur.close()
			con.close()
			return absences
	
	@staticmethod
	def select_by_excuse(excuse_id):
		''' Return the list of Absences of a given excuse ID.
		Should only return one, but returning a list in case of
		data integrity issues related to Excuse-Event mis-assignment. '''
		absences = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (excuse_id,)
			cur.execute('SELECT * FROM absences WHERE excuseid=?', symbol)
			for row in cur.fetchall():
				absence = Absence(row[0], row[1], row[2], row[3])
				absences.append(absence)
				
		except:
			print 'Exception in Absence.select_by_excuse( %s )' % excuse_id
			
		finally:
			cur.close()
			con.close()
			return absences
	
	@staticmethod
	def select_by_all(student_id='*', absence_type='*', event_dt='*', excuse_id='*'):
		''' Return the list of Absences using any combination of filters. '''
		absences = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (student_id, absence_type, isoformat(event_dt), excuse_id,)
			cur.execute('''SELECT * FROM absences WHERE student=? INTERSECT
			SELECT * FROM absences WHERE type=? INTERSECT
			SELECT * FROM absences WHERE eventdt=? INTERSECT
			SELECT * FROM absences WHERE excuseid=?''', symbol)
			for row in cur.fetchall():
				absence = Absence(row[0], row[1], row[2], row[3])
				absences.append(absence)
				
		except:
			print 'Exception in Absence.select_by_all( %s, %s, %s, %s )' % student_id, absence_type, isoformat(event_dt), excuse_id
			
		finally:
			cur.close()
			con.close()
			return absences
	
	def __init__(self, student_id, t, event_dt, excuse_dt=None):
		self.student = student_id
		self.type = t		# An Absence.TYPE_ string constant
		self.event_dt = event_dt	# Get the actual event via dt lookup
		self.excuse_id = excuse_id
	
class Excuse:
	''' A Student's excuse for missing an Event sent to gc-excuse. 
	The datetime and student ID are the primary key colums.'''
	
	# Cutoffs for when students can email gc-excuse (relative to event start time)
	EXCUSES_OPENS = datetime.timedelta(-1, 0, 0, 0, 0, -18, 0)	# 1 day, 18 hours before
	EXCUSES_CLOSES = datetime.timedelta(0, 0, 0, 0, 0, 6, 0)	# 6 hours after
	
	@staticmethod
	def select_by_id(excuse_id):
		''' Return the Excuse of given unique ID. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (excuse_id,)
			cur.execute('SELECT * FROM excuses WHERE id=?', symbol)
			row = cur.fetchone()
			excuse = Excuse(row[0], row[1], row[2], row[3])
				
		except:
			print 'Exception in Excuse.select_by_student( %s )' % excuse_id
			
		finally:
			cur.close()
			con.close()
			return excuse
	
	@staticmethod
	def select_by_student(student_id):
		''' Return the list of Excuses by a Student. '''
		excuses = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (student_id,)
			cur.execute('SELECT * FROM excuses WHERE student=?', symbol)
			for row in cur.fetchall():
				excuse = Excuse(row[0], row[1], row[2], row[3])
				excuses.append(excuse)
				
		except:
			print 'Exception in Excuse.select_by_student( %s )' % student_id
			
		finally:
			cur.close()
			con.close()
			return excuses
	
	@staticmethod
	def select_by_datetime(start_dt, end_dt):
		''' Return the list of Excuses in a given datetime range. '''
		excuses = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (isoformat(start_dt), isoformat(end_dt),)
			cur.execute('SELECT * FROM excuses WHERE dt BETWEEN ? AND ?', symbol)
			for row in cur.fetchall():
				excuse = Excuse(row[0], row[1], row[2], row[3])
				excuses.append(excuse)
				
		except:
			print 'Exception in Excuse.select_by_datetime( %s, %s )' % isoformat(start_dt), isoformat(end_dt)
			
		finally:
			cur.close()
			con.close()
			return excuses
		
	@staticmethod
	def select_by_all(excuse_id='*', student_id='*', start_dt='*', end_dt='*'):
		''' Return a list of Excuses using any combination of filters. '''
		excuses = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (excuse_id, student_id, isoformat(start_dt), isoformat(end_dt),)
			cur.execute('''SELECT * FROM excuses WHERE id=? INTERSECT
			SELECT * FROM excuses WHERE student=? INTERSECT
			 SELECT * FROM excuses WHERE dt BETWEEN ? AND ?''', symbol)
			for row in cur.fetchall():
				excuse = Excuse(row[0], row[1], row[2], row[3])
				excuses.append(excuse)
				
		except:
			print 'Exception in Excuse.select_by_all( %s, %s, %s, %s )' % excuse_id, student_id, isoformat(start_dt), isoformat(end_dt)
			
		finally:
			cur.close()
			con.close()
			return excuses
	 
	def __init__(self, id, dt, r, s):
		self.id = id		# Unique primary key
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
		signins = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (id,)
			cur.execute('SELECT * FROM signins WHERE student=?', symbol)
			for row in cur.fetchall():
				signin = Signin(row[0], row[1])
				signins.append(signin)
				
		except:
			print 'Exception in Signin.select_by_student( %s )' % id
			
		finally:
			cur.close()
			con.close()
			return signins
	
	@staticmethod
	def select_by_datetime(start_dt, end_dt):
		''' Return the list of Signins in a given datetime range. '''
		signins = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (isoformat(start_dt), isoformat(end_dt),)
			cur.execute('SELECT * FROM signins WHERE dt BETWEEN ? AND ?', symbol)
			for row in cur.fetchall():
				signin = Signin(row[0], row[1])
				signins.append(signin)
				
		except:
			print 'Exception in Signin.select_by_datetime( %s, %s )' % isoformat(start_dt), isoformat(end_dt)
			
		finally:
			cur.close()
			con.close()
			return signins
	
	@staticmethod
	def select_by_all(id, start_dt, end_dt):
		''' Return a list of Signins using any combination of filters. '''
		signins = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (id, isoformat(start_dt), isoformat(end_dt),)
			cur.execute('''SELECT * FROM signins WHERE student=? INTERSECT
			 SELECT * FROM signins WHERE dt BETWEEN ? AND ?''', symbol)
			for row in cur.fetchall():
				signin = Signin(row[0], row[1])
				signins.append(signin)
				
		except:
			print 'Exception in Signin.select_by_all( %s, %s, %s )' % id, isoformat(start_dt), isoformat(end_dt)
			
		finally:
			cur.close()
			con.close()
			return signins
	
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
	
	# Cutoffs for when students can sign in (relative to event start time)
	ATTENDANCE_OPENS = datetime.timedelta(0, 0, 0, 0, -30, 0, 0)	# 30 minutes before
	ATTENDANCE_CLOSES = datetime.timedelta(0, 0, 0, 0, 30, 1, 0)	# 90 minutes after
	
	@staticmethod
	def select_by_name(name):
		''' Return the list of Events of a given name. '''
		events = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (name,)
			cur.execute('SELECT * FROM events WHERE eventname=?', symbol)
			for row in cur.fetchall():
				event = Event(row[0], row[1], row[2])
				events.append(event)
				
		except:
			print 'Exception in Signin.select_by_type( %s )' % type
			
		finally:
			cur.close()
			con.close()
			return events
	
	@staticmethod
	def select_by_datetime(start_dt, end_dt):
		''' Return the list of Events in a given datetime range. '''
		events = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (isoformat(start_dt), isoformat(end_dt),)
			cur.execute('SELECT * FROM events WHERE dt BETWEEN ? AND ?', symbol)
			for row in cur.fetchall():
				event = Event(row[0], row[1], row[2])
				events.append(event)
				
		except:
			print 'Exception in Event.select_by_datetime( %s, %s )' % isoformat(start_dt), isoformat(end_dt)
			
		finally:
			cur.close()
			con.close()
			return events
	
	@staticmethod
	def select_by_type(type):
		''' Return the list of Events of a given type. '''
		events = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (type,)
			cur.execute('SELECT * FROM events WHERE eventtype=?', symbol)
			for row in cur.fetchall():
				event = Event(row[0], row[1], row[2])
				events.append(event)
				
		except:
			print 'Exception in Signin.select_by_type( %s )' % type
			
		finally:
			cur.close()
			con.close()
			return events
	
	@staticmethod
	def select_by_all(name, start_dt, end_dt, type):
		''' Return a list of Events using any combination of filters. '''
		events = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (name, isoformat(start_dt), isoformat(end_dt), type,)
			cur.execute('''SELECT * FROM events WHERE eventname=? INTERSECT 
			SELECT * FROM events WHERE dt BETWEEN ? AND ? INTERSECT 
			SELECT * FROM events WHERE eventtype=?''', symbol)
			for row in cur.fetchall():
				event = Event(row[0], row[1], row[2])
				events.append(event)
				
		except:
			print 'Exception in Event.select_by_all( %s, %s, %s, %s )' % name, isoformat(start_dt), isoformat(end_dt), type
			
		finally:
			cur.close()
			con.close()
			return events
	
	def __init__(self, name, dt, t):
		self.event_name = name
		self.event_date = dt	# a datetime object, primary key
		self.event_type = t	# One of the Event.TYPE_ constants 
		self.signins = []
		self.excuses = []
	
	def __del__(self):
		self.delete()
		
	def fetch_signins(self):
		''' Fetch all Signins for this Event from the database. '''
		self.signins = Signin.select_by_datetime(self.event_date+Event.ATTENDANCE_OPENS, self.event_date+Event.ATTENDANCE_CLOSES)
	
	def fetch_excuses(self):
		''' Fetch all Excuses for this Event from the database. '''
		self.excuses = Excuse.select_by_datetime(self.event_date+Excuse.EXCUSES_OPENS, self.event_date+Excuse.EXCUSES_CLOSES)
	
	def update(self):
		''' Update an existing Event record in the DB. '''
		pass
	
	def insert(self):
		''' Write the Event to the DB. '''
		pass
	
	def delete(self):
		''' Delete the Event from the DB. '''
		pass
	