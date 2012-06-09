import csv
import shutil
import os
from datetime import *
import types

import xlsx
import apsw
# Google stuff
import httplib2
from apiclient.discovery import build
from apiclient.errors import *
from oauth2client.file import Storage
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import flow_from_clientsecrets
from oauth2client.tools import run

class GCal(object):
	
	"""Container class for Google Calendar API-related objects."""
	
	__slots__ = ["credentials", "http", "service"]
	
	@staticmethod
	def get_credentials(credentials_file='credentials.dat'):
		storage = Storage(credentials_file)
		credentials = storage.get()
		
		if credentials is None or credentials.invalid == True:
			flow = flow_from_clientsecrets('client_secrets.json', scope='https://www.googleapis.com/auth/calendar')
		
		credentials = run(flow, storage)
		return credentials
	
	def __init__(self, credentials_file='credentials.dat'):
		self.credentials = GCal.get_credentials(credentials_file)
		self.http = credentials.authorize(httplib2.Http(cache=".cache"))
		self.service = build("calendar", "v3", http=self.http)

class AttendanceDB(object):
	
	"""Base class for the attendance database."""
	
	db0 = os.path.join(os.getcwd(), 'gc-attendance.sqlite')
	__slots__ = ["disk_db", "memory"]
	
	def __init__(self, db_file=db0):
		self.disk_db = db_file
		self.memory = self.connect(":memory:")
	
	def connect(self, db):
		"""Connect to the DB, enable foreign keys, and return the opened connection."""
		
		con = apsw.Connection(db)
		cur = con.cursor()
		cur.execute('PRAGMA foreign_keys = ON')
		cur.close()
		return con
	
	def create_tables(self, connection):
		"""Create the database tables."""
		
		try:
			cur = connection.cursor()
			cur.execute('''CREATE TABLE IF NOT EXISTS students
			(id INTEGER PRIMARY KEY, 
			fname TEXT, 
			lname TEXT, 
			email TEXT, 
			goodstanding INTEGER, 
			current INTEGER)''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS organizations 
			(name TEXT PRIMARY KEY, 
			gcal_id STRING UNIQUE)''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS optional_member_orgs 
			(id INTEGER PRIMARY KEY, 
			parent TEXT NOT NULL REFERENCES organizations(name) ON DELETE CASCADE ON UPDATE CASCADE, 
			child TEXT NOT NULL REFERENCES organizations(name) ON DELETE CASCADE ON UPDATE CASCADE, 
			UNIQUE(parent ASC, child) ON CONFLICT IGNORE)''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS mandatory_member_orgs 
			(id INTEGER PRIMARY KEY, 
			parent TEXT NOT NULL REFERENCES organizations(name) ON DELETE CASCADE ON UPDATE CASCADE, 
			child TEXT NOT NULL REFERENCES organizations(name) ON DELETE CASCADE ON UPDATE CASCADE, 
			UNIQUE(parent ASC, child) ON CONFLICT IGNORE)''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS groups 
			(id INTEGER PRIMARY KEY, 
			organization TEXT REFERENCES organizations(name) ON DELETE CASCADE ON UPDATE CASCADE, 
			semester TEXT NOT NULL REFERENCES semesters(name) ON DELETE CASCADE ON UPDATE CASCADE, 
			UNIQUE(organization ASC, semester ASC) ON CONFLICT IGNORE )''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS group_memberships 
			(id INTEGER PRIMARY KEY, 
			student INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE ON UPDATE CASCADE, 
			group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE ON UPDATE CASCADE, 
			credit INTEGER NOT NULL, 
			UNIQUE(student ASC, group_id ASC) ON CONFLICT REPLACE)''')

			cur.execute('''CREATE TABLE IF NOT EXISTS absences
			(student INTEGER REFERENCES students(id) ON DELETE CASCADE ON UPDATE CASCADE, 
			type TEXT, 
			event INTEGER REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE, 
			excuseid TEXT REFERENCES excuses(id) ON DELETE CASCADE ON UPDATE CASCADE, 
			CONSTRAINT pk_absence PRIMARY KEY (event, student))''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS excuses
			(id INTGER PRIMARY KEY, 
			dt TEXT, 
			event INTEGER REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE,
			reason TEXT, 
			student INTEGER REFERENCES students(id) ON DELETE CASCADE ON UPDATE CASCADE)''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS signins
			(dt TEXT, 
			event INTEGER REFERENCES events(id) ON DELETE CASCADE ON UPDATE CASCADE,
			student INTEGER REFERENCES students(id) ON DELETE CASCADE ON UPDATE CASCADE, 
			CONSTRAINT pk_signin PRIMARY KEY (dt, student), 
			UNIQUE(event ASC, student ASC), 
			UNIQUE(student ASC, dt ASC) )''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS events
			(id INTEGER PRIMARY KEY, 
			eventname TEXT NOT NULL, 
			description TEXT, 
			location TEXT, 
			start TEXT NOT NULL, 
			end TEXT NOT NULL, 
			eventtype TEXT, 
			group_id INTEGER REFERENCES groups(id) ON DELETE CASCADE ON UPDATE CASCADE, 
			semester TEXT REFERENCES semesters(name) ON DELETE CASCADE ON UPDATE CASCADE,
			gcal_id TEXT UNIQUE, 
			UNIQUE(group_id ASC, start ASC) )''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS terms
			(name TEXT PRIMARY KEY,
			startdate TEXT UNIQUE,
			enddate TEXT UNIQUE)''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS semesters
			(name TEXT PRIMARY KEY,
			termone TEXT REFERENCES terms(name) ON DELETE RESTRICT ON UPDATE CASCADE,
			termtwo TEXT REFERENCES terms(name) ON DELETE RESTRICT ON UPDATE CASCADE)''')
			
			# Days where WPI closed (holidays, snow days, etc)
			cur.execute('CREATE TABLE IF NOT EXISTS daysoff (date TEXT PRIMARY KEY)')
			
		finally:
			cur.close()
	
	def read_attendance(self, infile):
		"""Parse the attendance record spreadsheet and write to the database."""
		
		try:
			signins = []
			with open(infile, 'rb') as f:
				reader = csv.reader(f, delimiter=',')
				for row in reader:
					# row[0] is the mystery blank column
					# row[1] is the date MM/D/YYYY
					# row[2] is the 24-hour time HH:MM 
					# row[3] is the RFID number
					date = row[1].split('/')
					time = row[2].split(':')
					dt = datetime.datetime(int(date[2]), int(date[0]), int(date[1]), int(time[0]), int(time[1]))
					# The record variable formatting matches the Sigin.__init__ arguments list
					record = (dt, None, int(row[3]))
					signins.append(record)
			
			cur = self.memory.cursor()
			
			for t in signins:
				# Check that student ID is in DB, if not, create a blank entry
				if Student.select_by_id(t[3]) is None:
					new_student = Student(t[3], None, None, None)
					print 'Adding unknown member to database, RFID# = ', t[3]
					new_student.insert(self.memory)
			cur.executemany('INSERT OR ABORT INTO signins VALUES (?,?,?)', signins)
			
		finally:
			cur.close()

gcdb = AttendanceDB()

class RosterException(Exception):
	
	"""Exception raised when something goes wrong parsing a roster spreadsheet."""
	
	__slots__ = ["source", "text"]
	
	def __init__(self, source, text):
		self.source = source
		self.text = text
	
	def __str__(self):
		return repr(self.text)

class DatabaseException(Exception):
	
	"""Exception raised when a SQL operation executed OK, but returned unexpected results."""
	
	__slots__ = ["source", "text"]
	
	def __init__(self, source, text):
		self.source = source
		self.text = text
	
	def __str__(self):
		return repr(self.text)

class Term(object):
	
	"""Corresponds to one 7-week term on WPI's academic calendar."""
	
	__slots__ = ["name", "start_date", "end_date", "days_off"]
	
	@classmethod
	def new_from_row(cls, row):
		"""Given a terms row from the DB, returns a Term object."""
		
		return cls(row[0], convert_date(row[1]), convert_date(row[2]))
	
	@staticmethod
	def select_by_name(name, connection):
		"""Return the Term of given name."""
		
		try:
			cur = connection.cursor()
			term = None
			rows = list(cur.execute('SELECT * FROM terms WHERE name=?', (name,)))
			if len(rows) > 1 or len(rows) < 0:
				raise DatabaseException(Term.select_by_name.__name__, "Query returned %s rows, expected one." % len(rows))
			elif len(rows) == 1:
				#print 'No term row found!'
				term = Term.new_from_row(rows[0], connection)
			elif len(rows) == 0:
				#print 'No term rows found'
				term = None
	
		finally:
			cur.close()
			return term
	
	@staticmethod
	def select_by_date(start_date, end_date, connection):
		"""Return the list of Terms in a given datetime range. 
		
		Any Term whose startdate or enddate column falls within the
		given range will be returned.
		"""
		
		terms = []
		
		if type(start_date == date):
			start_date = start_date.isoformat()
		if type(end_date == date):
			end_date = end_date.isoformat()
		
		try:
			cur = connection.cursor()
			sql = 'SELECT * FROM terms WHERE startdate BETWEEN ?1 AND ?2 UNION SELECT * FROM terms WHERE enddate BETWEEN ?1 AND ?2'
			params = (start_date, end_date,)
			for row in cur.execute(sql, params):
				terms.append(Term.new_from_row(row, connection))
				
		finally:
			cur.close()
			return terms
	
	@staticmethod
	def select_by_all(name, start_date, end_date, connection):
		"""Return a list of Terms using any combination of filters."""
		
		terms = []
		
		if type(start_date == date):
			start_date = start_date.isoformat()
		if type(end_date == date):
			end_date = end_date.isoformat()
			
		try:
			cur = connection.cursor()
			sql = '''SELECT * FROM terms WHERE startdate BETWEEN ?1 AND ?2 UNION
			SELECT * FROM terms WHERE enddate BETWEEN ?1 AND ?2 INTERSECT
			SELECT * FROM terms WHERE name=?3'''
			params = (start_date, end_date, name,)
			for row in cur.execute(sql, params):
				terms.append(Term.new_from_row(row, connection))
				
		finally:
			cur.close()
			return terms
	
	def __init__(self, name, start_date, end_date, days_off=[]):
		self.name = name				# Something like "A09", "D12", etc. Primary key.
		self.start_date = start_date	# A date object
		self.end_date = end_date		# A date object
		self.days_off = []	# A list of dates that class is cancelled (holidays, snow days, etc)
		for d in days_off:
			self.days_off.append(d)	
	
	def fetch_days_off(self, connection):
		"""Fetch all daysoff table entries for this Term from the database.
		 
		Returns a list of date objects.
		"""
		
		result = []
		try:
			if type(self.start_date == date):
				start = self.start_date.isoformat()
			elif type(self.start_date == str):
				start = self.start_date
			else:
				raise TypeError
				
			if type(self.end_date == date):
				end = self.end_date.isoformat()
			elif type(self.end_date == str):
				end = self.end_date
			else:
				raise TypeError
		
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM daysoff WHERE date BETWEEN ? AND ?', (start, end,)):
				result.append(convert_date(row[0]))
				
		finally:
			cur.close()
			return result
			
	def update(self, connection):
		"""Update an existing Term record in the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.name, self.start_date.isoformat(), self.end_date.isoformat(),)
			cur.execute('UPDATE terms SET name=?1, startdate=?2, enddate=?3 WHERE name=?1', params)
				
		finally:
			cur.close()
	
	def insert(self, connection):
		"""Write the Term to the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.name, self.start_date.isoformat(), self.end_date.isoformat(), )
			cur.execute('INSERT INTO terms VALUES (?,?,?)', params)
				
		finally:
			cur.close()
	
	def delete(self, connection):
		"""Delete the Term from the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.name,)
			cur.execute('DELETE FROM terms WHERE name=?', params)
				
		finally:
			cur.close()

class Semester(object):
	
	"""Corresponds to one 2-term semester on WPI's academic calendar."""
	
	__slots__ = ["name", "term_one", "term_two"]
	
	@classmethod
	def new_from_row(cls, row, connection):
		"""Given a semester row from the DB, returns a Semester object."""
		
		t1 = Term.select_by_name(row[1], connnection)
		t2 = Term.select_by_name(row[2], connnection)
		semester = cls(row[0], t1, t2)
				
	@staticmethod
	def select_by_name(name, connection):
		"""Return the Semester of given name."""
		
		try:
			cur = connection.cursor()
			
			params = (name,)
			rows = list(cur.execute('SELECT * FROM semester WHERE name=?', params))
			if len(rows) > 1 or len(rows) < 0:
				raise DatabaseException(Semester.select_by_name.__name__, "Query returned %s rows, expected one." % len(rows))
			elif len(rows) == 1:
				semester = Semester.new_from_row(rows[0], connection)
			elif len(rows) == 0:
				semester = None
				
		finally:
			cur.close()
			return semester
	
	@staticmethod
	def select_by_date(start_date, end_date, connection):
		"""Return the list of Semesters in a given datetime range. 
		
		Any Semester whose startdate or enddate falls within the
		given range will be returned.
		"""
		
		semesters = []
		if type(start_date == date):
			start_date = start_date.isoformat()
		if type(end_date == date):
			end_date = end_date.isoformat()
			
		try:
			cur = connection.cursor()
			
			sql = '''
			SELECT * from semesters WHERE termone IN 
			(SELECT name FROM terms WHERE startdate BETWEEN ?1 AND ?2 UNION 
			SELECT name FROM terms WHERE enddate BETWEEN ?1 AND ?2) 
			UNION SELECT * from semesters WHERE termtwo IN 
			(SELECT name FROM terms WHERE startdate BETWEEN ?1 AND ?2 UNION 
			SELECT name FROM terms WHERE enddate BETWEEN ?1 AND ?2)
			 '''
			
			for row in cur.execute(sql, (start_date, end_date,)):
				semesters.append(Semester.new_from_row(row, connection))
							
		finally:
			cur.close()
			return semesters
	
	@staticmethod
	def select_by_all(name, start_date, end_date, connection):
		"""Return a list of Semesters using any combination of filters."""
		
		semesters = []
		
		if type(start_date == date):
			start_date = start_date.isoformat()
		if type(end_date == date):
			end_date = end_date.isoformat()
			
		try:
			cur = connection.cursor()
			
			sql = '''SELECT * from semesters WHERE termone IN 
			(SELECT name FROM terms WHERE startdate BETWEEN ?1 AND ?2 UNION 
			SELECT name FROM terms WHERE enddate BETWEEN ?1 AND ?2) 
			UNION SELECT * from semesters WHERE termtwo IN 
			(SELECT name FROM terms WHERE startdate BETWEEN ?1 AND ?2 UNION 
			SELECT name FROM terms WHERE enddate BETWEEN ?1 AND ?2) INTERSECT 
			SELECT * FROM terms WHERE name=?3'''
			params = (start_date, end_date, name,)
			
			for row in cur.execute(sql, params):
				semesters.append(Semester.new_from_row(row, connection))
				
		finally:
			cur.close()
			return semesters
	
	def __init__(self, name, term_one, term_two):
		self.name = name			# Something like "Fall 2011". Primary key.
		self.term_one = term_one	# A or C term
		self.term_two = term_two	# B or D term
		
	def fetch_days_off(self, connection):
		"""Fetch all daysoff table entries for this Semester from the database. 
		
		Returns a list of date objects.
		"""
		
		result = []
		try:
			if type(self.term_one.start_date == date):
				start = self.term_one.start_date.isoformat()
			elif type(self.term_one.start_date == str):
				start = self.term_one.start_date
			else:
				raise TypeError
				
			if type(self.term_two.end_date == date):
				end = self.term_two.end_date.isoformat()
			elif type(self.term_two.end_date == str):
				end = self.term_two.end_date
			else:
				raise TypeError
		
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM daysoff WHERE date BETWEEN ? AND ?', (start, end,)):
				result.append(convert_date(row[0]))
				
		finally:
			cur.close()
			return result
			
	def update(self, connection):
		"""Update an existing Semester record in the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.name, self.term_one.name, self.term_two.name,)
			cur.execute('UPDATE semesters SET name=?1 termone=?2, termtwo=?3 WHERE name=?1', params)
				
		finally:
			cur.close()
	
	def insert(self, connection):
		"""Write the Semester to the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.name, self.term_one.name, self.term_two.name,)
			cur.execute('INSERT INTO semesters VALUES (?,?,?)', params)
				
		finally:
			cur.close()
	
	def delete(self, connection):
		"""Delete the Semester from the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.name,)
			cur.execute('DELETE FROM semesters WHERE name=?', params)
				
		finally:
			cur.close()

class Student(object):
	
	"""A Student who has signed into the attendance system. 
	
	The student's RFID ID number is the primary key column.
	"""

	@classmethod
	def new_from_row(cls, row):
		"""Given a students row from the DB, returns a Student object."""
		
		return cls(row[0], row[1], row[2], row[3], row[4], row[5])

	@staticmethod
	def select_by_id(id, connection):
		"""Return the Student of given ID."""
		
		try:
			cur = connection.cursor()
			
			rows = list(cur.execute('SELECT * FROM students WHERE id=?', (id,)))
			if len(rows) > 1 or len(rows) < 0:
				raise DatabaseException(Student.select_by_id.__name__, "Query returned %s rows, expected one." % len(rows))
			elif len(rows) == 1:
				student = Student.new_from_row(rows[0], connection)
			elif len(rows) == 0:
				student = None
			
		finally:
			cur.close()
			return student
	
	@staticmethod
	def select_by_name(fname, lname, connection):
		"""Return the Student(s) of given name."""
		
		students = []
		try:
			cur = connection.cursor()
			for row in cur.execute('SELECT * FROM students WHERE fname=? AND lname=?', (fname, lname,)):
				students.append(Student.new_from_row(row, connection))
				
		finally:
			cur.close()
			return students
	
	@staticmethod
	def select_by_email(email, connection):
		"""Return the Student(s) with given email address."""
		
		students = []
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM students WHERE email=?', (email,)):
				students.append(Student.new_from_row(row, connection))
				
		finally:
			cur.close()
			return students
	
	@staticmethod
	def select_by_standing(good_standing, connection):
		"""Return the list of Students of given standing."""
		
		students = []
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM students WHERE goodstanding=?', (int(good_standing),)):
				students.append(Student.new_from_row(row, connection))
				
		finally:
			cur.close()
			return students
	
	@staticmethod
	def select_by_group(group, in_group, connection):
		"""Return the list of Students in some group (or not)."""
		
		students = []
		try:
			cur = connection.cursor()
			
			if in_group == True:
				sql = '''SELECT * FROM students WHERE id IN
				(SELECT DISTINCT student FROM group_memberships WHERE id=?)'''
			else:
				sql = '''SELECT * FROM students WHERE id NOT IN
				(SELECT DISTINCT student FROM group_memberships WHERE id=?)'''

			for row in cur.execute(sql, (group.id,)):
				students.append(Student.new_from_row(row, connection))
				
		finally:
			cur.close()
			return students
	
	@staticmethod
	def select_by_current(current, connection):
		"""Return the list of current Students on the roster (or not)."""
		
		students = []
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM students WHERE current=?', (int(current),)):
				students.append(Student.new_from_row(row, connection))
				
		finally:
			cur.close()
			return students
		
	@staticmethod
	def select_by_all(id, fname, lname, email, standing, current, connection):
		"""Return a list of Students using any combination of filters."""
		
		standing = int(standing)
			
		current = int(current)
			
		students = []
		
		try:
			cur = connection.cursor()
			
			sql = 'SELECT * FROM students WHERE id=? AND fname=? AND lname=? AND email=? AND goodstanding=? AND current=?'
			params = (id, fname, lname, email, standing, current,)
			for row in cur.execute(sql, params):
				students.append(Student.new_from_row(row, connection))
				
		finally:
			cur.close()
			return students
	
	@staticmethod
	def merge(old, new, connection):
		"""Merge the records of one student into another, deleting the first.
		
		This should be used when a student replaces their ID card, as the new
		ID card will have a different RFID number.
		"""
		
		try:
			cur = connection.cursor()
			
			params = (new.rfid, old.rfid, )
			cur.execute('UPDATE excuses SET student=? WHERE student=?', params)
			cur.execute('UPDATE signins SET student=? WHERE student=?', params)
			cur.execute('UPDATE absences SET student=? WHERE student=?', params)
			
			old.delete()
		
		finally:
			cur.close()
			new.fetch_signins(db, connection)
			new.fetch_excuses(db, connection)
	
	def __init__(self, r, fn, ln, email, standing=True, current=True):
		self.rfid = r		# Numeric ID seen by the RFID reader
		self.fname = fn
		self.lname = ln
		self.email = email
		self.good_standing = standing
		self.current = current # Set false when no longer in active roster
		self.signins = []
		self.excuses = []
		self.absences = []
		self.groups = []
		
	def fetch_signins(self, connection):
		"""Fetch all Signins by this Student from the database."""
		
		self.signins = Signin.select_by_student(self, connection)
	
	def fetch_excuses(self, connection):
		"""Fetch all Excuses by this Student from the database."""
		
		self.excuses = Excuse.select_by_student(self, connection)
	
	def fetch_absences(self, connection):
		"""Fetch all Absences by this Student from the database."""
		
		self.absences = Absence.select_by_student(self, connection)
	
	def fetch_groups(self, connection):
		"""Fetch all Groups this Student is a member of from the database."""
		
		try:
			cur = connection.cursor()
			
			params = (self.name,)
			cur.execute('''SELECT * FROM groups WHERE name IN
					(SELECT DISTINCT group FROM group_memberships WHERE student=?)''', params)
			rows = cur.fetchall()
			for row in rows:
				group = Group(row[0], row[1], row[2])
				group.fetch_members(connection)
				self.groups.append(group)
			
		finally:
			cur.close()
	
	def join_group(self, group, credit, connection):
		"""Add the Student to a Group."""
		
		try:
			cur = connection.cursor()
			
			params = (self.name, group.id, int(credit))
			cur.execute('INSERT INTO group_memberships VALUES (?,?,?)', params)
				
		finally:
			cur.close()
			self.groups.append(group)
	
	def leave_group(self, group, connection):
		"""Remove the Student from a Group."""
		
		try:
			cur = connection.cursor()
			
			params = (self.name, group.id,)
			cur.execute('DELETE FROM group_memberships WHERE student=? AND group=?', params)
			del self.groups[self.groups.index(group)]
				
		finally:
			cur.close()
	
	def update(self, connection):
		"""Update an existing Student record in the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.fname, self.lname, self.email, self.good_standing, self.current, self.rfid,)
			cur.execute('''UPDATE students 
			SET fname=?, lname=?, email=?, goodstanding=?, current=? 
			WHERE id=?''', params)
				
		finally:
			cur.close()
	
	def insert(self, connection):
		"""Write the Student to the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.rfid, self.fname, self.lname, self.email, self.good_standing, self.current,)
			cur.execute('INSERT INTO students VALUES (?,?,?,?,?,?)', params)
				
		finally:
			cur.close()
	
	def delete(self, connection):
		"""Delete the Student from the DB."""
		
		try:
			cur = connection.cursor()
			
			cur.execute('DELETE FROM students WHERE id=?', (self.rfid,))
				
		finally:
			cur.close()

class Organization(object):
	
	"""An organization that uses the RFID reader for attendance."""

	@classmethod
	def new_from_row(cls, row):
		"""Given an organization row from the DB, returns an Organization object."""
		
		return cls(row[0], row[1])
	
	@staticmethod
	def select_by_name(name, connection):
		"""Return the Organization of given name."""
		
		try:
			cur = connection.cursor()
			
			rows = list(cur.execute('SELECT * FROM organizations WHERE name=?', (name,)))
			if len(rows) > 1 or len(rows) < 0:
				raise DatabaseException(Organization.select_by_name.__name__, "Query returned %s rows, expected one." % len(rows))
			elif len(rows) == 1:
				organization = Organization.new_from_row(rows[0])
			elif len(rows) == 0:
				organization = None
				
		finally:
			cur.close()
			return organization

	def __init__(self, name, gcal_id=None):
		self.name = name
		self.calendar = {}	
		# Optional Google Calendar ID, e.g. wpigleeclub@gmail.com
		if gcal_id is not None:
			self.calendar['id'] = gcal_id
		# Organizations whose members are granted "attendance optional" status
		# for this Organization's events (if they are even on the roster)
		self.optional_member_orgs = []
		# Similar to the above, but attendance is mandatory for members
		# of organizations on this list
		self.mandatory_member_orgs = []
	
	def fetch_optional_member_orgs(self, connection):
		"""Fetch all optional member Organizations from the database."""
		
		try:
			orgs = []
			cur = connection.cursor()
			
			sql = '''SELECT * FROM organizations WHERE name IN 
				(SELECT child FROM optional_member_orgs WHERE parent=?)'''
			for row in cur.execute(sql, (self.name,)):
				orgs.append(Organization.new_from_row(row))
				
		finally:
			cur.close()
			self.optional_member_orgs = orgs
	
	def add_optional_member_org(self, org, connection):
		"""Add an optional member Organization relationship."""
		
		try:
			cur = connection.cursor()
			
			cur.execute('INSERT INTO optional_member_orgs VALUES (NULL,?,?)', (self.name, org.name,))
			self.optional_member_orgs.append(org)
				
		finally:
			cur.close()
	
	def remove_optional_member_org(self, org, connection):
		"""Remove an optional member Organization relationship."""
		
		try:
			cur = connection.cursor()
			
			cur.execute('DELETE FROM optional_member_orgs WHERE parent=? AND child=?', (self.name, org.name,))
			del self.optional_member_orgs[self.optional_member_orgs.index(org)]
				
		finally:
			cur.close()
		
			
	def fetch_mandatory_member_orgs(self, connection):
		"""Fetch all mandatory member Organizations from the database."""
		
		try:
			orgs = []
			cur = connection.cursor()
			
			sql = '''SELECT * FROM organizations WHERE name IN 
				(SELECT child FROM mandatory_member_orgs WHERE parent=?)'''
			for row in cur.execute(sql, (self.name,)):
				orgs.append(Organization.new_from_row(row))
				
		finally:
			cur.close()
			self.mandatory_member_orgs = orgs
			
	def add_mandatory_member_org(self, org, connection):
		"""Add a mandatory member Organization relationship."""
		
		try:
			cur = connection.cursor()
			
			cur.execute('INSERT INTO mandatory_member_orgs VALUES (NULL,?,?)', (self.name, org.name,))
			self.mandatory_member_orgs.append(org)
				
		finally:
			cur.close()
	
	def remove_mandatory_member_org(self, org, connection):
		"""Remove a mandatory member Organization relationship."""
		
		try:
			cur = connection.cursor()
			
			cur.execute('DELETE FROM mandatory_member_orgs WHERE parent=? AND child=?', (self.name, org.name,))
			del self.mandatory_member_orgs[self.mandatory_member_orgs.index(org)]
				
		finally:
			cur.close()
		
	def get_calendar(self, gcal):
		"""Gets the Organization's Google calendar resource dictionary.
		
		@param gcal: A GCal instance.
		"""
		
		if 'id' in self.calendar:
			response = gcal.service.calendars().get(calendarId=self.calendar['id']).execute()
			self.calendar = response
	
	def update_calendar(self, gcal):
		"""Updates the Organization's Google calendar with local changes.
		
		@param gcal: A GCal instance.
		"""
		
		# Make sure the local copy is a full resource so we don't overwrite the
		# calendar with a blank dict
		if ('description', 'location', 'summary', 'timeZone',) in self.calendar:
			response = gcal.service.calendars().update(calendarId=self.calendar['id'], body=self.calendar).execute()
			self.calendar = response 
	
	def update(self, connection):
		"""Update an existing Organization record in the DB."""
		
		try:
			cur = connection.cursor()
			
			cur.execute('UPDATE organizations SET name=?1, gcal_id=?2 WHERE name=?1', (self.name, self.calendar['id'],))
				
		finally:
			cur.close()
	
	def insert(self, connection):
		"""Write the Organization to the DB."""
		
		try:
			cur = connection.cursor()
			
			cur.execute('INSERT INTO organizations VALUES (?,?)', (self.name, self.calendar['id'],))
				
		finally:
			cur.close()
	
	def delete(self, connection):
		"""Delete the Organization from the DB."""
		
		try:
			cur = connection.cursor()
			
			cur.execute('DELETE FROM organizations WHERE name=?', (self.name,))
				
		finally:
			cur.close()

class Group(object):
	
	"""A group of students. 
	
	This is usually an ensemble's roster for a semester. 
	Each Group has a parent Organization.
	"""
	
	@classmethod
	def new_from_row(cls, row, connection):
		"""Given a groups row from the DB, returns a Group object."""
		
		if row[1] is None:
			organization = None
		else:
			organization = Organization.select_by_name(row[1], connection)
		if row[2] is None:
			semester = None
		else:
			semester = Semester.select_by_name(row[2], connection)
		return cls(row[0], organization, semester)
				
	@staticmethod
	def select_by_id(gid, connection):
		"""Return the Group of given ID."""
		
		try:
			cur = connection.cursor()
			
			rows = list(cur.execute('SELECT * FROM groups WHERE id=?', (gid,)))
			if len(rows) > 1 or len(rows) < 0:
				raise DatabaseException(Group.select_by_id.__name__, "Query returned %s rows, expected one." % len(rows))
			elif len(rows) == 1:
				group = Group.new_from_row(rows[0], connection)
			elif len(rows) == 0:
				group = None
				
		finally:
			cur.close()
			return group
	
	@staticmethod
	def select_by_organization(organization, connection):
		"""Return the Group(s) of given parent Organization."""
		
		groups = []
		try:
			if hasattr(organization, name):	# Probably an Organization object
				org = organization.name
			elif isinstance(organization, basestring):
				org = organization
			else:
				raise TypeError
				
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM groups WHERE organization=?', (org,)):
				groups.append(Group.new_from_row(row, connection))
				
		finally:
			cur.close()
			return groups
		
	@staticmethod
	def select_by_semester(semester, connection):
		"""Return the Group(s) of given Semester."""
		
		groups = []
		try:
			if hasattr(semester, term_one):	# Probably a Semester object
				sem = semester.name
			elif isinstance(semester, basestring):
				sem = semester
			else:
				raise TypeError
			
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM groups WHERE semester=?', (sem,)):
				groups.append(Group.new_from_row(row, connection))
				
		finally:
			cur.close()
			return groups
	
	def __init__(self, id, organization, semester, students=[]):
		self.id = id
		self.organization = organization
		self.semester = semester
		self.members = students
		
	def fetch_members(self, connection):
		"""Fetch all Students in this group from the database."""
		
		self.members = Student.select_by_group(self, True, connection)
	
	def add_member(self, student, credit, connection):
		"""Add a new member to the group."""
		
		try:
			cur = connection.cursor()
			
			cur.execute('INSERT OR ABORT INTO group_memberships VALUES (?,?,?)', (student.name, self.id, int(credit),))
				
		finally:
			cur.close()
			self.members.append(student)
	
	def remove_member(self, student, connection):
		"""Remove a member from the group."""
		
		try:
			cur = connection.cursor()
			
			params = (student.name, self.id,)
			cur.execute('DELETE FROM group_memberships WHERE student=? AND group_id=?', params)
			del self.members[self.members.index(student)]
				
		finally:
			cur.close()
		
	def update(self, connection):
		"""Update an existing Group record in the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.id, self.name, self.semester.name,)
			cur.execute('UPDATE groups SET id=?1, name=?2, semester=?3 WHERE id=?1', params)
				
		finally:
			cur.close()
	
	def insert(self, connection):
		"""Write the Group to the DB and retrieve the auto-assigned ID."""
		
		try:
			cur = connection.cursor()
			
			params = (self.name, self.semester.name,)
			cur.execute('INSERT INTO groups VALUES (NULL,?,?)', params)
			
			rows = list(cur.execute('SELECT id FROM groups WHERE name=? AND semester=?', params))
			if len(rows) > 1 or len(rows) < 0:
				raise DatabaseException(self.insert.__name__, "Query returned %s rows, expected one." % len(rows))
			elif len(rows) == 1:
				self.id = rows[0][0]
			elif len(rows) == 0:
				raise DatabaseException(self.insert.__name__, "Could not retrieve group ID post-insert.")
				
		finally:
			cur.close()
	
	def delete(self, connection):
		"""Delete the Group from the DB."""
		
		try:
			cur = connection.cursor()
			
			cur.execute('DELETE FROM groups WHERE id=?', (self.id,))
				
		finally:
			cur.close()
	
	def find_concurrent_optional_groups(self, connection):
		"""Finds this Group's concurrent optional member Groups.
		
		Returns a list of Groups for the Organizations in this Group's parent 
		Organization's optional_member_orgs list with the same Semester as the 
		current Group.
		"""
		
		groups = []
		try:
			cur = connection.cursor()
			
			sql = '''SELECT * FROM groups WHERE semester=? AND organization IN 
			(SELECT child FROM optional_member_orgs WHERE parent=?)'''
			params = (self.semester.name, self.organization.name,)
			for row in cur.execute(sql, params):
				groups.append(Group.new_from_row(row, connection))
		
		finally:
			cur.close()
			return groups
		
	def find_concurrent_mandatory_groups(self, connection):
		"""Finds this Group's concurrent mandatory member Groups.
		
		Return a list of Groups for the Organizations in this Group's parent 
		Organization's mandatory_member_orgs list with the same Semester as the 
		current Group.
		"""
		
		groups = []
		try:
			cur = connection.cursor()
			
			sql = '''SELECT * FROM groups WHERE semester=? AND organization IN 
			(SELECT child FROM mandatory_member_orgs WHERE parent=?)'''
			params = (self.semester.name, self.organization.name,)
			for row in cur.execute(sql, params):
				groups.append(Group.new_from_row(row, connection))
		
		finally:
			cur.close()
			return groups
			
	def read_gc_roster(self, infile, connection):
		"""Parse the group's roster into the database using the Glee Club roster format."""
		
		book = Workbook(infile)
		sheet = book['Sheet1']
		rfid_col = 0
		fname_col = 1
		lname_col = 2
		email_col = 3
		shm_col = 4
		cred_col = 5
		officer_col = 6
		for row, cells in sheet.rows().iteritems():	# row is the row number
			if row == 1: # skip header
				continue
			student = Student(cells[rfid_col].value, cells[fname_col].value, cells[lname_col].value, cells[email_col].value)
			if Student.select_by_id(student.id, connection) is None:	# Not in DB
				student.insert(connection)
			else:
				student.update(connection)
			if '1' in cells[cred_col].value or 'y' in string.lower(cells[cred_col].value) or 't' in string.lower(cells[cred_col].value):
				credit = True
			elif '0' in cells[cred_col].value or 'n' in string.lower(cells[cred_col].value) or 'f' in string.lower(cells[cred_col].value):
				credit = False
			else:
				raise RosterException(self.read_gc_roster.__name__, "Failure parsing contents of credit column in roster row " + row)
			self.add_member(student, credit, connection)

class Absence(object):
	
	"""An instance of a Student not singing into an Event.
	
	May or may not have an Excuse attached to it.
	"""
	
	TYPE_PENDING = "Pending"
	TYPE_EXCUSED = "Excused"
	TYPE_UNEXCUSED = "Unexcused"
	
	@classmethod
	def new_from_row(cls, row, connection):
		"""Given an absences row from the DB, returns an Absence object."""
		
		if row[0] is None:
			student = None
		else:
			student = Student.select_by_id(row[0], connection)
		if row[2] is None:
			event = None
		else:
			event = Event.select_by_id(row[2], connection)[0]
		if row[3] is None:
			excuse = None
		else:
			excuse = Excuse.select_by_id(row[3], connection)
		return cls(student, row[1], event, excuse)
		
	@staticmethod
	def select_by_student(student, connection):
		"""Return the list of Absences by a Student."""
		
		absences = []
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM absences WHERE student=?', (student.id,)):
				absences.append(Absence.new_from_row(row, connection))
				
		finally:
			cur.close()
			return absences
	
	@staticmethod
	def select_by_type(absence_type, connection):
		"""Return the list of Absences of a given ABSENCE.TYPE_ string."""
		
		absences = []
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM absences WHERE type=?', (absence_type,)):
				absences.append(Absence.new_from_row(row, connection))
				
		finally:
			cur.close()
			return absences
		
	@staticmethod
	def select_by_event(event, connection):
		"""Return the list of Absences of a given datetime."""
		
		absences = []
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM absences WHERE event=?', (event.id,)):
				absences.append(Absence.new_from_row(row, connection))
				
		finally:
			cur.close()
			return absences
	
	@staticmethod
	def select_by_excuse(excuse_id, connection):
		"""Return the list of Absences of a given excuse ID.
		
		Should only return one, but returning a list in case of
		data integrity issues related to Excuse-Event mis-assignment.
		"""
		
		absences = []
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM absences WHERE excuseid=?', (excuse_id,)):
				absences.append(Absence.new_from_row(row, connection))
				
		finally:
			cur.close()
			return absences
	
	@staticmethod
	def select_by_all(student_id, absence_type, event_id, excuse_id, connection):
		"""Return the list of Absences using any combination of filters."""
		
		absences = []
		try:
			cur = connection.cursor()
			
			sql = 'SELECT * FROM absences WHERE student=? AND type=? AND event=? AND excuseid=?'
			params = (student_id, absence_type, event_id, excuse_id,)
			for row in cur.execute(sql, params):
				absences.append(Absence.new_from_row(row, connection))
				
		finally:
			cur.close()
			return absences
	
	def __init__(self, student, type, event, excuse=None):
		self.student = student	# A Student object
		self.type = type				# An Absence.TYPE_ string constant
		self.event = event		# An Event object
		self.excuse = excuse	# An Excuse object
	
	def update(self, connection):
		"""Update an existing Absence record in the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.student.rfid, self.type, self.event.id, self.excuse.id,)
			cur.execute('UPDATE absences SET student=?1, type=?2, event=?3, excuseid=?4 WHERE student=?1 AND event=?3', params)
				
		finally:
			cur.close()
	
	def insert(self, connection):
		"""Write the Absence to the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.student, self.type, self.event.id, self.excuse_id,)
			cur.execute('INSERT INTO absences VALUES (?,?,?,?)', params)
				
		finally:
			cur.close()
	
	def delete(self, connection):
		"""Delete the Absence from the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.student.rfid, self.event.id,)
			cur.execute('DELETE FROM absences WHERE student=? AND eventdt=?', params)
				
		finally:
			cur.close()
	
class Excuse(object):
	
	"""A Student's excuse for missing an Event sent to gc-excuse.
	
	The datetime and student ID are the primary key colums.
	"""
	
	# Cutoffs for when students can email gc-excuse (relative to event start time)
	EXCUSES_OPENS = timedelta(-1, 0, 0, 0, 0, -18, 0)	# 1 day, 18 hours before
	EXCUSES_CLOSES = timedelta(0, 0, 0, 0, 0, 6, 0)	# 6 hours after
	
	@classmethod
	def new_from_row(cls, row, connection):
		"""Given an excuses row from the DB, returns an Excuse object."""
		
		if row[2] is None:
			event = None
		else:
			event = Event.select_by_id(row[2], connection)[0]
		if row[4] is None:
			student = None
		else:
			student = Student.select_by_id(row[4], connection)
		return cls(row[0], convert_timestamp(row[1]), event, row[3], student)
	
	@staticmethod
	def select_by_id(excuse_id, connection):
		"""Return the Excuse of given unique ID."""
		
		try:
			cur = connection.cursor()
			
			rows = list(cur.execute('SELECT * FROM excuses WHERE id=?', (excuse_id,)))
			if len(rows) > 1 or len(rows) < 0:
				raise DatabaseException(Excuse.select_by_id.__name__, "Query returned %s rows, expected one." % len(rows))
			elif len(rows) == 1:
				excuse = Excuse.new_from_row(rows[0], connection)
			elif len(rows) == 0:
				excuse = None
				
		finally:
			cur.close()
			return excuse
	
	@staticmethod
	def select_by_student(student, connection):
		"""Return the list of Excuses by a Student."""
		
		excuses = []
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM excuses WHERE student=?', (student.id,)):
				excuses.append(Excuse.new_from_row(row, connection))
				
		finally:
			cur.close()
			return excuses
	
	@staticmethod
	def select_by_datetime_range(start_dt, end_dt, connection):
		"""Return the list of Excuses in a given datetime range."""
		
		if type(start_dt == datetime):
			start_dt = start_dt.isoformat()
		if type(end_dt == datetime):
			end_dt = end_dt.isoformat()
		
		excuses = []
		try:
			cur = connection.cursor()
			
			params = (start_dt, end_dt,)
			for row in cur.execute('SELECT * FROM excuses WHERE dt BETWEEN ? AND ?', params):
				excuses.append(Excuse.new_from_row(row, connection))
				
		finally:
			cur.close()
			return excuses
		
	@staticmethod
	def select_by_event(event, connection):
		"""Return the list of Excuses associated with a given Event."""
		
		excuses = []
		
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM excuses WHERE event=?', (event.id,)):
				excuses.append(Excuse.new_from_row(row, connection))
				
		finally:
			cur.close()
			return excuses
		
	@staticmethod
	def select_by_all(excuse_id, student_id, start_dt, end_dt, event_id, connection):
		"""Return a list of Excuses using any combination of filters."""
		
		excuses = []
		
		if type(start_dt == datetime):
			start_dt = start_dt.isoformat()
		if type(end_dt == datetime):
			end_dt = end_dt.isoformat()
		
		try:
			cur = connection.cursor()
			
			sql = '''SELECT * FROM excuses WHERE id=? AND student=? AND (dt BETWEEN ? AND ?) AND event=?''' 
			params = (excuse_id, student_id, start_dt, end_dt, event_id,)
			for row in cur.execute(sql, params):
				excuses.append(Excuse.new_from_row(row, connection))
				
		finally:
			cur.close()
			return excuses
	 
	def __init__(self, id, dt, event, reason, s):
		self.id = id				# Unique primary key
		self.excuse_dt = dt			# a datetime object
		self.event = event			# an Event object
		self.reason = reason		# Student's message to gc-excuse
		self.student = s			# a Student object
	
	def update(self, connection):
		"""Update an existing Excuse record in the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.excuse_dt.isoformat(), self.event.id, self.reason, self.student.rfid, self.id,)
			cur.execute('UPDATE excuses SET dt=?, event=?, reason=?, student=? WHERE id=?', params)
				
		finally:
			cur.close()
	
	def insert(self, connection):
		"""Write the Excuse to the DB and retrieve the auto-assigned ID."""
		
		try:
			cur = connection.cursor()
			
			params = (self.excuse_dt.isoformat(), self.event.id, self.reason, self.student.rfid,)
			# INSERTing 'NULL' for the integer primary key column autogenerates an id
			cur.execute('INSERT INTO excuses VALUES (NULL,?,?,?,?)', params)
			
			params = (self.excuse_dt.isoformat(), self.event.id, self.student.rfid,)
			rows = list(cur.execute('SELECT id FROM excuses WHERE dt=? AND event=? AND student=?', params))
			if len(rows) > 1 or len(rows) < 0:
				raise DatabaseException(self.insert.__name__, "Query returned %s rows, expected one." % len(rows))
			elif len(rows) == 1:
				self.id = rows[0][0]
			elif len(rows) == 0:
				raise DatabaseException(self.insert.__name__, "Could not retrieve excuse ID post-insert.")
				
		finally:
			cur.close()
	
	def delete(self, connection):
		"""Delete the Excuse from the DB."""
		
		try:
			cur = connection.cursor()
			
			cur.execute('DELETE FROM excuses WHERE id=?', (self.id,))
				
		finally:
			cur.close()
	
class Signin(object):
	
	"""Corresponds to a row in the RFID output record file. 
	
	The datetime and student ID are the primary key colums.
	"""
	
	@classmethod
	def new_from_row(cls, row, connection):
		"""Given a signins row from the DB, returns a signin object."""
		
		if row[1] is None:
			event = None
		else:
			event = Event.select_by_id(row[1], connection)[0]
		if row[2] is None:
			student = None
		else:
			student = Student.select_by_id(row[2], connection)
		return cls(convert_timestamp(row[0]), event, student)
		
	@staticmethod
	def select_by_student(student, connection):
		"""Return the list of Signins by a Student."""
		
		signins = []
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM signins WHERE student=?', (student.id,)):
				signins.append(Signin.new_from_row(row, connection))
				
		finally:
			cur.close()
			return signins
	
	@staticmethod
	def select_by_start(start_dt, end_dt, connection):
		"""Return the list of Signins in a given datetime range."""
		
		signins = []
		if type(start_dt == datetime):
			start_dt = start_dt.isoformat()
		if type(end_dt == datetime):
			end_dt = end_dt.isoformat()
		
		try:
			cur = connection.cursor()
			
			params = (start_dt, end_dt,)
			for row in cur.execute('SELECT * FROM signins WHERE dt BETWEEN ? AND ?', params):
				signins.append(Signin.new_from_row(row, connection))
				
		finally:
			cur.close()
			return signins
	
	@staticmethod
	def select_by_event(event, connection):
		"""Return the list of Signins associated with a given Event."""
		
		signins = []
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM signins WHERE event=?', (event.id,)):
				signins.append(Signin.new_from_row(row, connection))
				
		finally:
			cur.close()
			return signins
	
	@staticmethod
	def select_by_all(id, start_dt, end_dt, event_id, connection):
		"""Return a list of Signins using any combination of filters."""
		
		signins = []
		if type(start_dt == datetime):
			start_dt = start_dt.isoformat()
		if type(end_dt == datetime):
			end_dt = end_dt.isoformat()
			
		try:
			cur = connection.cursor()
			
			sql = 'SELECT * FROM signins WHERE student=? AND (dt BETWEEN ? AND ?) AND event=?'
			params = (id, start_dt, end_dt, event_id,)
			for row in cur.execute(sql, params):
				signins.append(Signin.new_from_row(row, connection))
				
		finally:
			cur.close()
			return signins
	
	def __init__(self, dt, event, student):
		self.signin_dt = dt			# a datetime object
		self.event = event			# an Event object
		self.student = student		# a Student object
	
	def guess_event(self, connection):
		"""Search the database for events that this Signin might correspond to.
		
		Likely events are defined as starting within 2 hours of self.signin_dt
		(to allow for showing up and/or signing in late) and are held by a group
		that self.student is a member of.
		@return: A list of Event objects (without setting self.event directly).
		"""
		
		time_window = datetime.timedelta(0, 0, 0, 0, 0, 2, 0)	# 2 hours
		try:
			events = []
			cur = connection.cursor()
			
			sql = 'SELECT * FROM events WHERE (dt BETWEEN ? AND ?) AND group IN (SELECT group FROM group_memberships WHERE student=?)'
			params = ((self.signin_dt - time_window).isoformat(), (self.signin_dt + time_window).isoformat(), self.student.rfid,) 
			for row in cur.execute(sql, params):
				if row[4] is None:
					group = None
				else:
					group = Group.select_by_id(row[4], connection)
				if row[5] is None:
					semester = None
				else:
					semester = Semester.select_by_name(row[5], connection)
				event = Event(int(row[0]), row[1], convert_timestamp(row[2]), row[3], group, semester)
				events.append(event)
		
		finally:
			cur.close()
			return events
	
	def update(self, connection):
		"""Update an existing Signin record in the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.signin_dt.isoformat(), self.event.id, self.student.rfid,)
			cur.execute('UPDATE signins SET dt=?1, event=?2, student=?3 WHERE dt=?1 AND student=?3', params)
				
		finally:
			cur.close()
	
	def insert(self, connection):
		"""Write the Signin to the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.signin_dt.isoformat(), self.event.id, self.student.rfid,)
			cur.execute('INSERT OR ABORT INTO signins VALUES (?,?,?)', params)
				
		finally:
			cur.close()
	
	def delete(self, connection):
		"""Delete the Signin from the DB."""
		
		try:
			cur = connection.cursor()
			
			params = (self.signin_dt.isoformat(), self.student.rfid,)
			cur.execute('DELETE FROM signins WHERE dt=? AND student=?', params)
				
		finally:
			cur.close()

class Event(object):
	
	"""An event where attendance is taken.
	
	The datetime is the primary key column.
	"""
	
	TYPE_REHEARSAL = 'Rehearsal'
	TYPE_MAKEUP = 'Makeup Rehearsal'
	TYPE_DRESS = 'Dress Rehearsal'	# Mandatory for a concert
	TYPE_CONCERT = 'Concert'
	
	# Cutoffs for when students can sign in (relative to event start time)
	ATTENDANCE_OPENS = timedelta(0, 0, 0, 0, -30, 0, 0)	# 30 minutes before
	ATTENDANCE_CLOSES = timedelta(0, 0, 0, 0, 30, 1, 0)	# 90 minutes after
	
	@classmethod
	def new_from_row(cls, row, connection):
		"""Given an events row from the DB, returns an Event object."""
		
		if row[7] is None:
			group = None
		else:
			group = Group.select_by_id(row[7], connection)
		if row[8] is None:
			semester = None
		else:
			semester = Semester.select_by_name(row[8], connection)
		return cls(row[0], row[1], row[2], convert_timestamp(row[4]), convert_timestamp(row[5]), row[6], group, semester, row[9])

	@staticmethod
	def select_by_id(event_id, connection):
		"""Return the Event of given unique ID."""
		
		try:
			cur = connection.cursor()
			
			rows = list(cur.execute('SELECT * FROM events WHERE id=?', (event_id,)))
			if len(rows) > 1 or len(rows) < 0:
				raise DatabaseException(Event.select_by_id.__name__, "Query returned %s rows, expected one." % len(rows))
			elif len(rows) == 1:
				event = Event.new_from_row(rows[0], connection)
			elif len(rows) == 0:
				event = None
				
		finally:
			cur.close()
			return event
	
	@staticmethod
	def select_by_name(name, connection):
		"""Return the list of Events of a given name."""
		
		events = []
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM events WHERE eventname=?', (name,)):
				events.append(Event.new_from_row(row, connection))
				
		finally:
			cur.close()
			return events
	
	@staticmethod
	def select_by_start(event_dt, connection):
		"""Return the list of Events starting at a specific datetime."""
		
		events = []
		if type(event_dt == datetime):
			event_dt = event_dt.isoformat()
		
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM events WHERE start=?', (event_dt,)):
				events.append(Event.new_from_row(row, connection))
				
		finally:
			cur.close()
			return events
	
	@staticmethod
	def select_by_datetime_range(start_dt, end_dt, connection):
		"""Return the list of Events in a given datetime range."""
		
		events = []
		if type(start_dt == datetime):
			start_dt = start_dt.isoformat()
		if type(end_dt == datetime):
			end_dt = end_dt.isoformat()
		
		try:
			cur = connection.cursor()
			
			params = (start_dt, end_dt,)
			for row in cur.execute('SELECT * FROM events WHERE start BETWEEN ? AND ?', params):
				events.append(Event.new_from_row(row, connection))
				
		finally:
			cur.close()
			return events
	
	@staticmethod
	def select_by_type(type, connection):
		"""Return the list of Events of a given type."""
		
		events = []
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM events WHERE eventtype=?', (type,)):
				events.append(Event.new_from_row(row, connection))
				
		finally:
			cur.close()
			return events
	
	@staticmethod
	def select_by_group(group, connection):
		"""Return the list of Events of a given group."""
		
		events = []
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM events WHERE group_id=?', (group,)):
				events.append(Event.new_from_row(row, connection))
				
		finally:
			cur.close()
			return events
	
	@staticmethod
	def select_by_semester(semester, connection):
		"""Return the list of Events in a given Semester."""
		
		events = []
		try:
			cur = connection.cursor()
			
			for row in cur.execute('SELECT * FROM events WHERE semester=?', (semester,)):
				events.append(Event.new_from_row(row, connection))
				
		finally:
			cur.close()
			return events
	
	@staticmethod
	def select_by_gcal_id(gcal_id, connection):
		"""Return the Event of a given Google Calendar event ID."""
		
		try:
			cur = connection.cursor()
			event = None
			rows = list(cur.execute('SELECT * FROM events WHERE gcal_id=?', (gcal_id,)))
			if len(rows) > 1 or len(rows) < 0:
				raise DatabaseException(Event.select_by_gcal_id.__name__, "Query returned %s rows, expected one." % len(rows))
			elif len(rows) == 1:
				event = Event.new_from_row(rows[0], connection)
			elif len(rows) == 0:
				event = None
				
		finally:
			cur.close()
			return event
	
	@staticmethod
	def select_by_all(name, start_dt, end_dt, type, group, semester, gcal_id, connection):
		"""Return a list of Events using any combination of filters."""
		
		events = []
		if type(start_dt == datetime):
			start_dt = start_dt.isoformat()
		if type(end_dt == datetime):
			end_dt = end_dt.isoformat()
		
		try:
			cur = connection.cursor()
			
			sql = 'SELECT * FROM events WHERE eventname=? AND (start BETWEEN ? AND ?) AND eventtype=? AND group_id=? AND semester=? AND gcal_id=?'
			params = (name, start_dt, end_dt, type, group, semester, gcal_id,)
			for row in cur.execute(sql, params):
				events.append(Event.new_from_row(row, connection))
				
		finally:
			cur.close()
			return events
	
	def __init__(self, id, name, description, location, start, end, type, group, semester, gcal_id):
		self.id = id
		self.event_name = name
		self.description = description
		self.location = location
		self.start = start			# a datetime object
		self.end = end				# a datetime object
		self.event_type = type		# One of the Event.TYPE_ constants
		self.group = group			# Roster to check against
		self.semester = semester	# A Semester object
		self.gcal_id = gcal_id		# Google Calendar event ID
		self.signins = []
		self.excuses = []
		self.absences = []
	
	def fetch_signins(self, connection):
		"""Fetch all Signins for this Event from the database."""
		
		self.signins = Signin.select_by_start(self.event_dt+Event.ATTENDANCE_OPENS, self.event_dt+Event.ATTENDANCE_CLOSES, connection)
	
	def fetch_excuses(self, connection):
		"""Fetch all Excuses for this Event from the database."""
		
		self.excuses = Excuse.select_by_start(self.event_dt+Excuse.EXCUSES_OPENS, self.event_dt+Excuse.EXCUSES_CLOSES, connection)
		
	def fetch_absences(self, connection):
		"""Fetch all Absences for this Event from the database."""
		
		self.absences = Absence.select_by_event(self, connection)
	
	def make_json(self, connection):
		"""Converts the Event object into a JSON object suitable for use in Google calendar."""
		
		event = {}
		attendees = []
		optional_attendees = set()
		mandatory_attendees = set()
		for group in self.group.find_concurrent_optional_groups(connection):
			for member in group.members:
				optional_attendees.add(member)
		
		for group in self.group.find_mandatory_optional_groups(connection):
			for member in group.members:
				mandatory_attendees.add(member)
		
		# Set attendee 'optional' flag 
		for member in self.group.members:
			attendee = {
					'email' : member.email, 
					'displayName' : member.lname + ', ' + member.fname }
			if member in optional_attendees and member not in mandatory_attendees:
				attendee['optional'] = True
			else:
				attendee['optional'] = False
			attendees.append(attendee)
		
		event['attendees'] = attendees
		event['start'] = {'dateTime' : self.start.isoformat()}
		event['end'] = {'dateTime' : self.end.isoformat()}
		event['summary'] = self.event_name
		event['status'] = 'confirmed'
		if self.description is not None and len(self.description) > 0:
			event['description'] = self.description
		if self.loctaion is not None and len(self.location) > 0:
			event['location'] = self.location
		if self.gcal_id is not None:
			event['id'] = self.gcal_id
		
		event = simplejson.dumps(event)
		return event
	
	def gcal_get(self, gcal):
		"""Get this Event from the parent Organization's Google calendar.
		
		@requires: self.gcal_id is not None. Will return None in this case.
		@param gcal: A GCal instance.
		@return: The retrieved event resource or None.
		"""
		
		resource = None
		if self.gcal_id is not None:
			resource = gcal.service.events().get(calendarId=self.group.organization.calendar['id'], eventId=self.gcal_id).execute()
		
		return resource
	
	def gcal_insert(self, gcal, resource):
		"""Insert this Event into the parent Organization's Google calendar.
		
		@param gcal: A GCal instance.
		@param resource: A gcal event resource JSON object in dict format.
		@return: The inserted event resource. 
		Setting self.gcal_id to return_val['id'] is normally recommended.
		"""
		
		inserted_event = gcal.service.events().insert(calendarId=self.group.organization.calendar['id'], body=resource).execute()
		return inserted_event
	
	def gcal_update(self, gcal, resource):
		"""Update this Event on the parent Organization's Google calendar. 
		
		If the passed resource has no event ID, inserts the resource as a new event.
		@requires: self.gcal_id is not None
		@param gcal: A GCal instance. 
		@param resource: A gcal event resource JSON object in dict format. 
		@return: The updated event resource.
		"""
		
		if 'id' in resource:	# Resource has a gcal event ID to pass
			updated_event = gcal.service.events().update(calendarId=self.group.organization.calendar['id'], eventId=resource['id'], body=resource).execute()
			return updated_event
		else:
			inserted_event = self.gcal_insert(gcal, resource)
			self.gcal_id = inserted_event['id']
			return inserted_event
	
	def update(self, connection):
		"""Update an existing Event record in the DB."""
		
		try:
			cur = connection.cursor()
			
			sql = 'UPDATE events SET eventname=?, description=?, location=? start=?, end=?, type=?, group_id=?, gcal_id=? WHERE id=?'
			params = (self.name, self.description, self.location, self.start.isoformat(), self.end.isoformat(), self.event_type, self.group.id, self.gcal_id, self.id,)
			cur.execute(sql, params)
				
		finally:
			cur.close()
	
	def insert(self, connection):
		"""Write the Event to the DB and retrieve the auto-assigned ID."""
		
		try:
			cur = connection.cursor()
			
			params = (self.name, self.description, self.location, self.start.isoformat(), self.end.isoformat(), self.event_type, self.group.id, self.gcal_id,)
			cur.execute('INSERT INTO events VALUES (NULL,?,?,?,?,?,?,?,?)', params)
			
			params = (self.name, self.start.isoformat(), self.end.isoformat(), self.event_type, self.group.id, self.gcal_id,)
			rows = list(cur.execute('SELECT id FROM events WHERE eventname=? AND start=? AND end=? AND type=? AND group_id=? AND gcal_id=?', params))
			if len(rows) > 1 or len(rows) < 0:
				raise DatabaseException(self.insert.__name__, "Query returned %s rows, expected one." % len(rows))
			elif len(rows) == 1:
				self.id = rows[0][0]
			elif len(rows) == 0:
				raise DatabaseException(self.insert.__name__, "Could not retrieve event ID post-insert.")
				
		finally:
			cur.close()
	
	def delete(self, connection):
		"""Delete the Event from the DB."""
		
		try:
			cur = connection.cursor()
			
			cur.execute('DELETE FROM events WHERE id=?', (self.id,))
				
		finally:
			cur.close()
